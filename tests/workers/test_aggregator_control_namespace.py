"""Regression test for the inbox collision between DATA messages from upstream
workers and FLUSH_ACK / FLUSH_ORDER control messages.

Root cause: both DATA packets (from upstream sum/filter workers) and control
messages (FLUSH_ACK, FLUSH_ORDER) shared the same sender_id integer space
(worker IDs 0-N). The inbox keys by (client_id, sender_id, seq), so a FLUSH_ACK
from aggregator_1 (sender_id=1, seq=client_id=0) would match a DATA message from
filter_q5_usd_1 (sender_id=1, seq=0) and be silently discarded as DONE.

The fix: the inbox key is now (client_id, kind, sender_id, seq) where MsgKind
is an explicit enum field — DATA, CTRL_FLUSH_ORDER, CTRL_FLUSH_ACK, etc.
Different kinds are always in separate buckets regardless of sender_id or seq
values.
"""

import pytest

from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import PersistentStateHandler
from common.fault_tolerance.inbox import InboxStatus, MsgKind

from tests.common.fault_tolerance.fakes import (
    FakeLastState,
    FakeWal,
    FakeWorkerState,
    change,
)

# A small client_id like 0 is the worst case: after just 1 DATA message
# (seq=0), a FLUSH_ACK with seq=client_id=0 would previously collide.
CLIENT = 0
UPSTREAM_SENDER = 1   # sum worker / filter_q5_usd worker ID
AGG_NON_LEADER  = 1   # non-leader aggregator ID (same integer, different role)
LEADER_ID       = 0


def _make_handler():
    ws = FakeWorkerState()
    h = PersistentStateHandler(
        state_dir="unused",
        node_id="agg_0",
        worker_state=ws,
        snapshot_every=1000,
        last_state=FakeLastState(),
        wal=FakeWal(),
    )
    return h, ws


def _data_bfn():
    return lambda _p: (change(1), [])


def _ctrl_bfn():
    """Control business_fn — raises if accidentally skipped (never called on DONE)."""
    called = []

    def fn(_p):
        called.append(True)
        return change(100), []

    return fn, called


def _process_data(handler, seq):
    """Simulate one DATA addressed packet from UPSTREAM_SENDER for CLIENT."""
    msg_id = f"d:{UPSTREAM_SENDER}:{CLIENT}:{seq}"
    instr = handler.handle(msg_id, CLIENT, UPSTREAM_SENDER, seq, b"data", _data_bfn())
    if instr.action is Action.PUBLISH_THEN_COMMIT:
        handler.commit_done(*instr.ctx)
    return instr


# ---------------------------------------------------------------------------
# Bug reproduction — demonstrates what broke before the fix
# ---------------------------------------------------------------------------

def test_without_kind_flush_ack_collides_after_one_data_message():
    """With the old single-dimension key the FLUSH_ACK is silently dropped.

    After one DATA(client=0, sender=1, seq=0) is committed, the inbox marks
    (client=0, sender=1, seq=0) as DONE.  A FLUSH_ACK that also passes
    sender=1, seq=0 with MsgKind.DATA (wrong kind) is then classified DONE
    and the business_fn is never called.
    """
    handler, ws = _make_handler()

    # One DATA message: marks tracker(client=0, kind=DATA, sender=1).biggest = 0
    _process_data(handler, seq=0)
    assert ws.total == 1

    # Simulate the old behaviour: pass DATA kind for a FLUSH_ACK.
    bfn, called = _ctrl_bfn()
    msg_id = f"fa:{CLIENT}:{AGG_NON_LEADER}"
    instr = handler.handle(
        msg_id, CLIENT, AGG_NON_LEADER, CLIENT, b"ack", bfn, kind=MsgKind.DATA
    )

    # Bug: handler returns ACK immediately (DONE path) — business_fn was skipped.
    assert instr.action is Action.ACK
    assert not called, "business_fn should NOT be called when kind is wrong (bug demonstrated)"
    assert ws.total == 1  # state unchanged


# ---------------------------------------------------------------------------
# Fix verification — distinct kinds never collide
# ---------------------------------------------------------------------------

def test_flush_ack_kind_does_not_collide_with_data():
    """With MsgKind.CTRL_FLUSH_ACK the FLUSH_ACK is always treated as NEW.

    Even after processing many DATA messages from the same upstream sender,
    the FLUSH_ACK lives in a disjoint inbox bucket.
    """
    handler, ws = _make_handler()

    # Process several DATA messages — enough that seq=CLIENT would be in done.
    for seq in range(CLIENT + 2):
        _process_data(handler, seq=seq)
    assert ws.total == CLIENT + 2

    # FLUSH_ACK with correct kind — separate inbox bucket.
    bfn, called = _ctrl_bfn()
    msg_id = f"fa:{CLIENT}:{AGG_NON_LEADER}"
    instr = handler.handle(
        msg_id, CLIENT, AGG_NON_LEADER, CLIENT, b"ack", bfn,
        kind=MsgKind.CTRL_FLUSH_ACK,
    )

    assert called, "business_fn MUST be called: FLUSH_ACK is a new event"
    assert instr.action is Action.PUBLISH_THEN_COMMIT
    handler.commit_done(*instr.ctx)
    assert ws.total == CLIENT + 2 + 100  # business effect applied


def test_flush_order_kind_does_not_collide_with_data():
    """MsgKind.CTRL_FLUSH_ORDER never collides with DATA even for the same sender."""
    handler, ws = _make_handler()

    upstream_sender_0 = LEADER_ID  # sender_id=0 — same int as leader

    # Process DATA from upstream sender 0 — covers seq=CLIENT.
    for seq in range(CLIENT + 2):
        instr = handler.handle(
            f"d:{upstream_sender_0}:{CLIENT}:{seq}",
            CLIENT, upstream_sender_0, seq, b"data", _data_bfn(),
        )
        if instr.action is Action.PUBLISH_THEN_COMMIT:
            handler.commit_done(*instr.ctx)

    # FLUSH_ORDER with correct kind.
    bfn, called = _ctrl_bfn()
    msg_id = f"fo:{CLIENT}:{LEADER_ID}"
    instr = handler.handle(
        msg_id, CLIENT, LEADER_ID, CLIENT, b"order", bfn,
        kind=MsgKind.CTRL_FLUSH_ORDER,
    )

    assert called, "business_fn MUST be called: FLUSH_ORDER is a new event"


def test_eof_received_kind_does_not_collide_with_data():
    """MsgKind.CTRL_EOF_RECEIVED never collides with DATA for the same sender."""
    handler, ws = _make_handler()

    # Process DATA from the same sender/seq tuple that EOF_RECEIVED uses.
    _process_data(handler, seq=CLIENT)
    assert ws.total == 1

    bfn, called = _ctrl_bfn()
    msg_id = f"er:{CLIENT}:{UPSTREAM_SENDER}"
    instr = handler.handle(
        msg_id, CLIENT, UPSTREAM_SENDER, CLIENT, b"eof-received", bfn,
        kind=MsgKind.CTRL_EOF_RECEIVED,
    )

    assert called, "business_fn MUST be called: EOF_RECEIVED is a new event"
    assert instr.action is Action.PUBLISH_THEN_COMMIT


def test_flush_ack_idempotent_on_redeliver():
    """A redelivered FLUSH_ACK (same kind+sender+seq) is correctly deduplicated."""
    handler, ws = _make_handler()

    msg_id = f"fa:{CLIENT}:{AGG_NON_LEADER}"

    bfn, called = _ctrl_bfn()
    instr = handler.handle(
        msg_id, CLIENT, AGG_NON_LEADER, CLIENT, b"ack", bfn,
        kind=MsgKind.CTRL_FLUSH_ACK,
    )
    assert called
    handler.commit_done(*instr.ctx)

    # Second delivery (RabbitMQ redeliver) — must be classified DONE.
    bfn2, called2 = _ctrl_bfn()
    instr2 = handler.handle(
        msg_id, CLIENT, AGG_NON_LEADER, CLIENT, b"ack", bfn2,
        kind=MsgKind.CTRL_FLUSH_ACK,
    )
    assert instr2.action is Action.ACK
    assert not called2, "business_fn must NOT run on duplicate FLUSH_ACK"


def test_data_and_ctrl_kinds_are_independent_per_sender():
    """All three kinds for the same (client, sender, seq) are independent buckets."""
    handler, ws = _make_handler()
    SENDER = 5
    SEQ = 7

    for kind, delta in [
        (MsgKind.DATA, 1),
        (MsgKind.CTRL_FLUSH_ORDER, 100),
        (MsgKind.CTRL_FLUSH_ACK, 10000),
        (MsgKind.CTRL_EOF_RECEIVED, 100000),
    ]:
        bfn = lambda _p, d=delta: (change(d), [])
        instr = handler.handle(
            f"{kind.name}:{CLIENT}:{SENDER}:{SEQ}",
            CLIENT, SENDER, SEQ, b"x", bfn, kind=kind,
        )
        assert instr.action is Action.PUBLISH_THEN_COMMIT
        handler.commit_done(*instr.ctx)

    assert ws.total == 1 + 100 + 10000 + 100000
