from common.message_protocol.external import FileChunk, FileEof
from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
    MSG_CHUNK,
    MSG_EOF,
)
from common.message_protocol.internal import (
    ControlMessageSerializer,
    InternalProtocol,
    LineBatchSerializer,
    MessageType,
)
from workers.file_splitter.file_splitter import (
    FileSplitter,
    FileSplitterConfig,
    _sender_id_for_path,
)
import workers.file_splitter.file_splitter as file_splitter_module
from workers.file_splitter.file_splitter_state import (
    ACCOUNTS_EDGE,
    LINE_BATCH_EDGE,
    FileKey,
    FileSplitterState,
)


HEADER_LINE = (
    b"Timestamp,From Bank,Account,To Bank,Account,Amount Paid,"
    b"Payment Currency,Payment Format"
)
ROW_1 = b"2022/09/01 00:08,1,from-1,2,to-1,12.5,US Dollar,Wire"
ROW_2 = b"2022/09/01 00:09,3,from-2,4,to-2,22.0,US Dollar,ACH"
ROW_3 = b"2022/09/01 00:10,5,from-3,6,to-3,33.0,US Dollar,Cash"
PATH = "LI-Mini_Trans.csv"


# --------------------------------------------------------------------------- #
# FileSplitterState: simulate (output) + apply_change (state) stay in lockstep
# --------------------------------------------------------------------------- #


def _new_state(max_batch_bytes=4096, accounts_enabled=False):
    return FileSplitterState(
        max_line_bytes=1024,
        max_batch_bytes=max_batch_bytes,
        splitter_id=5,
        accounts_enabled=accounts_enabled,
    )


def _drive(state, change):
    """Mirror the worker: collect outputs from simulate, then apply the change."""
    out = state.simulate(change)
    state.apply_change(change)
    return out


def _line_batch(emitted):
    edge, msg_type, payload = emitted
    assert msg_type == MessageType.DATA
    return edge, LineBatchSerializer.deserialize(payload)


def _control(emitted):
    edge, msg_type, payload = emitted
    assert msg_type == MessageType.EOF
    return edge, ControlMessageSerializer.deserialize(payload)


def test_state_emits_line_batch_and_eof_across_chunks():
    state = _new_state()

    first = HEADER_LINE + b"\n" + ROW_1 + b"\npar"
    out = _drive(state, FileSplitterState.chunk_change(7, PATH, FILE_TYPE_TRANSACTIONS, 0, first))
    assert out == []  # batch still accumulating

    second = b"tial,4,to-2,22.0,US Dollar,ACH\n"
    out = _drive(
        state, FileSplitterState.chunk_change(7, PATH, FILE_TYPE_TRANSACTIONS, len(first), second)
    )
    assert out == []

    out = _drive(state, FileSplitterState.file_eof_change(7, PATH))
    assert len(out) == 2

    edge, batch = _line_batch(out[0])
    assert edge == LINE_BATCH_EDGE
    assert batch.file_type == FILE_TYPE_TRANSACTIONS
    assert batch.rel_path == PATH
    assert batch.batch_id == 0
    assert batch.first_line_number == 2
    assert batch.lines == (ROW_1, b"partial,4,to-2,22.0,US Dollar,ACH")

    _, eof = _control(out[1])
    assert eof.expected_total == len(batch.lines)


def test_state_flushes_batches_by_size():
    state = _new_state(max_batch_bytes=1)
    data = HEADER_LINE + b"\n" + ROW_1 + b"\n" + ROW_2 + b"\n" + ROW_3 + b"\n"

    emitted = []
    emitted += _drive(state, FileSplitterState.chunk_change(7, PATH, FILE_TYPE_TRANSACTIONS, 0, data))
    emitted += _drive(state, FileSplitterState.file_eof_change(7, PATH))

    batches = [_line_batch(e)[1] for e in emitted[:-1]]
    _, eof = _control(emitted[-1])

    assert [b.batch_id for b in batches] == [0, 1, 2]
    assert [b.first_line_number for b in batches] == [2, 3, 4]
    assert [b.lines for b in batches] == [(ROW_1,), (ROW_2,), (ROW_3,)]
    assert eof.expected_total == 3


def test_state_strips_cr_from_header_but_keeps_raw_data_lines():
    state = _new_state()
    data = HEADER_LINE + b"\r\n" + ROW_1 + b"\r\n"

    emitted = []
    emitted += _drive(state, FileSplitterState.chunk_change(7, PATH, FILE_TYPE_TRANSACTIONS, 0, data))
    emitted += _drive(state, FileSplitterState.file_eof_change(7, PATH))

    _, batch = _line_batch(emitted[0])
    assert batch.header[-1] == "Payment Format"
    assert batch.lines == (ROW_1 + b"\r",)


def test_state_drops_accounts_when_disabled():
    state = _new_state(accounts_enabled=False)
    path = "LI-Mini_accounts.csv"

    emitted = []
    emitted += _drive(state, FileSplitterState.chunk_change(7, path, FILE_TYPE_ACCOUNTS, 0, b"Bank,Account\n1,abc\n"))
    emitted += _drive(state, FileSplitterState.file_eof_change(7, path))

    assert emitted == []


def test_state_accounts_emits_batch_and_eof_when_enabled():
    state = _new_state(accounts_enabled=True)
    path = "LI-Mini_accounts.csv"

    emitted = []
    emitted += _drive(state, FileSplitterState.chunk_change(7, path, FILE_TYPE_ACCOUNTS, 0, b"Bank ID,Bank Name\n001,Raw One\n"))
    emitted += _drive(state, FileSplitterState.file_eof_change(7, path))

    edge, batch = _line_batch(emitted[0])
    assert edge == ACCOUNTS_EDGE
    assert batch.file_type == FILE_TYPE_ACCOUNTS
    assert batch.lines == (b"001,Raw One",)
    edge, eof = _control(emitted[1])
    assert edge == ACCOUNTS_EDGE
    assert eof.expected_total == 1


def test_state_snapshot_restore_round_trip():
    state = _new_state()
    state.apply_change(FileSplitterState.chunk_change(7, PATH, FILE_TYPE_TRANSACTIONS, 0, HEADER_LINE + b"\n" + ROW_1 + b"\npar"))
    key = FileKey(client_id=7, rel_path=PATH)
    expected_offset = state.expected_offset(key)
    next_ordinal = state.next_ordinal(key)

    restored = _new_state()
    restored.restore(state.snapshot())

    assert restored.expected_offset(key) == expected_offset
    assert restored.next_ordinal(key) == next_ordinal

    # Continuing from the restored state produces the same output as the live one.
    second = FileSplitterState.chunk_change(7, PATH, FILE_TYPE_TRANSACTIONS, expected_offset, b"tial\n")
    assert restored.simulate(second) == state.simulate(second)


# --------------------------------------------------------------------------- #
# FileSplitter dispatch: addressing, in-order dedup, applied-not-done re-drive
# --------------------------------------------------------------------------- #


class FakeShardedPublisher:
    def __init__(self):
        self.sent = []  # (shard, body)
        self.fail_next = 0

    def send(self, body):
        self.sent.append((None, body))

    def send_to_shard(self, body, shard):
        if self.fail_next > 0:
            self.fail_next -= 1
            raise RuntimeError("publish boom")
        self.sent.append((shard, body))

    def close(self):
        pass


def _make_splitter(tmp_path, accounts=False, shard_count=3, max_batch_bytes=4096):
    config = FileSplitterConfig(
        id=5,
        mom_host="localhost",
        input_exchange="file_ingestor_exchange",
        queue_name="file_splitter_5",
        output_exchange="line_batch_exchange",
        output_routing_prefix="file_ingestor",
        output_shard_count=shard_count,
        max_line_bytes=1024,
        max_batch_bytes=max_batch_bytes,
        logging_level="INFO",
        accounts_output_queue="accounts_line_batch_queue" if accounts else None,
        state_dir=str(tmp_path),
        snapshot_interval=1000,
    )
    splitter = FileSplitter(config)
    splitter._handler = splitter._build_handler()
    splitter._handler.recover()
    publishers = {LINE_BATCH_EDGE: FakeShardedPublisher()}
    if accounts:
        publishers[ACCOUNTS_EDGE] = FakeShardedPublisher()
    splitter._publishers = publishers
    return splitter, publishers


class _Acks:
    def __init__(self):
        self.acks = 0
        self.nacks = 0

    def ack(self, *_a, **_k):
        self.acks += 1

    def nack(self, *_a, **_k):
        self.nacks += 1


def _chunk_msg(client, file_type, offset, data, path=PATH):
    return bytes([MSG_CHUNK]) + FileChunk(
        rel_path=path, client_id=client, file_type=file_type, offset=offset, data=data
    ).serialize()


def _eof_msg(client, file_type, path=PATH):
    return bytes([MSG_EOF]) + FileEof(
        rel_path=path, client_id=client, file_type=file_type
    ).serialize()


def _unpack(body):
    return InternalProtocol().unpack_addressed_packet(body)


def test_dispatch_emits_addressed_transaction_outputs(tmp_path):
    splitter, pubs = _make_splitter(tmp_path)
    acks = _Acks()
    data = HEADER_LINE + b"\n" + ROW_1 + b"\n"

    splitter._on_message(_chunk_msg(7, FILE_TYPE_TRANSACTIONS, 0, data), acks.ack, acks.nack)
    splitter._on_message(_eof_msg(7, FILE_TYPE_TRANSACTIONS), acks.ack, acks.nack)

    assert acks.nacks == 0
    sent = pubs[LINE_BATCH_EDGE].sent
    types = [_unpack(body)[0] for _shard, body in sent]
    assert MessageType.DATA in types and MessageType.EOF in types
    for shard, body in sent:
        msg_type, client_id, sender_id, seq, _payload = _unpack(body)
        assert client_id == 7
        assert sender_id == 5  # config.id


def test_dispatch_accounts_addressing_is_dense(tmp_path):
    splitter, pubs = _make_splitter(tmp_path, accounts=True)
    acks = _Acks()
    path = "LI-Mini_accounts.csv"

    splitter._on_message(
        _chunk_msg(7, FILE_TYPE_ACCOUNTS, 0, b"Bank ID,Bank Name\n001,Raw One\n", path),
        acks.ack,
        acks.nack,
    )
    splitter._on_message(_eof_msg(7, FILE_TYPE_ACCOUNTS, path), acks.ack, acks.nack)

    sent = pubs[ACCOUNTS_EDGE].sent
    assert len(sent) == 2
    d_type, d_client, d_sender, d_seq, d_payload = _unpack(sent[0][1])
    e_type, _c, _s, e_seq, e_payload = _unpack(sent[1][1])
    assert (d_type, d_client, d_sender, d_seq) == (MessageType.DATA, 7, 5, 0)
    assert (e_type, e_seq) == (MessageType.EOF, 1)
    assert LineBatchSerializer.deserialize(d_payload).lines == (b"001,Raw One",)
    assert ControlMessageSerializer().deserialize(e_payload).expected_total == 1


def test_dispatch_dedups_redelivered_chunk(tmp_path):
    splitter, pubs = _make_splitter(tmp_path)
    acks = _Acks()
    data = HEADER_LINE + b"\n" + ROW_1 + b"\n"
    msg = _chunk_msg(7, FILE_TYPE_TRANSACTIONS, 0, data)

    splitter._on_message(msg, acks.ack, acks.nack)
    after_first = len(pubs[LINE_BATCH_EDGE].sent)

    # Exact redelivery of an already-committed chunk: no new outputs, still acked.
    splitter._on_message(msg, acks.ack, acks.nack)
    assert len(pubs[LINE_BATCH_EDGE].sent) == after_first
    assert acks.nacks == 0
    assert acks.acks == 2


def test_dispatch_redrives_applied_but_uncommitted_chunk(tmp_path):
    splitter, pubs = _make_splitter(tmp_path, max_batch_bytes=1)
    acks = _Acks()
    # max_batch_bytes=1 makes the first data line flush a batch *during* the chunk,
    # so the chunk has a publishable output we can fail.
    data = HEADER_LINE + b"\n" + ROW_1 + b"\n" + ROW_2 + b"\n"
    msg = _chunk_msg(7, FILE_TYPE_TRANSACTIONS, 0, data)

    pubs[LINE_BATCH_EDGE].fail_next = 1  # crash the publish after the state was applied
    splitter._on_message(msg, acks.ack, acks.nack)
    assert acks.nacks == 1
    assert acks.acks == 0

    key = FileKey(client_id=7, rel_path=PATH)
    assert splitter._state.expected_offset(key) == len(data)  # state was applied

    # Redelivery with a working publisher re-drives: republishes + commits.
    splitter._on_message(msg, acks.ack, acks.nack)
    assert acks.acks == 1
    assert len(pubs[LINE_BATCH_EDGE].sent) >= 1
    # A second redelivery is now a clean committed-duplicate (no further output).
    before = len(pubs[LINE_BATCH_EDGE].sent)
    splitter._on_message(msg, acks.ack, acks.nack)
    assert len(pubs[LINE_BATCH_EDGE].sent) == before


def test_dispatch_recovers_from_snapshot(tmp_path):
    splitter, pubs = _make_splitter(tmp_path, accounts=True)
    acks = _Acks()
    path = "LI-Mini_accounts.csv"
    splitter._on_message(
        _chunk_msg(7, FILE_TYPE_ACCOUNTS, 0, b"Bank ID,Bank Name\n001,Raw One\n", path),
        acks.ack,
        acks.nack,
    )
    splitter._handler.snapshot_now()

    # Fresh worker over the same state dir recovers the applied chunk: a redelivery
    # of that chunk is recognized as a committed duplicate (no re-emit).
    revived, revived_pubs = _make_splitter(tmp_path, accounts=True)
    racks = _Acks()
    revived._on_message(
        _chunk_msg(7, FILE_TYPE_ACCOUNTS, 0, b"Bank ID,Bank Name\n001,Raw One\n", path),
        racks.ack,
        racks.nack,
    )
    assert revived_pubs[ACCOUNTS_EDGE].sent == []
    assert racks.nacks == 0


def test_sender_id_for_path_is_deterministic():
    assert _sender_id_for_path("a/b.csv") == _sender_id_for_path("a/b.csv")
    assert _sender_id_for_path("a/b.csv") != _sender_id_for_path("a/c.csv")


def test_predeclares_line_batch_bindings(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        file_splitter_module,
        "ensure_exchange_queue_bindings",
        lambda *args: calls.append(args),
    )
    splitter, _ = _make_splitter(tmp_path)
    splitter._ensure_line_batch_bindings()

    assert calls == [
        (
            "localhost",
            "line_batch_exchange",
            {
                "file_ingestor_0": "file_ingestor_0",
                "file_ingestor_1": "file_ingestor_1",
                "file_ingestor_2": "file_ingestor_2",
            },
        )
    ]
