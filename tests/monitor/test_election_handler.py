import threading

import pytest

from monitor.election import (
    ElectionHandler,
    ElectionMessage,
    ElectionMessageType,
)
from monitor.election.epoch_store import EpochStore, MAX_EPOCH


class FakeSender:
    def __init__(self, responses=None, unavailable=None) -> None:
        self.responses = responses or {}
        self.unavailable = set(unavailable or [])
        self.sent = []

    def __call__(self, peer_id, message, expect_response):
        self.sent.append((peer_id, message, expect_response))
        if peer_id in self.unavailable:
            raise ConnectionRefusedError(f"monitor {peer_id} unavailable")
        return self.responses.get(peer_id)


def test_rejects_monitor_count_outside_protocol_range() -> None:
    with pytest.raises(ValueError, match="monitor_count must be in range"):
        ElectionHandler(monitor_id=1, monitor_count=256)


def test_rejects_non_positive_election_timeout() -> None:
    with pytest.raises(ValueError, match="election_timeout"):
        ElectionHandler(
            monitor_id=1,
            monitor_count=1,
            election_timeout=0,
        )


def test_highest_active_monitor_wins_and_announces_coordinator() -> None:
    sender = FakeSender()
    handler = ElectionHandler(
        monitor_id=3,
        monitor_count=3,
        message_sender=sender,
    )

    handler.start_election()

    assert handler.i_am_leader()
    assert handler.get_leader() == 3
    assert [
        (peer_id, message.message_type, expect_response)
        for peer_id, message, expect_response in sender.sent
    ] == [
        (1, ElectionMessageType.COORDINATOR, False),
        (2, ElectionMessageType.COORDINATOR, False),
    ]
    assert all(message.epoch == 1 for _, message, _ in sender.sent)


def test_epoch_survives_handler_restart(tmp_path) -> None:
    state_path = tmp_path / "epoch.json"
    first_handler = ElectionHandler(
        monitor_id=3,
        monitor_count=3,
        message_sender=FakeSender(),
        epoch_store=EpochStore(state_path),
    )
    first_handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=5,
            sender_id=2,
        )
    )

    sender = FakeSender()
    restarted_handler = ElectionHandler(
        monitor_id=3,
        monitor_count=3,
        message_sender=sender,
        epoch_store=EpochStore(state_path),
    )
    restarted_handler.start_election()

    assert restarted_handler.i_am_leader()
    assert restarted_handler._epoch == 6
    assert all(message.epoch == 6 for _, message, _ in sender.sent)


def test_coordinator_epoch_is_persisted(tmp_path) -> None:
    state_path = tmp_path / "epoch.json"
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=3,
        message_sender=FakeSender(),
        epoch_store=EpochStore(state_path),
    )

    handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=8,
            sender_id=3,
        )
    )

    assert EpochStore(state_path).load() == 8


def test_monitor_does_not_lead_after_epoch_is_exhausted(tmp_path) -> None:
    state_path = tmp_path / "epoch.json"
    EpochStore(state_path).save(MAX_EPOCH)
    sender = FakeSender()
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=1,
        message_sender=sender,
        epoch_store=EpochStore(state_path),
    )

    with pytest.raises(RuntimeError, match="epoch exhausted"):
        handler.start_election()

    assert not handler.i_am_leader()
    assert sender.sent == []


def test_monitor_wins_when_all_higher_monitors_are_unavailable() -> None:
    sender = FakeSender(unavailable={2, 3})
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=3,
        message_sender=sender,
    )

    handler.start_election()

    assert handler.i_am_leader()
    assert handler.get_leader() == 1
    assert [peer_id for peer_id, _, _ in sender.sent[:2]] == [2, 3]


def test_monitor_waits_when_higher_monitor_answers_ok() -> None:
    sender = FakeSender(
        responses={
            3: ElectionMessage(ElectionMessageType.OK, epoch=0, sender_id=3)
        },
        unavailable={2},
    )
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=3,
        message_sender=sender,
    )

    handler.start_election()

    assert not handler.i_am_leader()
    assert not handler.leader_is_running()
    assert handler.get_leader() == 3


def test_coordinator_updates_leader_and_releases_waiter() -> None:
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=3,
        message_sender=FakeSender(),
    )

    response, should_elect = handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=4,
            sender_id=2,
        )
    )

    assert response is None
    assert not should_elect
    assert handler.get_leader() == 2
    assert handler.leader_is_running()
    assert not handler.i_am_leader()
    assert handler.wait_for_new_leader(timeout=0)


def test_stale_message_is_ignored() -> None:
    handler = ElectionHandler(
        monitor_id=2,
        monitor_count=3,
        message_sender=FakeSender(),
    )
    handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=5,
            sender_id=3,
        )
    )

    response, should_elect = handler._handle_message(
        ElectionMessage(
            ElectionMessageType.ELECTION,
            epoch=4,
            sender_id=1,
        )
    )

    assert response is None
    assert not should_elect
    assert handler.get_leader() == 3


def test_restarted_higher_monitor_can_reclaim_leadership() -> None:
    handler = ElectionHandler(
        monitor_id=2,
        monitor_count=3,
        message_sender=FakeSender(),
    )
    handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=5,
            sender_id=2,
        )
    )

    response, should_elect = handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=1,
            sender_id=3,
        )
    )

    assert response is None
    assert not should_elect
    assert handler.get_leader() == 3
    assert handler.leader_is_running()
    assert handler._epoch == 5


def test_stale_lower_coordinator_does_not_replace_running_leader() -> None:
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=3,
        message_sender=FakeSender(),
    )
    handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=5,
            sender_id=3,
        )
    )

    handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=4,
            sender_id=2,
        )
    )

    assert handler.get_leader() == 3
    assert handler._epoch == 5


def test_stale_lower_coordinator_is_ignored_during_election() -> None:
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=3,
        message_sender=FakeSender(),
    )
    handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=5,
            sender_id=3,
        )
    )
    with handler._leader_lock:
        handler._leader_running = False

    handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=4,
            sender_id=2,
        )
    )

    assert handler.get_leader() == 3
    assert not handler.leader_is_running()
    assert handler._epoch == 5


def test_running_monitor_answers_election_and_starts_own_round() -> None:
    sender = FakeSender()
    handler = ElectionHandler(
        monitor_id=2,
        monitor_count=2,
        message_sender=sender,
    )
    handler.start_election()
    sender.sent.clear()

    response, should_elect = handler._handle_message(
        ElectionMessage(
            ElectionMessageType.ELECTION,
            epoch=1,
            sender_id=1,
        )
    )

    assert response == ElectionMessage(
        ElectionMessageType.OK,
        epoch=1,
        sender_id=2,
    )
    assert should_elect


def test_coordinator_invalidates_election_in_progress() -> None:
    sender_entered = threading.Event()
    release_sender = threading.Event()

    def blocking_sender(peer_id, message, expect_response):
        sender_entered.set()
        assert release_sender.wait(timeout=1)
        raise ConnectionRefusedError(f"monitor {peer_id} unavailable")

    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=3,
        message_sender=blocking_sender,
    )
    election = threading.Thread(target=handler.start_election)
    election.start()
    assert sender_entered.wait(timeout=1)

    handler._handle_message(
        ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch=1,
            sender_id=3,
        )
    )
    release_sender.set()
    election.join(timeout=1)

    assert not election.is_alive()
    assert not handler.i_am_leader()
    assert handler.get_leader() == 3
    assert handler.leader_is_running()


def test_connection_starts_election_asynchronously(monkeypatch) -> None:
    election_started = threading.Event()
    release_election = threading.Event()

    class FakeConnection:
        def __init__(self, payload: bytes) -> None:
            self.payload = bytearray(payload)
            self.sent = bytearray()

        def recv(self, size: int) -> bytes:
            chunk = self.payload[:size]
            del self.payload[:size]
            return bytes(chunk)

        def sendall(self, payload: bytes) -> None:
            self.sent.extend(payload)

    handler = ElectionHandler(
        monitor_id=2,
        monitor_count=2,
        message_sender=FakeSender(),
    )
    with handler._leader_lock:
        handler._leader = 2
        handler._leader_running = True

    def blocking_election() -> None:
        election_started.set()
        release_election.wait(timeout=1)

    monkeypatch.setattr(handler, "start_election", blocking_election)
    connection = FakeConnection(
        ElectionMessage(
            ElectionMessageType.ELECTION,
            epoch=0,
            sender_id=1,
        ).serialize()
    )

    handler._handle_connection(connection)

    assert election_started.wait(timeout=1)
    assert ElectionMessage.deserialize(bytes(connection.sent)) == ElectionMessage(
        ElectionMessageType.OK,
        epoch=0,
        sender_id=2,
    )
    release_election.set()


def test_wait_timeout_starts_election() -> None:
    sender = FakeSender()
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=1,
        message_sender=sender,
    )

    assert not handler.wait_for_new_leader(timeout=0)
    assert handler.i_am_leader()


def test_stop_releases_waiter_without_starting_election() -> None:
    sender = FakeSender()
    handler = ElectionHandler(
        monitor_id=1,
        monitor_count=1,
        message_sender=sender,
    )

    handler.stop()

    assert handler.wait_for_new_leader(timeout=0)
    assert not handler.i_am_leader()
    assert sender.sent == []
