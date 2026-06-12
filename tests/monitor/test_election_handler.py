import pytest

from monitor.election import (
    ElectionHandler,
    ElectionMessage,
    ElectionMessageType,
)


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


@pytest.mark.parametrize("monitor_count", [0, 256])
def test_rejects_monitor_count_outside_protocol_range(
    monitor_count: int,
) -> None:
    with pytest.raises(ValueError, match="monitor_count must be in range"):
        ElectionHandler(monitor_id=1, monitor_count=monitor_count)


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
