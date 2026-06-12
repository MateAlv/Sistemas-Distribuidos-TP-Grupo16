import threading

import pytest

from monitor import Monitor


class FakeElectionHandler:
    def __init__(
        self,
        leader: bool,
        leader_id: int = 3,
        leader_running: bool | None = None,
    ) -> None:
        self.leader = leader
        self.leader_id = leader_id
        self.leader_running = leader if leader_running is None else leader_running
        self.wait_calls = []
        self.listen_started = threading.Event()
        self.listen_stopped = threading.Event()
        self.stop_calls = 0

    def i_am_leader(self) -> bool:
        return self.leader

    def get_leader(self) -> int:
        return self.leader_id

    def leader_is_running(self) -> bool:
        return self.leader_running

    def wait_for_new_leader(self, timeout: float) -> bool:
        self.wait_calls.append(timeout)
        return False

    def listen_for_election(self) -> None:
        self.listen_started.set()
        self.listen_stopped.wait(timeout=1)

    def stop(self) -> None:
        self.stop_calls += 1
        self.listen_stopped.set()


class FakeHeartbeatReceiver:
    def __init__(self, last_seen=None) -> None:
        self.timestamps = dict(last_seen or {})
        self.listen_started = threading.Event()
        self.listen_stopped = threading.Event()
        self.stop_calls = 0

    def last_seen(self, node_id: str):
        return self.timestamps.get(node_id)

    def listen(self) -> None:
        self.listen_started.set()
        self.listen_stopped.wait(timeout=1)

    def stop(self) -> None:
        self.stop_calls += 1
        self.listen_stopped.set()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("check_interval", 0, "check_interval"),
        ("max_missed", 0, "max_missed"),
        ("coordinator_timeout", 0, "coordinator_timeout"),
    ],
)
def test_rejects_invalid_timing_configuration(
    field: str,
    value: float,
    message: str,
) -> None:
    kwargs = {field: value}
    with pytest.raises(ValueError, match=message):
        Monitor(
            FakeElectionHandler(leader=True),
            FakeHeartbeatReceiver(),
            [],
            **kwargs,
        )


def test_leader_recovers_missing_and_expired_nodes() -> None:
    recovered = []
    heartbeat = FakeHeartbeatReceiver(
        {
            "healthy": 95.0,
            "expired": 60.0,
        }
    )
    monitor = Monitor(
        FakeElectionHandler(leader=True),
        heartbeat,
        ["healthy", "missing", "expired"],
        recovery=recovered.append,
        check_interval=10,
        max_missed=3,
        clock=lambda: 100.0,
    )

    monitor.run_once()

    assert recovered == ["missing", "expired"]


def test_leader_does_not_repeat_recovery_during_cooldown() -> None:
    now = [100.0]
    recovered = []
    monitor = Monitor(
        FakeElectionHandler(leader=True),
        FakeHeartbeatReceiver(),
        ["worker_0"],
        recovery=recovered.append,
        check_interval=10,
        max_missed=3,
        clock=lambda: now[0],
    )

    monitor.run_once()
    now[0] = 120.0
    monitor.run_once()
    now[0] = 131.0
    monitor.run_once()

    assert recovered == ["worker_0", "worker_0"]


def test_fresh_heartbeat_clears_recovery_cooldown() -> None:
    now = [100.0]
    recovered = []
    heartbeat = FakeHeartbeatReceiver()
    monitor = Monitor(
        FakeElectionHandler(leader=True),
        heartbeat,
        ["worker_0"],
        recovery=recovered.append,
        check_interval=10,
        max_missed=3,
        clock=lambda: now[0],
    )

    monitor.run_once()
    heartbeat.timestamps["worker_0"] = 110.0
    now[0] = 115.0
    monitor.run_once()
    heartbeat.timestamps["worker_0"] = 110.0
    now[0] = 150.0
    monitor.run_once()

    assert recovered == ["worker_0", "worker_0"]


def test_recovery_error_does_not_skip_other_failed_nodes() -> None:
    recovered = []

    def recovery(node_id: str) -> None:
        recovered.append(node_id)
        if node_id == "broken_recovery":
            raise RuntimeError("recovery failed")

    monitor = Monitor(
        FakeElectionHandler(leader=True),
        FakeHeartbeatReceiver(),
        ["broken_recovery", "worker_1"],
        recovery=recovery,
        clock=lambda: 100.0,
    )

    monitor.run_once()

    assert recovered == ["broken_recovery", "worker_1"]


def test_follower_waits_when_leader_heartbeat_is_fresh() -> None:
    election = FakeElectionHandler(
        leader=False,
        leader_id=2,
        leader_running=True,
    )
    monitor = Monitor(
        election,
        FakeHeartbeatReceiver({"monitor_2": 95.0}),
        [],
        check_interval=10,
        max_missed=3,
        coordinator_timeout=7,
        clock=lambda: 100.0,
    )

    monitor.run_once()

    assert election.wait_calls == []


def test_follower_waits_for_coordinator_when_leader_is_expired() -> None:
    election = FakeElectionHandler(
        leader=False,
        leader_id=2,
        leader_running=True,
    )
    monitor = Monitor(
        election,
        FakeHeartbeatReceiver({"monitor_2": 50.0}),
        [],
        check_interval=10,
        max_missed=3,
        coordinator_timeout=7,
        clock=lambda: 100.0,
    )

    monitor.run_once()

    assert election.wait_calls == [7]


def test_follower_waits_for_first_coordinator_even_with_fresh_heartbeat() -> None:
    election = FakeElectionHandler(leader=False, leader_id=3)
    monitor = Monitor(
        election,
        FakeHeartbeatReceiver({"monitor_3": 100.0}),
        [],
        coordinator_timeout=7,
        clock=lambda: 100.0,
    )

    monitor.run_once()

    assert election.wait_calls == [7]


def test_run_starts_listeners_and_stops_components() -> None:
    election = FakeElectionHandler(leader=True)
    heartbeat = FakeHeartbeatReceiver({"worker_0": 100.0})
    monitor = Monitor(
        election,
        heartbeat,
        ["worker_0"],
        check_interval=0.01,
        clock=lambda: 100.0,
    )

    thread = threading.Thread(target=monitor.run)
    thread.start()
    assert election.listen_started.wait(timeout=1)
    assert heartbeat.listen_started.wait(timeout=1)

    monitor.stop()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert election.stop_calls == 1
    assert heartbeat.stop_calls == 1


def test_nodes_to_watch_are_deduplicated() -> None:
    recovered = []
    monitor = Monitor(
        FakeElectionHandler(leader=True),
        FakeHeartbeatReceiver(),
        ["worker_0", "worker_0"],
        recovery=recovered.append,
        clock=lambda: 100.0,
    )

    monitor.run_once()

    assert recovered == ["worker_0"]
