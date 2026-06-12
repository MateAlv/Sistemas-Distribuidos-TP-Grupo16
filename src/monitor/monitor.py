import logging
import threading
import time
from collections.abc import Callable, Iterable

from monitor.election import ElectionHandler
from monitor.heartbeat import HeartbeatReceiver
from monitor.recovery import RecoveryFn, docker_start


DEFAULT_CHECK_INTERVAL = 3.0
DEFAULT_MAX_MISSED = 3
DEFAULT_COORDINATOR_TIMEOUT = 10.0
THREAD_JOIN_TIMEOUT = 5.0

Clock = Callable[[], float]


class Monitor:
    def __init__(
        self,
        election_handler: ElectionHandler,
        heartbeat_receiver: HeartbeatReceiver,
        nodes_to_watch: Iterable[str],
        recovery: RecoveryFn = docker_start,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
        max_missed: int = DEFAULT_MAX_MISSED,
        coordinator_timeout: float = DEFAULT_COORDINATOR_TIMEOUT,
        clock: Clock = time.time,
    ) -> None:
        if check_interval <= 0:
            raise ValueError("check_interval must be greater than 0")
        if max_missed < 1:
            raise ValueError("max_missed must be at least 1")
        if coordinator_timeout <= 0:
            raise ValueError("coordinator_timeout must be greater than 0")

        self._election_handler = election_handler
        self._heartbeat_receiver = heartbeat_receiver
        self._nodes_to_watch = tuple(dict.fromkeys(nodes_to_watch))
        self._recovery = recovery
        self._check_interval = check_interval
        self._failure_timeout = max_missed * check_interval
        self._coordinator_timeout = coordinator_timeout
        self._clock = clock

        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._last_recovery_by_node: dict[str, float] = {}

    def run(self) -> None:
        self._start_listeners()
        try:
            while not self._stop_event.is_set():
                self.run_once()
                self._stop_event.wait(self._check_interval)
        finally:
            self.stop()
            self._join_listeners()

    def run_once(self) -> None:
        if self._stop_event.is_set():
            return

        if self._election_handler.i_am_leader():
            self._recover_failed_nodes()
            return

        if self._leader_is_alive():
            return

        self._election_handler.wait_for_new_leader(
            self._coordinator_timeout
        )

    def stop(self) -> None:
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        self._election_handler.stop()
        self._heartbeat_receiver.stop()

    def _start_listeners(self) -> None:
        if self._threads:
            return

        self._threads = [
            threading.Thread(
                target=self._election_handler.listen_for_election,
                name="monitor-election-listener",
            ),
            threading.Thread(
                target=self._heartbeat_receiver.listen,
                name="monitor-heartbeat-receiver",
            ),
        ]
        for thread in self._threads:
            thread.start()

    def _join_listeners(self) -> None:
        for thread in self._threads:
            thread.join(timeout=THREAD_JOIN_TIMEOUT)
            if thread.is_alive():
                logging.warning(
                    "monitor_thread_join_timeout | thread=%s",
                    thread.name,
                )

    def _leader_is_alive(self) -> bool:
        leader_id = self._election_handler.get_leader()
        return not self._is_failed(f"monitor_{leader_id}")

    def _recover_failed_nodes(self) -> None:
        now = self._clock()
        for node_id in self._nodes_to_watch:
            if not self._is_failed(node_id, now):
                self._last_recovery_by_node.pop(node_id, None)
                continue

            last_recovery = self._last_recovery_by_node.get(node_id)
            if (
                last_recovery is not None
                and now - last_recovery <= self._failure_timeout
            ):
                continue

            logging.warning(
                "monitor_node_failed | node_id=%s",
                node_id,
            )
            try:
                self._recovery(node_id)
            except Exception:
                logging.exception(
                    "monitor_recovery_unexpected_error | node_id=%s",
                    node_id,
                )
            self._last_recovery_by_node[node_id] = now

    def _is_failed(
        self,
        node_id: str,
        now: float | None = None,
    ) -> bool:
        last_seen = self._heartbeat_receiver.last_seen(node_id)
        if last_seen is None:
            return True
        current_time = self._clock() if now is None else now
        return current_time - last_seen > self._failure_timeout
