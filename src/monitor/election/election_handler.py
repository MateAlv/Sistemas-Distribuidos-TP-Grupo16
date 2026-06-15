import logging
import socket
import threading
from collections.abc import Callable

from monitor.election.epoch_store import EpochStore, MAX_EPOCH
from monitor.election.messages import ElectionMessage, ElectionMessageType


DEFAULT_ELECTION_HOST = "0.0.0.0"
DEFAULT_ELECTION_PORT = 9001
DEFAULT_ELECTION_TIMEOUT = 5.0
LISTEN_BACKLOG = 5


class ElectionHandler:
    def __init__(
        self,
        monitor_id: int,
        monitor_count: int,
        host: str = DEFAULT_ELECTION_HOST,
        port: int = DEFAULT_ELECTION_PORT,
        election_timeout: float = DEFAULT_ELECTION_TIMEOUT,
        message_sender: Callable[[int, ElectionMessage, bool], ElectionMessage | None] | None = None,
        epoch_store: EpochStore | None = None,
    ) -> None:
        if monitor_count < 1 or monitor_count > 255:
            raise ValueError("monitor_count must be in range [1, 255]")
        if not 1 <= monitor_id <= monitor_count:
            raise ValueError("monitor_id must be in range [1, monitor_count]")
        if election_timeout <= 0:
            raise ValueError("election_timeout must be greater than 0")

        self._monitor_id = monitor_id
        self._monitor_count = monitor_count
        self._host = host
        self._port = port
        self._election_timeout = election_timeout
        self._message_sender = message_sender or self._send_tcp_message
        self._epoch_store = epoch_store

        self._leader = monitor_count
        self._epoch = epoch_store.load() if epoch_store is not None else 0
        self._election_generation = 0
        self._leader_running = False
        self._leader_lock = threading.Lock()
        self._election_lock = threading.Lock()
        self._leader_semaphore = threading.Semaphore(0)
        self._stop_event = threading.Event()
        self._server_lock = threading.Lock()
        self._server_socket: socket.socket | None = None

    def start_election(self) -> None:
        if self._stop_event.is_set():
            return
        if not self._election_lock.acquire(blocking=False):
            return

        try:
            with self._leader_lock:
                self._election_generation += 1
                generation = self._election_generation
                self._leader_running = False
                epoch = self._epoch

            logging.info(
                "monitor_election_started | monitor_id=%s | epoch=%s | "
                "generation=%s",
                self._monitor_id,
                epoch,
                generation,
            )
            election = ElectionMessage(
                ElectionMessageType.ELECTION,
                epoch,
                self._monitor_id,
            )
            higher_monitor_answered = False

            for peer_id in range(1, self._monitor_count + 1):
                if peer_id == self._monitor_id:
                    continue
                try:
                    response = self._message_sender(peer_id, election, True)
                except OSError as exc:
                    logging.info(
                        "monitor_election_peer_unavailable | monitor_id=%s | "
                        "peer_id=%s | error=%s",
                        self._monitor_id,
                        peer_id,
                        exc,
                    )
                    continue

                if (
                    response is not None
                    and response.message_type == ElectionMessageType.OK
                    and response.sender_id == peer_id
                ):
                    with self._leader_lock:
                        self._set_epoch_locked(max(self._epoch, response.epoch))
                    if peer_id > self._monitor_id:
                        higher_monitor_answered = True

            if not higher_monitor_answered:
                self._become_leader(generation)
        finally:
            self._election_lock.release()

    def send_coordinator(self) -> None:
        with self._leader_lock:
            epoch = self._epoch

        coordinator = ElectionMessage(
            ElectionMessageType.COORDINATOR,
            epoch,
            self._monitor_id,
        )
        for peer_id in range(1, self._monitor_id):
            try:
                self._message_sender(peer_id, coordinator, False)
            except OSError as exc:
                logging.info(
                    "monitor_coordinator_peer_unavailable | monitor_id=%s | "
                    "peer_id=%s | error=%s",
                    self._monitor_id,
                    peer_id,
                    exc,
                )

    def listen_for_election(self) -> None:
        if self._stop_event.is_set():
            return

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with self._server_lock:
            if self._stop_event.is_set():
                server.close()
                return
            self._server_socket = server

        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self._host, self._port))
            server.listen(LISTEN_BACKLOG)
            server.settimeout(0.5)
            logging.info(
                "monitor_election_listen | monitor_id=%s | host=%s | port=%s",
                self._monitor_id,
                self._host,
                self._port,
            )

            while not self._stop_event.is_set():
                try:
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self._stop_event.is_set():
                        return
                    raise

                with connection:
                    connection.settimeout(self._election_timeout)
                    try:
                        self._handle_connection(connection)
                    except (OSError, ValueError) as exc:
                        logging.warning(
                            "monitor_election_connection_error | monitor_id=%s | error=%s",
                            self._monitor_id,
                            exc,
                        )
        finally:
            with self._server_lock:
                if self._server_socket is server:
                    self._server_socket = None
            server.close()

    def i_am_leader(self) -> bool:
        with self._leader_lock:
            return self._leader_running and self._leader == self._monitor_id

    def get_leader(self) -> int:
        with self._leader_lock:
            return self._leader

    def leader_is_running(self) -> bool:
        with self._leader_lock:
            return self._leader_running

    def wait_for_new_leader(self, timeout: float) -> bool:
        acquired = self._leader_semaphore.acquire(timeout=timeout)
        if not acquired:
            self.start_election()
        return acquired

    def stop(self) -> None:
        self._stop_event.set()
        self._leader_semaphore.release()
        with self._leader_lock:
            self._election_generation += 1
        with self._server_lock:
            server = self._server_socket
        if server is not None:
            try:
                server.close()
            except OSError:
                pass

    def _become_leader(self, generation: int) -> bool:
        with self._leader_lock:
            if (
                self._stop_event.is_set()
                or generation != self._election_generation
                or self._leader_running
            ):
                logging.info(
                    "monitor_election_cancelled | monitor_id=%s | "
                    "generation=%s | current_generation=%s | leader_id=%s",
                    self._monitor_id,
                    generation,
                    self._election_generation,
                    self._leader,
                )
                return False

            if self._epoch == MAX_EPOCH:
                raise RuntimeError("monitor election epoch exhausted")
            self._set_epoch_locked(self._epoch + 1)
            self._leader = self._monitor_id
            self._leader_running = True
            epoch = self._epoch

        logging.info(
            "monitor_election_won | monitor_id=%s | epoch=%s",
            self._monitor_id,
            epoch,
        )
        self.send_coordinator()
        return True

    def _handle_connection(self, connection: socket.socket) -> None:
        message = ElectionMessage.deserialize(_recv_exact(connection, 4))
        response, should_start_election = self._handle_message(message)
        if response is not None:
            connection.sendall(response.serialize())
        if should_start_election:
            self._start_election_async()

    def _start_election_async(self) -> None:
        thread = threading.Thread(
            target=self.start_election,
            name=f"monitor-election-{self._monitor_id}",
            daemon=True,
        )
        thread.start()

    def _handle_message(
        self,
        message: ElectionMessage,
    ) -> tuple[ElectionMessage | None, bool]:
        with self._leader_lock:
            if message.message_type == ElectionMessageType.COORDINATOR:
                if (
                    message.epoch < self._epoch
                    or (
                        message.epoch == self._epoch
                        and self._leader_running
                        and message.sender_id < self._leader
                    )
                ):
                    logging.info(
                        "monitor_coordinator_ignored | monitor_id=%s | "
                        "leader_id=%s | epoch=%s | sender_id=%s | "
                        "announced_epoch=%s",
                        self._monitor_id,
                        self._leader,
                        self._epoch,
                        message.sender_id,
                        message.epoch,
                    )
                    return None, False

                self._election_generation += 1
                self._set_epoch_locked(max(self._epoch, message.epoch))
                self._leader = message.sender_id
                self._leader_running = True
                self._leader_semaphore.release()
                logging.info(
                    "monitor_coordinator_accepted | monitor_id=%s | "
                    "leader_id=%s | epoch=%s | announced_epoch=%s",
                    self._monitor_id,
                    message.sender_id,
                    self._epoch,
                    message.epoch,
                )
                return None, False

            if message.message_type == ElectionMessageType.ELECTION:
                self._set_epoch_locked(max(self._epoch, message.epoch))
                should_start_election = message.sender_id < self._monitor_id
                response = ElectionMessage(
                    ElectionMessageType.OK,
                    self._epoch,
                    self._monitor_id,
                )
                return response, should_start_election

        return None, False

    def _set_epoch_locked(self, epoch: int) -> None:
        if epoch == self._epoch:
            return
        if self._epoch_store is not None:
            self._epoch_store.save(epoch)
        self._epoch = epoch

    def _send_tcp_message(
        self,
        peer_id: int,
        message: ElectionMessage,
        expect_response: bool,
    ) -> ElectionMessage | None:
        address = (f"monitor_{peer_id}", self._port)
        with socket.create_connection(
            address,
            timeout=self._election_timeout,
        ) as connection:
            connection.settimeout(self._election_timeout)
            connection.sendall(message.serialize())
            if not expect_response:
                return None
            return ElectionMessage.deserialize(_recv_exact(connection, 4))


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if not chunk:
            raise ConnectionError("connection closed before election message")
        data.extend(chunk)
    return bytes(data)
