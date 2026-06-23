"""End-to-end graceful-shutdown test for the gateway.

Exercises the real shutdown path: a real loopback client connects and parks in
_wait_for_results, then stop() (what the SIGTERM handler calls) must return the
run loop, unblock and close the client socket, and stop the result consumer on
its own thread. Only the broker-facing middleware is faked; the TCP server and
all threads are real.
"""

import socket
import threading
import time
import queue

import gateway.gateway as gw_module
from common.domain.account import Q2BankMaxResult
from gateway.gateway import Gateway, GatewayConfig

HANDSHAKE = 1
FINISH = 3
ACK = 4


class _FakePublisher:
    """Stand-in for the per-client FileIngestor exchange publisher."""

    def __init__(self, *args, **kwargs):
        self.closed = False

    def send(self, *args, **kwargs):
        pass

    def close(self):
        self.closed = True


class _ImmediateConsumer:
    def __init__(self, messages):
        self.messages = messages
        self.closed = False
        self.acks = 0
        self.nacks = 0

    def start_consuming(self, callback):
        for message in self.messages:
            callback(message, self._ack, self._nack)

    def _ack(self):
        self.acks += 1

    def _nack(self, *_args, **_kwargs):
        self.nacks += 1

    def close(self):
        self.closed = True


def _config() -> GatewayConfig:
    return GatewayConfig(
        server_host="127.0.0.1",
        server_port=0,  # ephemeral; real port read back after bind
        mom_host="unused",
        file_ingestor_exchange="file_ingestor_exchange",
        file_ingestor_partitions=1,
        file_splitter_queue_prefix="file_splitter",
        logging_level="WARNING",
    )


def _wait_for_bound_port(gw: Gateway, timeout: float = 3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sock = gw._server_sock
        if sock is not None:
            try:
                port = sock.getsockname()[1]
            except OSError:
                port = 0
            if port:
                return port
        time.sleep(0.01)
    return None


def test_gateway_sigterm_unblocks_client_and_stops_consumers(pika_env, monkeypatch):
    # Single result consumer (Q1 only).
    monkeypatch.setenv("GATEWAY_Q1_ENABLED", "1")
    monkeypatch.setenv("GATEWAY_QUEUE", "gateway_results_queue")
    for var in ("GATEWAY_Q2_QUEUE", "GATEWAY_Q3_QUEUE", "GATEWAY_Q4_QUEUE", "GATEWAY_Q5_QUEUE"):
        monkeypatch.delenv(var, raising=False)

    created_consumers = []

    def fake_queue(_host, _queue_name):
        # Long block_timeout so the fake never self-releases before stop().
        consumer = pika_env.BlockingFakeConsumer(block_timeout=30)
        created_consumers.append(consumer)
        return consumer

    monkeypatch.setattr(gw_module, "ensure_exchange_queue_bindings", lambda *a, **k: None)
    monkeypatch.setattr(gw_module, "MessageMiddlewareQueueRabbitMQ", fake_queue)
    monkeypatch.setattr(gw_module, "MessageMiddlewareExchangeRabbitMQ", _FakePublisher)

    gw = Gateway(_config())

    run_done = threading.Event()

    def run():
        try:
            gw.run()
        finally:
            run_done.set()

    runner = threading.Thread(target=run, name="gateway-run")
    runner.start()
    try:
        port = _wait_for_bound_port(gw)
        assert port is not None, "gateway never bound its listening socket"

        # Real client: handshake, finish, then park in _wait_for_results.
        client = socket.create_connection(("127.0.0.1", port), timeout=2)
        client.settimeout(2)
        client.sendall(bytes([HANDSHAKE]) + (7).to_bytes(4, "big"))
        assert client.recv(1) == bytes([ACK]), "no handshake ack"
        client.sendall(bytes([FINISH]))
        assert client.recv(1) == bytes([ACK]), "no finish ack"

        # Consumer thread is actually consuming before we signal shutdown.
        assert created_consumers, "result consumer was never created"
        assert created_consumers[0].wait_started(2), "consumer never started"

        # SIGTERM-equivalent.
        gw.stop()

        assert run_done.wait(timeout=10), "gateway.run() did not return after stop()"

        # Gateway closed the client socket -> client sees EOF.
        client.settimeout(2)
        assert client.recv(1) == b"", "client socket was not closed by gateway"

        # Result consumer was stopped on its own thread and its connection closed.
        assert created_consumers[0].stop_calls == 1
        assert created_consumers[0].closed
    finally:
        gw.stop()
        runner.join(timeout=5)
        try:
            client.close()
        except (OSError, NameError, UnboundLocalError):
            pass

    assert not runner.is_alive(), "gateway run thread still alive after shutdown"


def test_gateway_dedups_addressed_q2_result_redelivery(monkeypatch):
    monkeypatch.setenv("GATEWAY_Q1_ENABLED", "0")
    gw = Gateway(_config())
    client_id = 7
    result_queue = queue.Queue()
    gw._client_queues[client_id] = result_queue

    payload = gw_module.Q2BankMaxResultSerializer.serialize(
        Q2BankMaxResult(
            bank_id="1",
            from_account="acc-1",
            bank_name="Raw One",
            amount=10.0,
        )
    )
    packet = gw_module.InternalProtocol.create_addressed_packet(
        msg_type=gw_module.MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=0,
        seq=0,
        payload=payload,
    )
    consumer = _ImmediateConsumer([packet, packet])

    gw._run_result_consumer(consumer, gw._q2_csv_lines, "Q2|", addressed=True)

    assert consumer.acks == 2
    assert consumer.nacks == 0
    assert consumer.closed
    assert result_queue.get_nowait() == "Q2|1,acc-1,Raw One,10.00\n"
    assert result_queue.empty()
