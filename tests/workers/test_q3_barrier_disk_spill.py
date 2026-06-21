"""
Tests para el spill a disco del q3_barrier (ADR: q3-barrier-state-store.md).

Invariantes verificadas:
    1. _ClientDiskLog roundtrip: lo que se escribe se lee idéntico.
    2. _process_candidate_message NO desearializa al recibir (escribe bytes crudos).
    3. _emit_client lee del disco con generator, no acumula en lista, y emite
       solo las transacciones que pasan el filtro amount < avg/100.
    4. El TemporaryFile se cierra (file descriptor liberado) tras el emit.
    5. close() del worker limpia estados pendientes.
"""
import importlib
import sys
import types

from common.domain.partial_result import Q3AverageResult
from common.domain.transaction import Transaction
from common.message_protocol.internal import (
    ControlMessage,
    ControlMessageSerializer,
    InternalProtocol,
    Q3AverageResultSerializer,
    TransactionSerializer,
)
from common.message_protocol.internal.common import MessageType


class FakeQueue:
    def __init__(self, *args, **kwargs):
        self.sent = []

    def send(self, message):
        self.sent.append(message)

    def close(self):
        pass

    def start_consuming(self, callback):
        pass

    def stop_consuming(self):
        pass


def _import_barrier_module(monkeypatch):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("Q3_AVERAGES_QUEUE", "q3_averages_0")
    monkeypatch.setenv("Q3_CANDIDATES_QUEUE", "q3_candidates_0")
    monkeypatch.setenv("GATEWAY_Q3_QUEUE", "gateway_q3_results_queue")
    monkeypatch.setenv("Q3_THRESHOLD_DIVISOR", "100")
    monkeypatch.setenv("Q3_BARRIER_AMOUNT", "1")
    monkeypatch.setenv("Q3_AVERAGES_EXCHANGE", "q3_averages_exchange")
    monkeypatch.setenv("Q3_CANDIDATES_EXCHANGE", "q3_candidates_exchange")
    monkeypatch.setenv("Q3_AVERAGES_ROUTING_PREFIX", "q3_averages")
    monkeypatch.setenv("Q3_CANDIDATES_ROUTING_PREFIX", "q3_candidates")

    fake_pika = types.SimpleNamespace(
        exceptions=types.SimpleNamespace(
            AMQPConnectionError=Exception,
            AMQPChannelError=Exception,
            StreamLostError=Exception,
        ),
        BasicProperties=lambda *args, **kwargs: None,
        BlockingConnection=lambda *args, **kwargs: None,
        ConnectionParameters=lambda *args, **kwargs: None,
    )
    monkeypatch.setitem(sys.modules, "pika", fake_pika)
    sys.modules.pop("workers.q3_barrier.q3_barrier", None)
    module = importlib.import_module("workers.q3_barrier.q3_barrier")
    monkeypatch.setattr(
        module.middleware, "MessageMiddlewareQueueRabbitMQ", FakeQueue
    )
    monkeypatch.setattr(
        module.middleware, "MessageMiddlewareExchangeRabbitMQ", FakeQueue
    )
    return module


def _tx(amount: float, fmt: str = "Wire") -> Transaction:
    return Transaction(
        date="2022/09/07 00:08",
        from_bank="1",
        from_account="abc",
        to_bank="2",
        to_account="def",
        amount=amount,
        currency="US Dollar",
        format=fmt,
    )


def _pack_data(proto: InternalProtocol, client_id: int, payload: bytes) -> bytes:
    return proto.create_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, "big"),
        payload=payload,
    )


def _pack_eof(proto: InternalProtocol, client_id: int, payload: bytes) -> bytes:
    return proto.create_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, "big"),
        payload=payload,
    )


# ---------------------------------------------------------------------------
# Invariante 1: _ClientDiskLog roundtrip
# ---------------------------------------------------------------------------
def test_disk_log_roundtrip_preserves_bytes_in_order(monkeypatch):
    module = _import_barrier_module(monkeypatch)
    log = module._ClientDiskLog()
    try:
        payloads = [b"\x00\x01\x02", b"hello world", b"x" * 1024, b""]
        for p in payloads:
            log.append(p)

        read_back = list(log.iter_raw_batches())
        assert read_back == payloads
        assert log.batch_count == len(payloads)
        assert log.byte_count == sum(len(p) for p in payloads)
    finally:
        log.close()


def test_disk_log_iter_is_a_generator_not_a_list(monkeypatch):
    """
    Invariante crítica del ADR: nunca debe existir un list() del right side.
    iter_raw_batches debe devolver un generator (lazy), no una lista materializada.
    """
    module = _import_barrier_module(monkeypatch)
    log = module._ClientDiskLog()
    try:
        log.append(b"a")
        log.append(b"b")

        it = log.iter_raw_batches()
        import types as _types
        assert isinstance(it, _types.GeneratorType), (
            "iter_raw_batches debe ser generator para mantener O(1) RAM"
        )
    finally:
        log.close()


def test_disk_log_close_releases_file(monkeypatch):
    module = _import_barrier_module(monkeypatch)
    log = module._ClientDiskLog()
    log.append(b"data")
    file_obj = log._file
    log.close()
    assert file_obj.closed, "TemporaryFile debe quedar cerrado tras close()"


# ---------------------------------------------------------------------------
# Invariante 2: el barrier NO desearializa candidatos al recibirlos
# ---------------------------------------------------------------------------
def test_candidate_message_stored_raw_without_deserialization(monkeypatch):
    """
    _process_candidate_message debe escribir el payload crudo al disco.
    Verificamos comparando bytes: el primer record del log debe ser exactamente
    el payload recibido (no una re-serialización).
    """
    module = _import_barrier_module(monkeypatch)
    worker = module.Q3BarrierWorker()
    proto = InternalProtocol()
    tx_ser = TransactionSerializer()

    txs = [_tx(amount=10.0, fmt="Wire"), _tx(amount=20.0, fmt="ACH")]
    raw_batch = tx_ser.serialize_batch(txs)
    msg = _pack_data(proto, client_id=42, payload=raw_batch)

    worker._process_candidate_message(msg)

    state = worker.states_by_client[42]
    stored = list(state.disk_log.iter_raw_batches())
    assert len(stored) == 1
    assert stored[0] == raw_batch, (
        "el payload debe guardarse byte-a-byte sin pasar por deserialize_batch"
    )

    worker.close()


# ---------------------------------------------------------------------------
# Invariante 3: _emit_client filtra correctamente desde disco
# ---------------------------------------------------------------------------
def test_emit_client_filters_using_disk_log(monkeypatch):
    """
    avg(Wire) = 1000 → threshold = 1000/100 = 10. Solo amount < 10 pasa.
    Candidatos: [5 Wire (pasa), 15 Wire (no), 0.5 ACH (pasa, avg ACH=200)].
    avg(ACH) = 200 → threshold = 2.
    """
    module = _import_barrier_module(monkeypatch)
    worker = module.Q3BarrierWorker()
    proto = InternalProtocol()
    tx_ser = TransactionSerializer()
    avg_ser = Q3AverageResultSerializer
    ctrl_ser = ControlMessageSerializer()

    client_id = 7

    # 1) Promedios para Wire y ACH
    for fmt, avg in [("Wire", 1000.0), ("ACH", 200.0)]:
        avg_payload = avg_ser.serialize(
            Q3AverageResult(payment_format=fmt, average=avg)
        )
        worker._process_average_message(_pack_data(proto, client_id, avg_payload))

    # 2) Candidatos (batch único de 3 transacciones)
    candidates = [
        _tx(amount=5.0, fmt="Wire"),    # pasa: 5 < 10
        _tx(amount=15.0, fmt="Wire"),   # no:   15 >= 10
        _tx(amount=0.5, fmt="ACH"),     # pasa: 0.5 < 2
    ]
    raw_batch = tx_ser.serialize_batch(candidates)
    worker._process_candidate_message(_pack_data(proto, client_id, raw_batch))

    # 3) EOFs → ready para emit
    eof_ctrl = ctrl_ser.serialize(
        ControlMessage(sender_id=0, expected_total=3, processed_count=0)
    )
    worker._process_average_message(_pack_eof(proto, client_id, eof_ctrl))
    ready = worker._process_candidate_message(_pack_eof(proto, client_id, eof_ctrl))
    assert ready is True

    # 4) Capturar el output y verificar contenido
    captured_outputs = []

    class CapturingQueue(FakeQueue):
        def send(self, message):
            captured_outputs.append(message)

    out_queue = CapturingQueue()
    worker._emit_client(client_id, out_queue)

    # Esperado: 1 DATA con 2 transacciones (Wire=5 y ACH=0.5) + 1 EOF
    assert len(captured_outputs) == 2
    data_msg, eof_msg = captured_outputs

    data_type, data_cid, data_payload = proto.unpack_packet(data_msg)
    assert data_type == MessageType.DATA
    assert data_cid == client_id
    emitted_txs = tx_ser.deserialize_batch(data_payload)
    assert len(emitted_txs) == 2
    emitted_amounts = sorted(t.amount for t in emitted_txs)
    assert emitted_amounts == [0.5, 5.0]

    eof_type, eof_cid, eof_payload = proto.unpack_packet(eof_msg)
    assert eof_type == MessageType.EOF
    assert eof_cid == client_id
    eof_ctrl_msg = ctrl_ser.deserialize(eof_payload)
    assert eof_ctrl_msg.expected_total == 2

    worker.close()


# ---------------------------------------------------------------------------
# Invariante 4: el archivo se cierra después del emit
# ---------------------------------------------------------------------------
def test_emit_closes_disk_log(monkeypatch):
    module = _import_barrier_module(monkeypatch)
    worker = module.Q3BarrierWorker()
    proto = InternalProtocol()
    tx_ser = TransactionSerializer()
    ctrl_ser = ControlMessageSerializer()
    client_id = 1

    raw_batch = tx_ser.serialize_batch([_tx(amount=1.0, fmt="Wire")])
    worker._process_candidate_message(_pack_data(proto, client_id, raw_batch))
    state = worker.states_by_client[client_id]
    file_obj = state.disk_log._file

    # Forzamos las dos EOFs para llegar a _emit_client
    eof_ctrl = ctrl_ser.serialize(
        ControlMessage(sender_id=0, expected_total=1, processed_count=0)
    )
    worker._process_average_message(_pack_eof(proto, client_id, eof_ctrl))
    worker._process_candidate_message(_pack_eof(proto, client_id, eof_ctrl))

    worker._emit_client(client_id, FakeQueue())
    assert file_obj.closed, "el TemporaryFile debe estar cerrado tras emit"

    worker.close()


# ---------------------------------------------------------------------------
# Invariante 5: close() del worker limpia estados pendientes
# ---------------------------------------------------------------------------
def test_worker_close_cleans_up_pending_client_state(monkeypatch):
    """
    Si SIGTERM llega antes del emit, close() debe cerrar los TemporaryFiles
    de los clientes que quedaron a medio procesar (liberar fds).
    """
    module = _import_barrier_module(monkeypatch)
    worker = module.Q3BarrierWorker()
    proto = InternalProtocol()
    tx_ser = TransactionSerializer()

    raw_batch = tx_ser.serialize_batch([_tx(amount=1.0)])
    worker._process_candidate_message(_pack_data(proto, 100, raw_batch))
    worker._process_candidate_message(_pack_data(proto, 200, raw_batch))

    pending_files = [
        worker.states_by_client[cid].disk_log._file for cid in (100, 200)
    ]

    worker.close()

    for f in pending_files:
        assert f.closed, "close() del worker debe cerrar TemporaryFiles pendientes"
    assert worker.states_by_client == {}
