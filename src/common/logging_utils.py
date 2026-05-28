import logging
import os
import threading
from dataclasses import dataclass

from common.message_protocol.external import FileChunkHeader, FileEof
from common.message_protocol.external.types import (
    MSG_CHUNK,
    MSG_EOF,
    file_type_name,
)
from common.message_protocol.internal import InternalProtocol
from common.message_protocol.internal.common import MessageType


DEFAULT_FLOW_LOG_EVERY_MESSAGES = 100
DEFAULT_FLOW_LOG_EVERY_BYTES = 8 * 1024 * 1024
DEFAULT_FLOW_LOG_FIRST_MESSAGES = 1
DEFAULT_WORKER_LOG_EVERY_MESSAGES = 100
FILE_INGESTOR_EXCHANGE = os.getenv("FILE_INGESTOR_EXCHANGE", "file_ingestor_exchange")
FILE_SPLITTER_QUEUE_PREFIX = os.getenv("FILE_SPLITTER_QUEUE_PREFIX", "file_splitter")


@dataclass(frozen=True)
class MessageSummary:
    family: str
    msg_type: str
    client_id: int | None = None
    payload_bytes: int | None = None
    file_type: str | None = None
    path: str | None = None
    offset: int | None = None
    always_log: bool = False


class MessageFlowLogger:
    def __init__(self) -> None:
        self._enabled = _env_bool("FLOW_LOG_ENABLED", True)
        self._every_messages = _env_int(
            "FLOW_LOG_EVERY_MESSAGES", DEFAULT_FLOW_LOG_EVERY_MESSAGES
        )
        self._every_bytes = _env_int(
            "FLOW_LOG_EVERY_BYTES", DEFAULT_FLOW_LOG_EVERY_BYTES
        )
        self._first_messages = _env_int(
            "FLOW_LOG_FIRST_MESSAGES", DEFAULT_FLOW_LOG_FIRST_MESSAGES
        )
        self._counters = {
            "publish": {"messages": 0, "bytes": 0, "last_log_bytes": 0},
            "consume": {"messages": 0, "bytes": 0, "last_log_bytes": 0},
            "rpc": {"messages": 0, "bytes": 0, "last_log_bytes": 0},
        }
        self._lock = threading.Lock()

    def observe(
        self,
        direction: str,
        endpoint_type: str,
        endpoint: str,
        message: bytes,
        routing_key: str | None = None,
    ) -> None:
        if not self._enabled:
            return

        message_size = len(message)
        summary = summarize_middleware_message(endpoint, message)

        with self._lock:
            counter = self._counters.setdefault(
                direction, {"messages": 0, "bytes": 0, "last_log_bytes": 0}
            )
            counter["messages"] += 1
            counter["bytes"] += message_size
            messages = counter["messages"]
            total_bytes = counter["bytes"]
            should_log = self._should_log(counter, summary)
            if should_log:
                counter["last_log_bytes"] = total_bytes

        if not should_log:
            return

        logging.info(
            "middleware_flow | direction=%s | endpoint_type=%s | endpoint=%s | "
            "routing_key=%s | messages=%s | bytes=%s | last_bytes=%s | "
            "family=%s | msg_type=%s | client_id=%s | payload_bytes=%s | "
            "file_type=%s | path=%s | offset=%s",
            direction,
            endpoint_type,
            endpoint,
            routing_key or "",
            messages,
            total_bytes,
            message_size,
            summary.family,
            summary.msg_type,
            _none_as_dash(summary.client_id),
            _none_as_dash(summary.payload_bytes),
            summary.file_type or "",
            summary.path or "",
            _none_as_dash(summary.offset),
        )

    def _should_log(self, counter: dict[str, int], summary: MessageSummary) -> bool:
        if summary.always_log:
            return True
        messages = counter["messages"]
        if self._first_messages > 0 and messages <= self._first_messages:
            return True
        if self._every_messages > 0 and messages % self._every_messages == 0:
            return True
        if (
            self._every_bytes > 0
            and counter["bytes"] - counter["last_log_bytes"] >= self._every_bytes
        ):
            return True
        return False


_FLOW_LOGGER_SINGLETON: MessageFlowLogger | None = None


def get_flow_logger() -> MessageFlowLogger:
    global _FLOW_LOGGER_SINGLETON
    if _FLOW_LOGGER_SINGLETON is None:
        _FLOW_LOGGER_SINGLETON = MessageFlowLogger()
    return _FLOW_LOGGER_SINGLETON


def summarize_middleware_message(endpoint: str, message: bytes) -> MessageSummary:
    if _is_file_ingestor_endpoint(endpoint):
        summary = _summarize_external_file_message(message)
        if summary is not None:
            return summary

    summary = _summarize_internal_message(message)
    if summary is not None:
        return summary

    return MessageSummary(
        family="raw",
        msg_type="unknown",
        payload_bytes=len(message),
    )


def _summarize_external_file_message(message: bytes) -> MessageSummary | None:
    if not message:
        return None

    msg_type = message[0]
    payload = message[1:]
    if msg_type == MSG_CHUNK:
        try:
            header = FileChunkHeader.deserialize(payload)
        except Exception:
            return None
        return MessageSummary(
            family="external_file",
            msg_type="CHUNK",
            client_id=header.client_id,
            payload_bytes=header.payload_size,
            file_type=file_type_name(header.file_type),
            path=header.rel_path,
            offset=header.offset,
        )

    if msg_type == MSG_EOF:
        if len(payload) == 4:
            return MessageSummary(
                family="external_file",
                msg_type="EOF",
                client_id=int.from_bytes(payload, byteorder="big"),
                always_log=True,
            )
        try:
            eof = FileEof.deserialize(payload)
        except Exception:
            return None
        return MessageSummary(
            family="external_file",
            msg_type="EOF",
            client_id=eof.client_id(),
            file_type=file_type_name(eof.file_type()),
            path=eof.path(),
            always_log=True,
        )

    return None


def _summarize_internal_message(message: bytes) -> MessageSummary | None:
    if len(message) < InternalProtocol.HEADER_SIZE:
        return None
    try:
        msg_type = MessageType(message[0])
        _, client_id, payload = InternalProtocol.unpack_packet(message)
    except Exception:
        return None

    return MessageSummary(
        family="internal",
        msg_type=msg_type.name,
        client_id=client_id,
        payload_bytes=len(payload),
        always_log=msg_type == MessageType.EOF,
    )


def _is_file_ingestor_endpoint(endpoint: str) -> bool:
    return endpoint == FILE_INGESTOR_EXCHANGE or endpoint.startswith(
        f"{FILE_SPLITTER_QUEUE_PREFIX}_"
    )


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def should_log_progress(count: int, every: int | None = None) -> bool:
    if every is None:
        every = _env_int(
            "WORKER_LOG_EVERY_MESSAGES",
            _env_int("FLOW_LOG_EVERY_MESSAGES", DEFAULT_WORKER_LOG_EVERY_MESSAGES),
        )
    return count == 1 or (every > 0 and count % every == 0)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _none_as_dash(value):
    return "-" if value is None else value
