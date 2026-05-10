import os
import logging
import signal
from dataclasses import dataclass

from common.chunk_reader import MESSAGE_TYPE_SIZE, ChunkReader
from common.sender import Sender

DEFAULT_CLIENT_ID = 1
DEFAULT_DATA_DIR = "/data"
DEFAULT_SERVER_HOST = "gateway"
DEFAULT_SERVER_PORT = 5678
DEFAULT_CHUNK_MAX_BYTES = 64 * 1024
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_IO_TIMEOUT_SECONDS = 60000.0
DEFAULT_LOGGING_LEVEL = "INFO"
MIN_TCP_PORT = 1
MAX_TCP_PORT = 65535
CSV_EXTENSIONS = (".csv",)


@dataclass(frozen=True)
class ClientConfig:
    client_id: int
    data_dir: str
    server_host: str
    server_port: int
    chunk_max_bytes: int
    connect_timeout_seconds: float
    io_timeout_seconds: float
    logging_level: str


class Client:
    def __init__(self, config: ClientConfig) -> None:
        self.config = config
        self.closed = False
        self.reader = ChunkReader(
            client_id=config.client_id,
            root=config.data_dir,
            max_message_size=config.chunk_max_bytes,
            extensions=CSV_EXTENSIONS,
            message_type_size=MESSAGE_TYPE_SIZE,
        )
        self.sender = Sender(
            host=config.server_host,
            port=config.server_port,
            connect_timeout=config.connect_timeout_seconds,
            io_timeout=config.io_timeout_seconds,
        )

    def start(self) -> None:
        logging.info(
            "client_start | client_id=%s | data_dir=%s | server=%s:%s",
            self.config.client_id,
            self.config.data_dir,
            self.config.server_host,
            self.config.server_port,
        )

    def close(self) -> None:
        self.closed = True
        self.sender.close()

    def handle_sigterm(self, signum, frame) -> None:
        logging.info("client_shutdown | client_id=%s | signal=%s", self.config.client_id, signum)
        self.close()


def main() -> int:
    try:
        config = load_config()
        initialize_log(config.logging_level)
        validate_config(config)
    except Exception as e:
        logging.basicConfig(level=logging.ERROR)
        logging.error("client_config | result=error | error=%s", e)
        return 2

    client = Client(config)
    signal.signal(signal.SIGTERM, client.handle_sigterm)

    logging.info(
        "client_config | result=ok | client_id=%s | data_dir=%s | "
        "server=%s:%s | chunk_max_bytes=%s | extensions=%s",
        config.client_id,
        config.data_dir,
        config.server_host,
        config.server_port,
        config.chunk_max_bytes,
        ",".join(CSV_EXTENSIONS),
    )

    client.start()

    return 0


def load_config() -> ClientConfig:
    return ClientConfig(
        client_id=get_int("CLIENT_ID", DEFAULT_CLIENT_ID),
        data_dir=os.getenv("DATA_DIR", DEFAULT_DATA_DIR),
        server_host=os.getenv("SERVER_HOST", DEFAULT_SERVER_HOST),
        server_port=get_int("SERVER_PORT", DEFAULT_SERVER_PORT),
        chunk_max_bytes=get_int("CHUNK_MAX_BYTES", DEFAULT_CHUNK_MAX_BYTES),
        connect_timeout_seconds=get_float(
            "CONNECT_TIMEOUT_SECONDS",
            DEFAULT_CONNECT_TIMEOUT_SECONDS,
        ),
        io_timeout_seconds=get_float("IO_TIMEOUT_SECONDS", DEFAULT_IO_TIMEOUT_SECONDS),
        logging_level=os.getenv("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL),
    )


def initialize_log(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def validate_config(config: ClientConfig) -> None:
    if config.client_id < 0:
        raise ValueError("CLIENT_ID must be greater than or equal to 0")
    if config.server_port < MIN_TCP_PORT or config.server_port > MAX_TCP_PORT:
        raise ValueError(f"SERVER_PORT must be in range [{MIN_TCP_PORT}, {MAX_TCP_PORT}]")
    if config.chunk_max_bytes <= 0:
        raise ValueError("CHUNK_MAX_BYTES must be greater than 0")
    if config.connect_timeout_seconds <= 0:
        raise ValueError("CONNECT_TIMEOUT_SECONDS must be greater than 0")
    if config.io_timeout_seconds <= 0:
        raise ValueError("IO_TIMEOUT_SECONDS must be greater than 0")


def get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc


if __name__ == "__main__":
    raise SystemExit(main())
