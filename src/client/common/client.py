import logging
import os
import signal
from dataclasses import dataclass

from common.chunk_reader import MESSAGE_TYPE_SIZE, ChunkReader, InputFile
from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
    file_type_name,
)
from common.sender import Sender


DEFAULT_CLIENT_ID = 1
DEFAULT_DATA_DIR = "/data"
DEFAULT_TRANSACTIONS_FILE = "transactions.csv"
DEFAULT_ACCOUNTS_FILE = "accounts.csv"
DEFAULT_SERVER_HOST = "gateway"
DEFAULT_SERVER_PORT = 5678
DEFAULT_CHUNK_MAX_BYTES = 64 * 1024
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_IO_TIMEOUT_SECONDS = 3600.0
DEFAULT_RESULT_LINE_MAX_BYTES = 1024 * 1024
DEFAULT_LOGGING_LEVEL = "INFO"
MIN_TCP_PORT = 1
MAX_TCP_PORT = 65535
MAX_CLIENT_ID = 2**32 - 1
CSV_EXTENSIONS = (".csv",)
MAX_LOGGED_RESULT_LINES = 5

@dataclass(frozen=True)
class ClientConfig:
    client_id: int
    data_dir: str
    transactions_file: str
    accounts_file: str
    server_host: str
    server_port: int
    chunk_max_bytes: int
    connect_timeout_seconds: float
    io_timeout_seconds: float
    result_line_max_bytes: int
    logging_level: str


class Client:
    def __init__(self, config: ClientConfig | None = None) -> None:
        self.config = config
        self.closed = False
        self.reader: ChunkReader | None = None
        self.sender: Sender | None = None

        if config is not None:
            self.configure(config)

    def configure(self, config: ClientConfig) -> None:
        self.config = config
        self.reader = ChunkReader(
            client_id=config.client_id,
            input_files=build_input_files(config),
            max_message_size=config.chunk_max_bytes,
            message_type_size=MESSAGE_TYPE_SIZE,
        )
        self.sender = Sender(
            host=config.server_host,
            port=config.server_port,
            connect_timeout=config.connect_timeout_seconds,
            io_timeout=config.io_timeout_seconds,
        )

    def start(self) -> int:
        try:
            if self.config is None:
                config = load_config()
                initialize_log(config.logging_level)
                validate_config(config)
                self.configure(config)
            else:
                initialize_log(self.config.logging_level)
                validate_config(self.config)
        except Exception as e:
            logging.basicConfig(level=logging.ERROR)
            logging.error("client_config | result=error | error=%s", e)
            return 2

        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.log_config()

        try:
            self.stream_files()
        except OSError as e:
            if self.closed:
                return 0
            logging.error("client_io | result=error | error=%s", e)
            return 1
        except Exception as e:
            if self.closed:
                return 0
            logging.error("client_run | result=error | error=%s", e)
            return 2

        self.close()
        return 0

    def stream_files(self) -> None:
        sent_chunks = 0
        sent_payload_bytes = 0
        config = self.require_config()
        reader = self.require_reader()
        sender = self.require_sender()

        logging.info(
            "client_connect | client_id=%s | data_dir=%s | server=%s:%s",
            config.client_id,
            config.data_dir,
            config.server_host,
            config.server_port,
        )

        sender.connect()
        try:
            sender.send_handshake_request(config.client_id)
            logging.info("client_handshake | client_id=%s | result=ok", config.client_id)

            for chunk in reader.iter():
                message_size = chunk.message_size(MESSAGE_TYPE_SIZE)
                if message_size > config.chunk_max_bytes:
                    raise RuntimeError(
                        "chunk message exceeded configured max size "
                        f"(message_size={message_size}, max={config.chunk_max_bytes}, "
                        f"path={chunk.path()!r}, offset={chunk.offset()})"
                    )

                logging.debug(
                    "client_send_chunk | client_id=%s | file_type=%s | path=%s | "
                    "offset=%s | payload_bytes=%s | message_bytes=%s",
                    config.client_id,
                    file_type_name(chunk.file_type()),
                    chunk.path(),
                    chunk.offset(),
                    chunk.payload_size(),
                    message_size,
                )
                sender.send_file_chunk(chunk.serialize())
                sent_chunks += 1
                sent_payload_bytes += chunk.payload_size()

            sender.send_finished()
            logging.info(
                "client_send_finished | client_id=%s | chunks=%s | payload_bytes=%s | result=ack",
                config.client_id,
                sent_chunks,
                sent_payload_bytes,
            )

            self.log_result_lines()
        except Exception:
            sender.close()
            raise

        sender.close()

    def close(self) -> None:
        self.closed = True
        if self.sender is not None:
            self.sender.close()

    def handle_sigterm(self, signum, frame) -> None:
        config = self.require_config()
        logging.info("client_shutdown | client_id=%s | signal=%s", config.client_id, signum)
        self.close()

    def log_config(self) -> None:
        config = self.require_config()
        logging.info(
            "client_config | result=ok | client_id=%s | data_dir=%s | "
            "transactions_file=%s | accounts_file=%s | server=%s:%s | "
            "chunk_max_bytes=%s",
            config.client_id,
            config.data_dir,
            config.transactions_file,
            config.accounts_file,
            config.server_host,
            config.server_port,
            config.chunk_max_bytes,
        )

    def log_result_lines(self) -> None:
        config = self.require_config()
        sender = self.require_sender()
        logging.info("client_results_wait | client_id=%s", config.client_id)

        output_dir = os.getenv("OUTPUT_DIR", os.path.join(config.data_dir, "output"))
        os.makedirs(output_dir, exist_ok=True)

        q1_file = os.path.join(output_dir, f"results_q1_{config.client_id}.csv")
        q2_file = os.path.join(output_dir, f"results_q2_{config.client_id}.csv")

        counts: dict[str, int] = {"q1": 0, "q2": 0}
        with open(q1_file, "w") as f1, open(q2_file, "w") as f2:
            f1.write("date,from_bank,from_account,to_bank,to_account,amount,currency,format\n")
            f2.write("bank_id,from_account,max_amount\n")

            for line in sender.iter_result_lines(config.result_line_max_bytes):
                if line.startswith("Q2|"):
                    data = line[3:]
                    counts["q2"] += 1
                    f2.write(data + "\n")
                    if counts["q2"] <= MAX_LOGGED_RESULT_LINES:
                        logging.info(
                            "client_result_line | query=q2 | client_id=%s | line_number=%s | data=%s",
                            config.client_id, counts["q2"], data,
                        )
                else:
                    counts["q1"] += 1
                    f1.write(line + "\n")
                    if counts["q1"] <= MAX_LOGGED_RESULT_LINES:
                        logging.info(
                            "client_result_line | query=q1 | client_id=%s | line_number=%s | data=%s",
                            config.client_id, counts["q1"], line,
                        )

        logging.info(
            "client_results_finished | client_id=%s | q1_lines=%s | q2_lines=%s | q1_file=%s | q2_file=%s",
            config.client_id, counts["q1"], counts["q2"], q1_file, q2_file,
        )

    def require_config(self) -> ClientConfig:
        if self.config is None:
            raise RuntimeError("client is not configured")
        return self.config

    def require_reader(self) -> ChunkReader:
        if self.reader is None:
            raise RuntimeError("client reader is not configured")
        return self.reader

    def require_sender(self) -> Sender:
        if self.sender is None:
            raise RuntimeError("client sender is not configured")
        return self.sender


def load_config() -> ClientConfig:
    return ClientConfig(
        client_id=get_int("CLIENT_ID", DEFAULT_CLIENT_ID),
        data_dir=os.getenv("DATA_DIR", DEFAULT_DATA_DIR),
        transactions_file=os.getenv("TRANSACTIONS_FILE", DEFAULT_TRANSACTIONS_FILE),
        accounts_file=os.getenv("ACCOUNTS_FILE", DEFAULT_ACCOUNTS_FILE),
        server_host=os.getenv("SERVER_HOST", DEFAULT_SERVER_HOST),
        server_port=get_int("SERVER_PORT", DEFAULT_SERVER_PORT),
        chunk_max_bytes=get_int("CHUNK_MAX_BYTES", DEFAULT_CHUNK_MAX_BYTES),
        connect_timeout_seconds=get_float(
            "CONNECT_TIMEOUT_SECONDS",
            DEFAULT_CONNECT_TIMEOUT_SECONDS,
        ),
        io_timeout_seconds=get_float("IO_TIMEOUT_SECONDS", DEFAULT_IO_TIMEOUT_SECONDS),
        result_line_max_bytes=get_int(
            "RESULT_LINE_MAX_BYTES",
            DEFAULT_RESULT_LINE_MAX_BYTES,
        ),
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
    if config.client_id > MAX_CLIENT_ID:
        raise ValueError(f"CLIENT_ID must be less than or equal to {MAX_CLIENT_ID}")
    if not os.path.isdir(config.data_dir):
        raise ValueError(f"DATA_DIR must be an existing directory: {config.data_dir}")
    for input_file in build_input_files(config):
        if not os.path.isfile(input_file.abs_path):
            raise ValueError(
                f"{file_type_name(input_file.file_type)} file must exist: "
                f"{input_file.abs_path}"
            )
        if not input_file.abs_path.lower().endswith(CSV_EXTENSIONS):
            raise ValueError(
                f"{file_type_name(input_file.file_type)} file must be a CSV: "
                f"{input_file.abs_path}"
            )
    if config.server_port < MIN_TCP_PORT or config.server_port > MAX_TCP_PORT:
        raise ValueError(f"SERVER_PORT must be in range [{MIN_TCP_PORT}, {MAX_TCP_PORT}]")
    if config.chunk_max_bytes <= 0:
        raise ValueError("CHUNK_MAX_BYTES must be greater than 0")
    if config.connect_timeout_seconds <= 0:
        raise ValueError("CONNECT_TIMEOUT_SECONDS must be greater than 0")
    if config.io_timeout_seconds <= 0:
        raise ValueError("IO_TIMEOUT_SECONDS must be greater than 0")
    if config.result_line_max_bytes <= 0:
        raise ValueError("RESULT_LINE_MAX_BYTES must be greater than 0")


def get_int(name: str, default: int) -> int:
    return get_env_value(name, default, int, "an integer")


def get_float(name: str, default: float) -> float:
    return get_env_value(name, default, float, "a number")


def get_env_value(name: str, default, parser, expected_type: str):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return parser(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be {expected_type}") from exc


def build_input_files(config: ClientConfig) -> tuple[InputFile, ...]:
    data_dir = os.path.abspath(config.data_dir)
    transactions_path = resolve_input_path(data_dir, config.transactions_file)
    accounts_path = resolve_input_path(data_dir, config.accounts_file)

    return (
        InputFile(
            abs_path=transactions_path,
            rel_path=relative_input_path(data_dir, transactions_path),
            file_type=FILE_TYPE_TRANSACTIONS,
        ),
        InputFile(
            abs_path=accounts_path,
            rel_path=relative_input_path(data_dir, accounts_path),
            file_type=FILE_TYPE_ACCOUNTS,
        ),
    )


def resolve_input_path(data_dir: str, configured_path: str) -> str:
    if os.path.isabs(configured_path):
        return os.path.abspath(configured_path)
    return os.path.abspath(os.path.join(data_dir, configured_path))


def relative_input_path(data_dir: str, abs_path: str) -> str:
    try:
        if os.path.commonpath([data_dir, abs_path]) == data_dir:
            return os.path.relpath(abs_path, data_dir)
    except ValueError:
        pass
    return os.path.basename(abs_path)
