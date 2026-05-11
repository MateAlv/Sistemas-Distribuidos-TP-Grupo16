import logging
from dataclasses import dataclass


@dataclass(frozen=True)
class FileIngestorConfig:
    id: int
    mom_host: str
    file_ingestor_exchange: str
    logging_level: str


class FileIngestor:
    def __init__(self, config: FileIngestorConfig) -> None:
        self._config = config
        self._stopped = False

    def start(self) -> None:
        logging.info(
            "file_ingestor_start | id=%s | mom_host=%s | exchange=%s",
            self._config.id,
            self._config.mom_host,
            self._config.file_ingestor_exchange,
        )

    def stop(self) -> None:
        self._stopped = True
        logging.info("file_ingestor_stop | id=%s", self._config.id)
