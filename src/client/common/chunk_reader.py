from collections.abc import Generator
from dataclasses import dataclass
import os

from common.message_protocol.external import FileChunk


MESSAGE_TYPE_SIZE = 1


@dataclass(frozen=True)
class InputFile:
    abs_path: str
    rel_path: str
    file_type: int


class ChunkReader:
    def __init__(
        self,
        client_id: int,
        input_files: tuple[InputFile, ...],
        max_message_size: int,
        message_type_size: int = MESSAGE_TYPE_SIZE,
    ) -> None:
        if max_message_size <= 0:
            raise ValueError("max_message_size must be greater than 0")
        if message_type_size <= 0:
            raise ValueError("message_type_size must be greater than 0")
        if not input_files:
            raise ValueError("at least one input file is required")

        self.client_id = int(client_id)
        self.input_files = input_files
        self.max_message_size = int(max_message_size)
        self.message_type_size = int(message_type_size)

    def iter(self) -> Generator[FileChunk, None, None]:
        for input_file in self.input_files:
            size = os.path.getsize(input_file.abs_path)
            max_payload_size = FileChunk.max_payload_size_for_message(
                rel_path=input_file.rel_path,
                max_message_size=self.max_message_size,
                message_type_size=self.message_type_size,
            )

            if size == 0:
                yield FileChunk(
                    rel_path=input_file.rel_path,
                    client_id=self.client_id,
                    file_type=input_file.file_type,
                    offset=0,
                    data=b"",
                )
                continue

            with open(input_file.abs_path, "rb") as file:
                offset = 0
                while True:
                    payload = file.read(max_payload_size)
                    if not payload:
                        break
                    yield FileChunk(
                        rel_path=input_file.rel_path,
                        client_id=self.client_id,
                        file_type=input_file.file_type,
                        offset=offset,
                        data=payload,
                    )
                    offset += len(payload)
