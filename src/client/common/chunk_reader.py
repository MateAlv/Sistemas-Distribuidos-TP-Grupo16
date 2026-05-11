from collections.abc import Generator

from common.directory_reader import DirectoryReader
from common.file_chunk import FileChunk


MESSAGE_TYPE_SIZE = 1


class ChunkReader:
    def __init__(
        self,
        client_id: int,
        root: str,
        max_message_size: int,
        extensions: tuple[str, ...] | None = None,
        message_type_size: int = MESSAGE_TYPE_SIZE,
    ) -> None:
        if max_message_size <= 0:
            raise ValueError("max_message_size must be greater than 0")
        if message_type_size <= 0:
            raise ValueError("message_type_size must be greater than 0")

        self.client_id = int(client_id)
        self.max_message_size = int(max_message_size)
        self.message_type_size = int(message_type_size)
        self.directory_reader = DirectoryReader(root, extensions)

    def iter(self) -> Generator[FileChunk, None, None]:
        for abs_path, rel_path, size in self.directory_reader.iter():
            max_payload_size = FileChunk.max_payload_size_for_message(
                rel_path=rel_path,
                max_message_size=self.max_message_size,
                message_type_size=self.message_type_size,
            )

            if size == 0:
                yield FileChunk(
                    rel_path=rel_path,
                    client_id=self.client_id,
                    offset=0,
                    data=b"",
                )
                continue

            with open(abs_path, "rb") as file:
                offset = 0
                while True:
                    payload = file.read(max_payload_size)
                    if not payload:
                        break
                    yield FileChunk(
                        rel_path=rel_path,
                        client_id=self.client_id,
                        offset=offset,
                        data=payload,
                    )
                    offset += len(payload)
