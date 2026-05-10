from collections.abc import Generator

from common.directory_reader import DirectoryReader
from common.file_chunk import FileChunk


class ChunkReader:
    def __init__(
        self,
        client_id: int,
        root: str,
        max_chunk_size: int,
        extensions: tuple[str, ...] | None = None,
    ) -> None:
        if max_chunk_size <= 0:
            raise ValueError("max_chunk_size must be greater than 0")

        self.client_id = int(client_id)
        self.max_chunk_size = int(max_chunk_size)
        self.directory_reader = DirectoryReader(root, extensions)

    def iter(self) -> Generator[FileChunk, None, None]:
        for abs_path, rel_path, size in self.directory_reader.iter():
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
                    payload = file.read(self.max_chunk_size)
                    if not payload:
                        break
                    yield FileChunk(
                        rel_path=rel_path,
                        client_id=self.client_id,
                        offset=offset,
                        data=payload,
                    )
                    offset += len(payload)
