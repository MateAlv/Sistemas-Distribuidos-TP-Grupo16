import os
from collections.abc import Generator


class DirectoryReader:
    def __init__(self, root: str, extensions: tuple[str, ...] | None = None) -> None:
        self.root = os.path.abspath(root)
        self.extensions = tuple(ext.lower() for ext in extensions or ())

    def iter(self) -> Generator[tuple[str, str, int], None, None]:
        for dirpath, _, filenames in sorted(os.walk(self.root)):
            for filename in sorted(filenames):
                if filename.startswith("."):
                    continue

                abs_path = os.path.join(dirpath, filename)
                if not os.path.isfile(abs_path):
                    continue

                if self.extensions and not filename.lower().endswith(self.extensions):
                    continue

                rel_path = os.path.relpath(abs_path, self.root)
                yield abs_path, rel_path, os.path.getsize(abs_path)
