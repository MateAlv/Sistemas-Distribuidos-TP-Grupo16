import json
import os
from pathlib import Path


MAX_EPOCH = 2**16 - 1


class EpochStore:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def load(self) -> int:
        try:
            with self._path.open("r", encoding="utf-8") as state_file:
                state = json.load(state_file)
        except FileNotFoundError:
            return 0
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"could not load monitor epoch from {self._path}: {exc}"
            ) from exc

        epoch = state.get("epoch") if isinstance(state, dict) else None
        if not isinstance(epoch, int) or isinstance(epoch, bool):
            raise ValueError(
                f"invalid monitor epoch state in {self._path}: epoch must be an integer"
            )
        if not 0 <= epoch <= MAX_EPOCH:
            raise ValueError(
                f"invalid monitor epoch state in {self._path}: "
                f"epoch must be in range [0, {MAX_EPOCH}]"
            )
        return epoch

    def save(self, epoch: int) -> None:
        if not isinstance(epoch, int) or isinstance(epoch, bool):
            raise ValueError("epoch must be an integer")
        if not 0 <= epoch <= MAX_EPOCH:
            raise ValueError(f"epoch must be in range [0, {MAX_EPOCH}]")

        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self._path.with_name(
            f".{self._path.name}.tmp-{os.getpid()}"
        )

        try:
            with temporary_path.open("w", encoding="utf-8") as state_file:
                json.dump({"epoch": epoch}, state_file, separators=(",", ":"))
                state_file.write("\n")
                state_file.flush()
                os.fsync(state_file.fileno())

            os.replace(temporary_path, self._path)
            directory_fd = os.open(self._path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
