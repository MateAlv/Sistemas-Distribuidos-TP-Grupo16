import logging
import os
import pickle


class LastStateManager:
    """Atomic snapshot rotation: write tmp → rename current→previous → rename tmp→current."""

    _TMP = "last_state.tmp"
    _CURRENT = "last_state.current"
    _PREVIOUS = "last_state.previous"

    def __init__(self, state_dir: str) -> None:
        os.makedirs(state_dir, exist_ok=True)
        self._dir = state_dir

    def save(self, data: dict) -> None:
        """Persist ``data`` atomically. ``data`` must already be a consistent copy
        taken under the caller's lock (this method does I/O outside any lock)."""
        tmp = self._p(self._TMP)
        current = self._p(self._CURRENT)
        previous = self._p(self._PREVIOUS)

        with open(tmp, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            f.flush()
            os.fsync(f.fileno())

        if os.path.exists(current):
            os.rename(current, previous)

        os.rename(tmp, current)

        dfd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)

    def load(self) -> dict | None:
        """Load the latest valid snapshot, falling back to previous if current is absent."""
        for name in (self._CURRENT, self._PREVIOUS):
            path = self._p(name)
            if not os.path.exists(path):
                continue
            try:
                with open(path, "rb") as f:
                    snap = pickle.load(f)
                logging.info("last_state_loaded | file=%s", path)
                return snap
            except Exception:
                logging.warning("last_state_load_error | file=%s", path, exc_info=True)
        return None

    def _p(self, name: str) -> str:
        return os.path.join(self._dir, name)
