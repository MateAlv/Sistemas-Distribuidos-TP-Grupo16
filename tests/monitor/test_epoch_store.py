import json

import pytest

from monitor.election.epoch_store import EpochStore, MAX_EPOCH


def test_missing_epoch_state_starts_at_zero(tmp_path) -> None:
    store = EpochStore(tmp_path / "monitor" / "epoch.json")

    assert store.load() == 0


def test_epoch_survives_new_store_instance(tmp_path) -> None:
    path = tmp_path / "monitor" / "epoch.json"

    EpochStore(path).save(42)

    assert EpochStore(path).load() == 42
    assert json.loads(path.read_text(encoding="utf-8")) == {"epoch": 42}


def test_incomplete_temporary_file_is_ignored(tmp_path) -> None:
    path = tmp_path / "epoch.json"
    EpochStore(path).save(7)
    path.with_name(f".{path.name}.tmp-999").write_text(
        '{"epoch":',
        encoding="utf-8",
    )

    assert EpochStore(path).load() == 7


@pytest.mark.parametrize(
    "content",
    [
        "",
        "not-json",
        "[]",
        '{"epoch": true}',
        '{"epoch": -1}',
        f'{{"epoch": {MAX_EPOCH + 1}}}',
    ],
)
def test_corrupt_epoch_state_is_rejected(tmp_path, content) -> None:
    path = tmp_path / "epoch.json"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="monitor epoch"):
        EpochStore(path).load()


@pytest.mark.parametrize("epoch", [-1, MAX_EPOCH + 1, True])
def test_save_rejects_invalid_epoch(tmp_path, epoch) -> None:
    with pytest.raises(ValueError, match="epoch"):
        EpochStore(tmp_path / "epoch.json").save(epoch)
