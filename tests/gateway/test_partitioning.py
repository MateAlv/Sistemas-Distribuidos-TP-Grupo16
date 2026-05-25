import pytest

from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
)
from gateway.gateway import file_splitter_bindings, partition_for


def test_partition_for_splits_client_files_when_possible():
    assert partition_for(0, FILE_TYPE_TRANSACTIONS, 2) == 0
    assert partition_for(0, FILE_TYPE_ACCOUNTS, 2) == 1


def test_partition_for_keeps_same_file_type_stable_per_client():
    assert partition_for(3, FILE_TYPE_TRANSACTIONS, 5) == 1
    assert partition_for(3, FILE_TYPE_TRANSACTIONS, 5) == 1
    assert partition_for(3, FILE_TYPE_ACCOUNTS, 5) == 2


def test_partition_for_single_partition_accepts_both_file_types():
    assert partition_for(0, FILE_TYPE_TRANSACTIONS, 1) == 0
    assert partition_for(0, FILE_TYPE_ACCOUNTS, 1) == 0


def test_partition_for_rejects_invalid_inputs():
    with pytest.raises(ValueError, match="partitions must be greater than 0"):
        partition_for(0, FILE_TYPE_TRANSACTIONS, 0)

    with pytest.raises(ValueError, match="unknown file_type"):
        partition_for(0, 99, 2)


def test_file_splitter_bindings_match_partitions():
    assert file_splitter_bindings("file_splitter", 3) == {
        "file_splitter_0": "file_ingestor.0",
        "file_splitter_1": "file_ingestor.1",
        "file_splitter_2": "file_ingestor.2",
    }


def test_file_splitter_bindings_reject_invalid_partitions():
    with pytest.raises(ValueError, match="partitions must be greater than 0"):
        file_splitter_bindings("file_splitter", 0)
