import pytest

from common.routing import (
    queue_name_for_worker,
    routing_key_for_key,
    routing_key_for_shard,
    shard_for_client_id,
    shard_for_key,
    shard_for_key_parts,
    stable_digest_shard,
)


def test_integer_keys_keep_modulo_sharding():
    assert shard_for_key(17, 5) == 2
    assert shard_for_client_id(17, 5) == 2


def test_text_and_bytes_keys_are_stable():
    assert shard_for_key("client-17", 7) == shard_for_key("client-17", 7)
    assert stable_digest_shard(b"client-17", 7) == stable_digest_shard(b"client-17", 7)


def test_compound_keys_are_length_prefixed():
    partitions = 257

    assert shard_for_key_parts(("ab", "c"), partitions) != shard_for_key_parts(
        ("a", "bc"),
        partitions,
    )


def test_routing_key_for_shard_uses_underscore_by_default():
    assert routing_key_for_shard("sum_q2", 3) == "sum_q2_3"


def test_routing_key_for_key_uses_selected_shard():
    assert routing_key_for_key("sum_q2", 17, 5) == "sum_q2_2"


def test_queue_name_for_worker_is_stable():
    assert queue_name_for_worker("q4_joiner", 4) == "q4_joiner_4"


@pytest.mark.parametrize("shard_count", [0, -1])
def test_invalid_shard_count_fails_immediately(shard_count):
    with pytest.raises(ValueError, match="shard_count must be greater than 0"):
        shard_for_key(1, shard_count)


def test_invalid_worker_index_fails_immediately():
    with pytest.raises(ValueError, match="index must be greater than or equal to 0"):
        queue_name_for_worker("sum", -1)


def test_invalid_shard_index_fails_immediately():
    with pytest.raises(ValueError, match="shard must be greater than or equal to 0"):
        routing_key_for_shard("sum", -1)
