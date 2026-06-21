"""Deterministic shard and routing-key helpers.

This module centralizes the rules used to choose a downstream worker queue.
Integer keys keep the current modulo behavior; text/bytes keys use a stable
digest so assignments do not depend on Python process hash randomization.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable


def shard_for_key(key: int | str | bytes, shard_count: int) -> int:
    """Return the deterministic shard index for a routing key."""
    _validate_shard_count(shard_count)
    if isinstance(key, int):
        return key % shard_count
    return stable_digest_shard(key, shard_count)


def shard_for_client_id(client_id: int, shard_count: int) -> int:
    """Return the downstream shard for a client id."""
    return shard_for_key(int(client_id), shard_count)


def stable_digest_shard(key: str | bytes, shard_count: int) -> int:
    """Return a stable digest-based shard for non-integer keys."""
    _validate_shard_count(shard_count)
    digest = hashlib.sha256(_key_bytes(key)).digest()
    return int.from_bytes(digest, "big") % shard_count


def shard_for_key_parts(parts: Iterable[object], shard_count: int) -> int:
    """Return a stable shard for a compound key.

    Parts are length-prefixed before hashing so different tuples cannot collide
    by simple concatenation, for example ("ab", "c") and ("a", "bc").
    """
    compound_key = "".join(f"{len(str(part))}:{part}" for part in parts)
    return stable_digest_shard(compound_key, shard_count)


def routing_key_for_shard(prefix: str, shard: int, separator: str = "_") -> str:
    """Build the RabbitMQ routing key for an already selected shard."""
    if int(shard) < 0:
        raise ValueError("shard must be greater than or equal to 0")
    return f"{prefix}{separator}{int(shard)}"


def routing_key_for_key(
    prefix: str,
    key: int | str | bytes,
    shard_count: int,
    separator: str = "_",
) -> str:
    """Build the RabbitMQ routing key for a routing key and shard count."""
    return routing_key_for_shard(
        prefix,
        shard_for_key(key, shard_count),
        separator=separator,
    )


def queue_name_for_worker(prefix: str, index: int, separator: str = "_") -> str:
    """Build the stable personal queue name for a worker instance."""
    if int(index) < 0:
        raise ValueError("index must be greater than or equal to 0")
    return f"{prefix}{separator}{int(index)}"


def _validate_shard_count(shard_count: int) -> None:
    if int(shard_count) <= 0:
        raise ValueError("shard_count must be greater than 0")


def _key_bytes(key: str | bytes) -> bytes:
    if isinstance(key, bytes):
        return key
    return str(key).encode("utf-8")
