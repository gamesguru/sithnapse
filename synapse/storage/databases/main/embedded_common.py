from __future__ import annotations

from enum import Enum, auto


class SyncTier(Enum):
    """Classification for embedded-sidecar write durability.

    DURABLE: No SQL fallback exists, or the write uses accumulating/delta
    semantics (counters, auth-chain links, HAMT roots).  A lost unflushed
    write here means silent data loss or incorrect state, so sync() is
    called after every batch.

    CACHE: A SQL fallback exists on the read path.  A lost unflushed write
    just means a slower read via that fallback, not data loss.  sync() is
    skipped to avoid per-event fsync cost on the hottest write path.
    """

    DURABLE = auto()
    CACHE = auto()


def maybe_sync(tier: SyncTier) -> None:
    """No-op: kept for call-site compatibility and to preserve the DURABLE
    vs. CACHE classification in each call site's docstring/comments (still
    meaningful documentation of which writes have no SQL fallback), but an
    actual fsync is a whole-device write-cache flush, not scoped to the
    bytes just written -- too expensive to pay per write/per batch (~59ms
    measured per call). A single periodic background task
    (`StateGroupDataStore._periodic_embedded_sync`, every ~1s) flushes the
    one shared shard file for every embedded write from every store
    instead, bounding the durability window to about a second rather than
    paying an fsync on the hot path.
    """
