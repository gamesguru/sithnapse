from collections.abc import Sequence

def room_structural_key(room_id: str) -> bytes: ...
def room_hamt_prefix(
    room_id: str,
    msc4291_room_ids_as_hashes: bool,
) -> bytes: ...
def build_root_handle(
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[tuple[bytes, bytes], list[tuple[bytes, bytes]]]: ...
def build_root_handle_with_lattice(
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[bytes, bytes, bytes, list[tuple[bytes, bytes]]]:
    """Returns (structural_hash, state_group_id, lattice_bytes, nodes).

    Same as `build_root_handle`, but also returns the full, retained
    2048-byte `LtHash` lattice (not just its collapsed `state_group_id`
    digest). A caller must keep `lattice_bytes` alongside the root if it
    wants to apply further incremental updates via
    `apply_flat_state_updates` later — the digest alone cannot be
    "un-collapsed" back into an updatable lattice.
    """

def apply_flat_state_updates(
    room_id: str,
    root_node_bytes: bytes,
    nodes: Sequence[tuple[bytes, bytes]],
    lattice_bytes: bytes,
    updates: Sequence[tuple[str, str, str | None]],
) -> tuple[tuple[bytes, bytes, bytes, list[tuple[bytes, bytes]]] | None, list[bytes]]:
    """Applies single-key changes to an existing flat HAMT root via
    O(log S) path-copying, without materializing or rebuilding the whole
    state map.

    Returns `(applied, missing)`. On success, `applied` is
    `(structural_hash, state_group_id, lattice_bytes, new_nodes)` for the
    resulting root and `missing` is empty; `new_nodes` contains *only* the
    newly created nodes (i.e. excluding anything already present in `nodes`)
    — this is the O(changed-path) node set, not the whole reachable tree.

    If a resolver lookup misses a hash not present in `nodes`, `applied` is
    `None` and `missing` names the hash(es) to fetch — mirroring
    `lookup_state_entries`'s retry contract: fetch `missing`, add it to
    `nodes`, and call again; each retry surfaces one more tree level's worth
    of missing hashes, same as the existing
    `_lookup_state_hamt_from_postgres_txn` pattern. Nothing is partially
    applied either way, since nothing is written to the working root until
    the whole batch of `updates` resolves successfully.

    `nodes` must include the nodes along the path(s) to every key being
    changed, plus the root itself; the caller is expected to fetch only
    what's needed for `updates`, not the whole tree. `lattice_bytes` is the
    *retained* lattice for the current root (from `build_root_handle_with_lattice`
    or a prior call to this function), not the collapsed `state_group_id`.

    `updates` is `(event_type, state_key, new_event_id)`, where
    `new_event_id=None` means "remove this key".

    Raises only on a genuine failure (bad encoding, corrupt bytes, or a HAMT
    hash collision) — a missing node is reported via the return value, not
    an exception.
    """

def build_typed_root(
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[bytes, bytes, bytes, list[tuple[bytes, bytes]]]:
    """Returns (structural_hash, state_group_id, root_bytes, nodes).

    `state_group_id` is the unkeyed, cross-server-comparable LtHash-derived
    identity (matches `build_root_handle`'s second tuple element for the same
    logical state); `structural_hash` is the typed directory's local, keyed
    structural identity and must not be used as a state-group identifier.
    """

def decode_typed_root(
    root_bytes: bytes,
) -> tuple[bytes, bytes, list[tuple[str, bytes]]]:
    """Returns (structural_hash, state_group_id, directory)."""

def build_typed_root_with_lattice(
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[bytes, bytes, bytes, bytes, list[tuple[bytes, bytes]]]:
    """Returns (structural_hash, state_group_id, lattice_bytes, root_bytes, nodes).

    Same as `build_typed_root`, but also returns the full, retained
    2048-byte `LtHash` lattice (not just its collapsed `state_group_id`
    digest). A caller must keep `lattice_bytes` alongside the typed root if
    it wants to apply further incremental updates via
    `apply_typed_state_updates` later.
    """

def apply_typed_state_updates(
    room_id: str,
    typed_root_bytes: bytes,
    nodes: Sequence[tuple[bytes, bytes]],
    lattice_bytes: bytes,
    updates: Sequence[tuple[str, str, str | None]],
) -> tuple[tuple[bytes, bytes, bytes, list[tuple[bytes, bytes]]] | None, list[bytes]]:
    """Applies single-key changes to an existing typed root by updating only
    the touched event types' subtrees via O(log S_T) path-copying and
    touching only their directory entries -- the typed-root analogue of
    `apply_flat_state_updates`. Doing anything less (e.g. rebuilding the
    whole typed structure from a materialized entry list per update) would
    reintroduce the O(S)-per-update tax on the typed side that
    `apply_flat_state_updates` already eliminates on the flat side.

    Returns `(applied, missing)`, with the same retry contract as
    `apply_flat_state_updates`: on success, `applied` is
    `(typed_root_bytes, state_group_id, lattice_bytes, new_nodes)` and
    `missing` is empty; on a resolver miss, `applied` is `None` and
    `missing` names the hash(es) to fetch and retry with. `new_nodes`
    contains only the newly created subtree nodes.

    `nodes` must include, for every event type touched by `updates`, the
    nodes along the path(s) to the changed keys within that type's subtree
    (the subtree root at minimum). A brand-new event type (no existing
    subtree) needs no prior nodes for that type.

    `updates` is `(event_type, state_key, new_event_id)`, where
    `new_event_id=None` means "remove this key". `lattice_bytes` is the
    *retained* lattice for the current typed root (from
    `build_typed_root_with_lattice` or a prior call to this function), not
    the collapsed `state_group_id`.
    """

def materialize_state_entries(
    root_node_bytes: bytes,
    nodes: Sequence[tuple[bytes, bytes]],
) -> list[tuple[str, str, str]]: ...
def lookup_state_entries(
    room_id: str,
    root_node_bytes: bytes,
    nodes: Sequence[tuple[bytes, bytes]],
    keys: Sequence[tuple[str, str]],
) -> tuple[list[tuple[str, str, str]], list[bytes]]:
    """Look up selected state entries in a persisted flat HAMT.

    Returns ``(entries, missing)``. When ``missing`` is non-empty, ``entries``
    is incomplete: an unresolved node path omits its entries rather than
    raising. Fetch every returned hash, add the nodes to ``nodes``, and retry
    until ``missing`` is empty before treating ``entries`` as complete.
    """

def node_child_hashes(node_bytes: bytes) -> list[bytes]: ...
def reachability_audit(
    roots: Sequence[bytes],
    universe: Sequence[bytes],
    nodes: Sequence[tuple[bytes, bytes]],
) -> tuple[list[bytes], list[bytes]]: ...
def unreachable_node_hashes(
    roots: Sequence[bytes],
    universe: Sequence[bytes],
    nodes: Sequence[tuple[bytes, bytes]],
) -> list[bytes]: ...
