use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use mtxdb::storage::{DigestAlgorithm, StorageError};
use mtxdb::SharedDatabase;
use mtxdb::{
    derive_collection_id, derive_group_full_id, derive_member_collection_id_from_group,
    CollectionMetadata, DatabaseLayout, FrameIdPolicy, NodeData, NodeId, PackfileStorage,
    PayloadPolicy, RecordIdentityRule, ShardType, StorageEngine, MEMBER_NAMESPACE_INTL,
};
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use sha2::{Digest, Sha256};

use crate::database::hamt_store::{NodeStore, ROOM_PREFIX_LEN};

struct MtxdbPools {
    state: Arc<PackfileStorage>,
    event_dag: Arc<PackfileStorage>,
    auth_chain: Arc<PackfileStorage>,
    _shared_database: Option<SharedDatabase>,
}

/// Base directory for the state_group -> room_prefix room-index file (see
/// `room_index` module below), set once by `open_client`.
static ROOM_INDEX_DIR: OnceCell<std::path::PathBuf> = OnceCell::new();

static DBS: OnceCell<MtxdbPools> = OnceCell::new();
/// Whether this process opened mtxdb writable (`open_client`) or
/// read-only (`open_client_read_only`). Checked by every mutating
/// pyfunction via `assert_writable()` before touching any fd -- a
/// read-only-opened handle fails a write at the OS level, but not until
/// flush time, by which point the failed write is already buffered and a
/// subsequent failed rollback can permanently poison the shard for every
/// process sharing it. Gating once here, at the single choke point every
/// mutating call passes through, means no future write door can reopen
/// that hole the way a per-Python-callsite guard could.
static WRITE_MODE: OnceCell<bool> = OnceCell::new();
/// The pid that actually opened `DBS`, recorded alongside it. `DBS` is a
/// process-global `OnceCell`; under a fork()-based worker launcher a
/// child that inherits an already-`Some` `DBS` from its parent's address
/// space must not silently reuse those fds as if it had opened them
/// itself -- see `check_pid_guard`.
static OPENER_PID: OnceCell<u32> = OnceCell::new();

/// Raises if this process opened mtxdb read-only (or never opened it),
/// before any mutating pyfunction touches a fd. See `WRITE_MODE`'s doc
/// comment for why this must be the single choke point, not a per-caller
/// guard.
pub(crate) fn assert_writable() -> PyResult<()> {
    match WRITE_MODE.get() {
        Some(true) => Ok(()),
        Some(false) => Err(pyo3::exceptions::PyRuntimeError::new_err(
            "mtxdb opened read-only in this process; refusing to write \
             (this is a routing bug -- writes must only be attempted on \
             the process that opened mtxdb via open_client)",
        )),
        None => Err(pyo3::exceptions::PyRuntimeError::new_err(
            "mtxdb not opened in this process",
        )),
    }
}

/// Returns `Ok(true)` if `DBS` is already open *in this process* and the
/// caller (`open_client`/`open_client_read_only`) should short-circuit
/// as a no-op, exactly as the old `DBS.get().is_some()` check did.
///
/// Returns `Err` instead of `Ok(true)` when `DBS` is `Some` but was
/// opened by a *different* pid -- i.e. this process inherited it via
/// `fork()` rather than opening it itself. Silently reusing inherited
/// fds here is exactly the fork-inheritance hazard: two processes
/// sharing one fd with independently-diverging in-memory bookkeeping
/// (buffered writes, file_len tracking) is how a flush lands on invalid
/// file state. There is no live incident this closes (the launcher in
/// use opens mtxdb strictly post-fork, per the topology audit), but a
/// future pre-fork opener would otherwise reintroduce it silently.
fn check_pid_guard() -> PyResult<bool> {
    if DBS.get().is_none() {
        return Ok(false);
    }
    let current = std::process::id();
    match OPENER_PID.get() {
        Some(&opener) if opener == current => Ok(true),
        opener => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "mtxdb DBS state was inherited from another process (opener \
             pid {opener:?}, current pid {current}); refusing to reuse \
             fds across a fork boundary"
        ))),
    }
}
/// Serialize all read-modify-write cycles through the embedded engine.
/// The mtxdb `StorageEngine` trait has no atomic increment or transaction API,
/// so we hold this across get→put_many for counters and auth-chain manifests.
/// Only one SQL transaction runs at a time via the DB pool, so contention is
/// negligible.
pub(crate) static RMW_LOCK: Mutex<()> = Mutex::new(());

/// Semantic counters for state-group refcount RMWs. These deliberately live
/// beside the Synapse wrapper rather than in generic mtxdb runtime stats:
/// only this layer knows that a missing counter means initialization.
static REFCOUNT_INITS: AtomicU64 = AtomicU64::new(0);
static REFCOUNT_EXISTING: AtomicU64 = AtomicU64::new(0);

fn pools() -> PyResult<&'static MtxdbPools> {
    DBS.get()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mtxdb not opened"))
}

fn state_db() -> PyResult<&'static Arc<PackfileStorage>> {
    Ok(&pools()?.state)
}

pub(crate) fn event_dag_db() -> PyResult<&'static Arc<PackfileStorage>> {
    Ok(&pools()?.event_dag)
}

pub(crate) fn auth_chain_db() -> PyResult<&'static Arc<PackfileStorage>> {
    Ok(&pools()?.auth_chain)
}

const RETRYABLE_READ_ERROR_PREFIX: &str = "__MTXDB_RETRYABLE_READ__: ";

/// Preserve journal contention as a retryable Python I/O error. Other storage
/// failures remain runtime errors so corruption is never retried as contention.
fn map_read_storage_error(error: StorageError) -> PyErr {
    let retryable = matches!(
        &error,
        StorageError::Io(io_error) if io_error.kind() == std::io::ErrorKind::WouldBlock
    );
    let message = format!("mtxdb get_read_committed error: {error}");
    if retryable {
        pyo3::exceptions::PyBlockingIOError::new_err(message)
    } else {
        pyo3::exceptions::PyRuntimeError::new_err(message)
    }
}

/// The generic HAMT NodeStore API carries string errors. Prefix transient
/// journal contention so its Python boundary can restore the retryable type.
fn storage_error_to_hamt_string(error: StorageError) -> String {
    match &error {
        StorageError::Io(io_error) if io_error.kind() == std::io::ErrorKind::WouldBlock => {
            format!("{RETRYABLE_READ_ERROR_PREFIX}{error}")
        }
        _ => error.to_string(),
    }
}

fn map_hamt_read_error(error: String) -> PyErr {
    if let Some(message) = error.strip_prefix(RETRYABLE_READ_ERROR_PREFIX) {
        pyo3::exceptions::PyBlockingIOError::new_err(message.to_owned())
    } else {
        pyo3::exceptions::PyRuntimeError::new_err(error)
    }
}

fn shard_type_for_key(key: &[u8]) -> ShardType {
    if key.starts_with(b"event_json:") || key.starts_with(b"prev_event_edges:") {
        ShardType::EventDag
    } else {
        ShardType::State
    }
}

fn db_for_shard_type(shard_type: ShardType) -> PyResult<&'static Arc<PackfileStorage>> {
    match shard_type {
        ShardType::State => state_db(),
        ShardType::EventDag => event_dag_db(),
        ShardType::Edges => auth_chain_db(),
    }
}

// -----------------------------------------------------------------------------
// HAMT Node Mapping
// -----------------------------------------------------------------------------

/// Extracts the room prefix and structural hash from a full node key.
fn parse_node_key(key: &[u8]) -> Option<([u8; ROOM_PREFIX_LEN], [u8; 32])> {
    if !key.starts_with(b"hamt:node:") {
        return None;
    }
    if key.len() != 124 {
        return None;
    }

    let room_prefix_hex = &key[43..59];
    let mut room_prefix = [0u8; ROOM_PREFIX_LEN];
    if hex::decode_to_slice(room_prefix_hex, &mut room_prefix).is_err() {
        return None;
    }

    let hash_hex = &key[60..124];
    let mut hash = [0u8; 32];
    if hex::decode_to_slice(hash_hex, &mut hash).is_err() {
        return None;
    }

    Some((room_prefix, hash))
}

/// Derive a HAMT root's `NodeId` within its room's own State collection --
/// a fixed `b"hamt:root:"` tag plus the namespace (still needed here: a
/// room's own collection is otherwise keyed only by the bare `state_group`
/// int, and different trial-test namespaces sharing one physical mtxdb
/// store can otherwise collide on the same state_group id -- see
/// tests/utils.py's default_config) plus the state_group, hashed and
/// truncated the same way kv_node_id/chain_node_id already derive node
/// ids from non-hash-shaped keys. Distinct from any real structural-hash
/// node id in that collection with overwhelming probability, the same
/// margin already relied on for kv_node_id's own use.
fn root_node_id(namespace: &str, state_group: i64) -> [u8; 16] {
    let mut buf = Vec::with_capacity(b"hamt:root:".len() + namespace.len() + 8);
    buf.extend_from_slice(b"hamt:root:");
    buf.extend_from_slice(namespace.as_bytes());
    buf.extend_from_slice(&state_group.to_be_bytes());
    let hash = Sha256::digest(&buf);
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

// -----------------------------------------------------------------------------
// Collection Group & Member Identity Derivations
// -----------------------------------------------------------------------------
// Ownership & Contract Division:
// - `mtxdb` owns derivation, wire encoding, and validation (recomputing the
//   128-bit member collection key from group canonical ID and member namespace,
//   and detecting 128-bit truncation collisions when an existing collection's
//   stored full identity/namespace disagrees with requested metadata).
// - `sithnapse` supplies the Matrix-specific canonical ID (`!room:server`),
//   member namespace (`STAT`, `EVNT`, `PREV`, `AUTH`), role, and schema.
//
// Metadata & Hierarchy Distinctions:
// - Physical pool:      State / Events / Edges (known from layout handle)
// - Member namespace:   STAT / EVNT / PREV / AUTH
// - Group canonical ID: !room:server
// - Group full logical: derived 256-bit BLAKE3 digest
// - Member collection:  derived 128-bit routing key (the collection key itself;
//                       not stored as a redundant metadata field)

/// Domain separation prefix for collection group full logical ID derivation.
pub const GROUP_DOMAIN_PREFIX: &[u8] = b"mtxdb/group/v1";

/// Domain separation prefix for member collection ID derivation.
pub const MEMBER_DOMAIN_PREFIX: &[u8] = b"mtxdb/member/v1";

/// Derive the full 256-bit logical identity for a collection group / entity (e.g. `!room:server`):
/// `BLAKE3-256("mtxdb/group/v1" || group_canonical_id)`
#[must_use]
pub fn group_full_logical_id(group_canonical_id: &[u8]) -> [u8; 32] {
    derive_group_full_id(group_canonical_id)
}

/// Derive the 128-bit physical collection ID for a member collection within its pool:
/// `BLAKE3-256("mtxdb/member/v1" || member_namespace || group_full_logical_id)[0..16]`
#[must_use]
pub fn member_collection_id(tag: [u8; 4], group_full_logical_id: &[u8; 32]) -> [u8; 16] {
    derive_member_collection_id_from_group(tag, *group_full_logical_id)
        .expect("member namespace is one of the mtxdb-defined namespaces")
}

/// Derive the State HAMT room collection ID from its room entity canonical ID (`!room:server`).
#[must_use]
pub(crate) fn state_hamt_room_id(room_id: &str) -> [u8; 16] {
    let group_digest = group_full_logical_id(room_id.as_bytes());
    member_collection_id(*b"STAT", &group_digest)
}

fn room_id_from_prefix(room_prefix: &[u8]) -> [u8; 16] {
    if !room_prefix.is_empty() && room_prefix[0] == b'!' {
        if let Ok(room_id) = std::str::from_utf8(room_prefix) {
            return state_hamt_room_id(room_id);
        }
    }

    let mut hasher = DigestAlgorithm::Blake3.hasher();
    hasher.update(MEMBER_DOMAIN_PREFIX);
    hasher.update(b"STAT");
    hasher.update(room_prefix);
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

/// Store HAMT root records in their room's own State collection, rather
/// than the single global flat-KV collection every other caller shares
/// (`kv_room_id()`). A root write/read is naturally room-scoped -- every
/// caller either already has `room_prefix` in hand or can derive it
/// cheaply (`state_hamt.room_hamt_prefix`) -- and mtxdb clones a
/// collection's entire index on every write, so parking every room's
/// roots in one shared collection meant a single root write's clone cost
/// scaled with the *server's total* accumulated root count instead of one
/// room's, growing without bound as a trial run persists more rooms. This
/// mirrors `put_state_hamt_nodes`'s existing per-room routing exactly.
#[pyfunction]
pub fn put_state_hamt_roots(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    roots: Vec<(i64, Vec<u8>)>,
) -> PyResult<()> {
    assert_writable()?;
    let room_id = room_id_from_prefix(&room_prefix);
    let pairs: Vec<(NodeId, NodeData)> = roots
        .into_iter()
        .map(|(state_group, value)| {
            (
                root_node_id(&namespace, state_group),
                NodeData::new(bytes::Bytes::from(value)),
            )
        })
        .collect();
    py.detach(|| {
        let engine = state_db()?;
        let committed = engine.put_many(&room_id, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })?;
        debug_assert_eq!(committed, pairs.len());
        Ok(())
    })
}

/// Tombstone HAMT root records in their room's own State collection --
/// the delete counterpart of `put_state_hamt_roots`, needed by
/// `purge_unreferenced_state_groups` and the root-deletion-queue drain.
/// mtxdb-core's `StorageEngine` trait exposes no per-key delete (only
/// `delete_collection`, a whole-collection range delete, which would drop
/// every other state_group's root in the room too) -- but `batch_delete`
/// on the old global collection was never anything more than `put_many`
/// with an empty `NodeData` value, the same "empty means absent" tombstone
/// convention `get_state_hamt_roots_for_room` and `batch_get` already
/// check for. Reuse that convention here instead of adding a new
/// mtxdb-core primitive. The corresponding room_index entry is left in
/// place: a tombstoned root reads back as a miss regardless, and
/// `state_group` ids are never reused (see the `room_index` module doc),
/// so the stale mapping is never looked up in a way that matters.
#[pyfunction]
pub fn delete_state_hamt_roots_for_room(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    state_groups: Vec<i64>,
) -> PyResult<()> {
    assert_writable()?;
    let room_id = room_id_from_prefix(&room_prefix);
    let pairs: Vec<(NodeId, NodeData)> = state_groups
        .into_iter()
        .map(|state_group| {
            (
                root_node_id(&namespace, state_group),
                NodeData::new(bytes::Bytes::new()),
            )
        })
        .collect();
    py.detach(|| {
        let engine = state_db()?;
        let committed = engine.put_many(&room_id, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb delete error: {}", e))
        })?;
        debug_assert_eq!(committed, pairs.len());
        Ok(())
    })
}

/// Point/batch-read HAMT root records from their room's own State
/// collection -- the read counterpart of `put_state_hamt_roots`. Returns
/// the raw encoded root value (as written by `_encode_state_hamt_root`)
/// for each `state_group`, or `None` on a miss; decoding stays in Python
/// (`_decode_state_hamt_root`) rather than duplicating that format here.
#[pyfunction]
pub fn get_state_hamt_roots_for_room(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    state_groups: Vec<i64>,
) -> PyResult<Vec<Option<Vec<u8>>>> {
    let room_id = room_id_from_prefix(&room_prefix);
    let node_ids: Vec<NodeId> = state_groups
        .iter()
        .map(|&sg| root_node_id(&namespace, sg))
        .collect();
    py.detach(|| {
        let engine = state_db()?;
        // Read-only workers hold an open-time collection index; refresh on a
        // miss so a state group root the writer appended after this worker
        // opened is visible (same pattern as `auth_chain_edges_get`).
        let results = engine
            .get_read_committed(&room_id, &node_ids)
            .map_err(map_read_storage_error)?;
        Ok(results
            .into_iter()
            .map(|res| {
                res.and_then(|data| {
                    if data.bytes.is_empty() {
                        None
                    } else {
                        Some(data.bytes.to_vec())
                    }
                })
            })
            .collect())
    })
}

/// Decode the root format written by Python's `_encode_state_hamt_root`.
///
/// Keeping this beside the bulk reader lets that reader cross the Python/Rust
/// boundary only once: it returns just the fields its caller needs rather than
/// raw records for Python to unpack one at a time.
fn decode_state_hamt_root(value: &[u8]) -> PyResult<(Vec<u8>, Vec<u8>, String)> {
    if value.len() < 9 || value.get(..4) != Some(b"MTHR") || value[4] != 1 {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "invalid or unsupported HAMT root record version",
        ));
    }

    let prefix_len = u16::from_be_bytes([value[5], value[6]]) as usize;
    let room_id_len_offset = 7 + prefix_len;
    if value.len() < room_id_len_offset + 2 {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "truncated HAMT root record",
        ));
    }

    let room_id_len =
        u16::from_be_bytes([value[room_id_len_offset], value[room_id_len_offset + 1]]) as usize;
    let room_id_start = room_id_len_offset + 2;
    let root_start = room_id_start + room_id_len;
    if value.len() < root_start + 32 {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "truncated HAMT root record",
        ));
    }

    let room_id = std::str::from_utf8(&value[room_id_start..root_start])
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "invalid UTF-8 room ID in HAMT root record: {e}"
            ))
        })?
        .to_owned();
    Ok((
        value[7..room_id_len_offset].to_vec(),
        value[root_start..root_start + 32].to_vec(),
        room_id,
    ))
}

/// Resolve each state group's room, fetch its HAMT root, and decode the
/// published root record in one Python/Rust FFI call. The output is aligned
/// with `state_groups`; `None` represents either a missing room-index entry or
/// a missing/tombstoned root record.
#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn get_state_hamt_roots_bulk(
    py: Python<'_>,
    namespace: String,
    state_groups: Vec<i64>,
) -> PyResult<Vec<Option<(Vec<u8>, Vec<u8>, String)>>> {
    py.detach(|| {
        let room_prefixes = room_index::get_many(&namespace, &state_groups)?;

        let mut groups_by_room: HashMap<Vec<u8>, Vec<(usize, i64)>> = HashMap::new();
        for (index, (&state_group, room_prefix)) in
            state_groups.iter().zip(room_prefixes).enumerate()
        {
            if let Some(room_prefix) = room_prefix {
                groups_by_room
                    .entry(room_prefix)
                    .or_default()
                    .push((index, state_group));
            }
        }

        let mut roots = vec![None; state_groups.len()];
        let engine = state_db()?;
        for (room_prefix, room_groups) in groups_by_room {
            let room_id = room_id_from_prefix(&room_prefix);
            let node_ids: Vec<NodeId> = room_groups
                .iter()
                .map(|(_, state_group)| root_node_id(&namespace, *state_group))
                .collect();
            // Refresh on a miss so roots for state groups the writer appended
            // after this worker opened are resolved instead of reported absent.
            let response_records = engine
                .get_read_committed(&room_id, &node_ids)
                .map_err(map_read_storage_error)?;

            for ((index, _), record) in room_groups.into_iter().zip(response_records) {
                if let Some(record) = record.filter(|record| !record.bytes.is_empty()) {
                    roots[index] = Some(decode_state_hamt_root(&record.bytes)?);
                }
            }
        }

        Ok(roots)
    })
}

/// Force a re-scan of the mtxdb collections backing `state_groups`, picking
/// up root/node writes another worker made after this process last loaded
/// (or never loaded) those rooms' collections.
///
/// Every collection's `PackfileStorage` index is private, in-process memory
/// (see `StorageEngine::refresh_collection`'s doc comment) -- a worker never
/// sees another worker's writes to a room it has already touched, and never
/// sees a room at all if it's never touched it, until this is called.
/// `room_index` itself needs no such call (`get_many`'s `pread` is always
/// current -- see its own module doc), so this only has to resolve
/// `state_groups` to their rooms via that index, then refresh each distinct
/// room's collection once. Groups with no room_index entry at all are
/// skipped: there is nothing to refresh for a group this deployment has
/// never (yet) migrated, and the caller's existing SQL-fallback/backfill
/// logic already covers that case.
///
/// Intended as a narrow, explicit retry for `_get_state_groups_from_groups_txn`
/// (bg_updates.py) to run once, on the specific groups a first read reported
/// missing, before concluding a group is genuinely corrupt -- not a blanket
/// per-read refresh, which would erase the whole point of the in-process
/// index by re-paying its scan cost on every access.
#[pyfunction]
pub fn refresh_state_hamt_collections_for_groups(
    py: Python<'_>,
    namespace: String,
    state_groups: Vec<i64>,
) -> PyResult<()> {
    py.detach(|| {
        let room_prefixes = room_index::get_many(&namespace, &state_groups)?;
        let mut seen_rooms: HashSet<Vec<u8>> = HashSet::new();
        let engine = state_db()?;
        for room_prefix in room_prefixes.into_iter().flatten() {
            if seen_rooms.insert(room_prefix.clone()) {
                let room_id = room_id_from_prefix(&room_prefix);
                engine.refresh_collection(&room_id).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "mtxdb refresh_collection error: {e}"
                    ))
                })?;
            }
        }
        Ok(())
    })
}

/// A flat, direct-offset `state_group -> room_prefix` index: entry N lives
/// at byte offset `N * ROOM_PREFIX_LEN` in a per-namespace file under
/// `<embedded_hamt_path>/room_index/<namespace_hash>.bin`. Exists so
/// `_fetch_hamt_roots_for_embedded_txn` (bg_updates.py) -- which only ever
/// has a bare `state_group` int, by design, and needs to resolve which
/// room's mtxdb collection to look a root up in -- doesn't have to touch
/// SQL or a shared PackfileStorage collection (whose per-write clone cost
/// scales with the server's *total* state-group count, the exact problem
/// this index exists to avoid). `state_group` is a small, dense, sequential
/// integer (not a content hash), so direct offset addressing needs no hash
/// table, no clone, and no rebuild -- write and read are both a single
/// syscall at a computed offset.
///
/// Three invariants this design depends on, stated explicitly:
///
/// 1. **Concurrent multi-worker writes need no locking.** Different
///    workers write disjoint `state_group` ids (ids are allocated once,
///    server-wide, by `_state_group_seq_gen`, never reused), so their
///    `pwrite`s land at disjoint, non-overlapping byte ranges. POSIX
///    requires no coordination between writers of non-overlapping regions
///    of the same regular file. Unlike `ShardPool`, this file needs no
///    `WriterLock`: two writers can never target the same offset.
/// 2. **All-zero is a valid "not (yet) written" sentinel, not ambiguous
///    with a real value.** A real `room_prefix` is derived from a hashed
///    `room_id` (`state_hamt.room_hamt_prefix`), so the chance of a
///    genuine value being `ROOM_PREFIX_LEN` zero bytes is negligible
///    (~2^-64) -- a smaller but still negligible margin than the 2^-128
///    this file relies on elsewhere (`kv_node_id`, `chain_node_id`, both
///    full 16-byte hashes) for the same kind of hash-derived id. A reader
///    that sees all-zero (sparse-file default, or a `pwrite` mid-flight and
///    not yet reflected) treats it as a miss. `pwrite`/`pread` of one
///    `ROOM_PREFIX_LEN`-byte value, well within a single page, is applied
///    atomically at the page-cache level on Linux -- a concurrent reader
///    observes either the complete old or complete new value, never a torn
///    mix.
/// 3. **This index has the same bounded durability window as the rest of
///    the embedded engine, not a weaker one.** There is no per-write
///    fsync here (matching the engine-wide move away from per-write
///    fsyncs -- see `_periodic_embedded_sync`): a crash can lose a very
///    recent mapping along with the root record it points to, since both
///    are written in the same uncommitted window. That's consistent, not
///    a new gap -- the root itself has no stronger guarantee in that same
///    window. A resulting miss for a group still inside the
///    `EMBEDDED_HAMT_MIGRATION_UPDATE_NAME` window falls through to
///    `_fetch_hamt_roots_for_embedded_txn`'s SQL fallback, same as a
///    genuine migration-window miss.
///
/// **Outside that window, a miss does not degrade gracefully.** A
/// missing or stale entry for an already-migrated, already-backfilled
/// `state_group` is not a slower read and does not self-heal on the next
/// read -- `_fetch_hamt_roots_for_embedded_txn` returns it as missing,
/// and `_get_state_groups_from_groups_txn` (store.py) then raises
/// `RuntimeError("State group(s) exist in SQL but have no HAMT root:
/// ...")`, since a group with a SQL `state_groups` row and no HAMT root
/// is treated as data corruption once both background updates have
/// finished. This fails loud, not silently, but it is a hard failure,
/// not a fallback. The only way an entry gets (re)written is a write
/// through `put_room_index` (called from
/// `_store_state_hamt_root_embedded_txn`) or a fresh run of the
/// `state_hamt_backfill_roots` background update -- never a plain read.
/// Anything that deletes or reinterprets this directory's files (e.g. a
/// change to `RECORD_LEN`/the on-disk record layout, applied in place
/// against files written under the old layout) hits this same failure
/// mode for every group it doesn't happen to already cover.
mod room_index {
    use std::collections::{HashMap, HashSet};
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::num::NonZeroUsize;
    use std::os::unix::fs::FileExt;
    use std::sync::{Arc, Mutex};

    use lru::LruCache;
    use pyo3::PyResult;
    use sha2::{Digest, Sha256};

    use super::{ROOM_INDEX_DIR, ROOM_PREFIX_LEN};

    const INDEX_MAGIC: [u8; 4] = *b"MTRI";
    const SHARD_COUNT: u8 = 64;
    const HANDLE_CACHE_CAPACITY: usize = SHARD_COUNT as usize;
    const NAMESPACE_DIGEST_LEN: usize = 16;
    const RECORD_LEN: usize = 4 + NAMESPACE_DIGEST_LEN + 8 + ROOM_PREFIX_LEN;

    // A real `room_prefix` is always exactly `ROOM_PREFIX_LEN` (8) bytes --
    // `room_hamt_prefix_raw` truncates to that length unconditionally in
    // both its branches (MSC4291 hash-derived and legacy). This used to be
    // hardcoded to 16, silently zero-padding every stored value; `get_many`
    // returned that padding along with the real prefix, and every reader
    // (`lookup_state_hamts` et al.) enforces the true 8-byte length, so a
    // padded value failed downstream with "room_prefix must be 8 bytes".
    // Keep recent mappings in-process after a successful write or read. This
    // is an optimization only: the direct-offset file remains authoritative,
    // so a worker that has not seen another worker's write simply falls back
    // to `pread`. Bounded retention keeps a long-running monolith from
    // turning the index into an unbounded second copy of all state groups.
    const PREFIX_CACHE_CAPACITY: usize = 100_000;

    #[allow(clippy::type_complexity)]
    static PREFIX_CACHE: Mutex<Option<LruCache<(String, i64), [u8; ROOM_PREFIX_LEN]>>> =
        Mutex::new(None);

    /// Handles are cached by fixed shard and access mode, never by namespace.
    /// A read-only lookup must never poison the writable handle used by `put`.
    /// This bounds the process to at most two descriptors per shard.
    #[allow(clippy::type_complexity)]
    static HANDLES: Mutex<Option<LruCache<(u8, bool), std::sync::Arc<File>>>> = Mutex::new(None);

    /// Namespaces `put` has written to since the last `sync()`. `sync()`
    /// used to unconditionally `sync_data()` every cached handle -- with
    /// `HANDLE_CACHE_CAPACITY` at 64 and one namespace per homeserver, a
    /// workload that opens many short-lived
    /// namespaces (e.g. the test suite, one per HS) fills the cache with
    /// mostly-idle handles from *other* namespaces, so every write-path
    /// sync (`sync_state`, called on essentially every event persist) paid
    /// an `fsync_data()` on all of them, not just the one actually
    /// written -- a roughly-constant per-call tax that scaled with cache
    /// occupancy, not with actual write volume. Mirrors the dirty-only
    /// fsync `PackfileStorage::sync()` (mtxdb-core) already does for the
    /// three pools proper.
    static DIRTY: Mutex<Option<HashSet<u8>>> = Mutex::new(None);

    fn shard_for(namespace: &str) -> u8 {
        namespace_digest(namespace)[0] % SHARD_COUNT
    }

    fn index_path(shard: u8, create: bool) -> PyResult<std::path::PathBuf> {
        let dir = ROOM_INDEX_DIR
            .get()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mtxdb not opened"))?;
        if create {
            std::fs::create_dir_all(dir).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to create room_index dir: {e}"
                ))
            })?;
        }
        Ok(dir.join(format!("index-{shard:02x}.bin")))
    }

    fn legacy_index_path(namespace: &str) -> PyResult<std::path::PathBuf> {
        let dir = ROOM_INDEX_DIR
            .get()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mtxdb not opened"))?;
        let namespace_hash = Sha256::digest(namespace.as_bytes());
        Ok(dir.join(format!("{}.bin", hex::encode(&namespace_hash[..16]))))
    }

    fn namespace_digest(namespace: &str) -> [u8; NAMESPACE_DIGEST_LEN] {
        let digest = Sha256::digest(namespace.as_bytes());
        let mut out = [0u8; NAMESPACE_DIGEST_LEN];
        out.copy_from_slice(&digest[..NAMESPACE_DIGEST_LEN]);
        out
    }

    fn cached_handle(namespace: &str, create: bool) -> PyResult<Option<std::sync::Arc<File>>> {
        let shard = shard_for(namespace);
        let key = (shard, create);
        let mut guard = HANDLES
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}")))?;
        let map = guard.get_or_insert_with(|| {
            LruCache::new(NonZeroUsize::new(HANDLE_CACHE_CAPACITY).expect("nonzero capacity"))
        });
        if let Some(file) = map.get(&key) {
            return Ok(Some(Arc::clone(file)));
        }
        let path = index_path(shard, create)?;
        let opened = OpenOptions::new()
            .create(create)
            .read(true)
            .write(create)
            .append(create)
            .open(&path);
        let file = match opened {
            Ok(f) => f,
            Err(e) if !create && e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to open room_index file: {e}"
                )))
            }
        };
        let file = Arc::new(file);
        map.put(key, Arc::clone(&file));
        Ok(Some(file))
    }

    pub fn put(namespace: &str, entries: &[(i64, Vec<u8>)]) -> PyResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut file = cached_handle(namespace, true)?.expect("create=true never returns None");
        let namespace_digest = namespace_digest(namespace);
        for (state_group, room_prefix) in entries {
            let mut record = [0u8; RECORD_LEN];
            record[..INDEX_MAGIC.len()].copy_from_slice(&INDEX_MAGIC);
            record[INDEX_MAGIC.len()..INDEX_MAGIC.len() + NAMESPACE_DIGEST_LEN]
                .copy_from_slice(&namespace_digest);
            let group_start = INDEX_MAGIC.len() + NAMESPACE_DIGEST_LEN;
            record[group_start..group_start + 8].copy_from_slice(&state_group.to_be_bytes());
            let prefix_start = group_start + 8;
            let n = std::cmp::min(room_prefix.len(), ROOM_PREFIX_LEN);
            record[prefix_start..prefix_start + n].copy_from_slice(&room_prefix[..n]);
            file.write_all(&record).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("room_index write failed: {e}"))
            })?;
        }
        DIRTY
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}")))?
            .get_or_insert_with(HashSet::new)
            .insert(shard_for(namespace));
        let mut cache = PREFIX_CACHE
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}")))?;
        let cache = cache.get_or_insert_with(|| {
            LruCache::new(NonZeroUsize::new(PREFIX_CACHE_CAPACITY).expect("nonzero capacity"))
        });
        for (state_group, room_prefix) in entries {
            let mut record = [0u8; ROOM_PREFIX_LEN];
            let n = std::cmp::min(room_prefix.len(), ROOM_PREFIX_LEN);
            record[..n].copy_from_slice(&room_prefix[..n]);
            // Preserve the on-disk all-zero sentinel's miss semantics even
            // for a malformed caller-provided empty prefix.
            if record.iter().any(|&b| b != 0) {
                cache.put((namespace.to_owned(), *state_group), record);
            }
        }
        Ok(())
    }

    pub fn get_many(namespace: &str, state_groups: &[i64]) -> PyResult<Vec<Option<Vec<u8>>>> {
        let mut out: Vec<Option<Vec<u8>>> = vec![None; state_groups.len()];
        let mut missing = Vec::new();
        {
            let mut cache = PREFIX_CACHE.lock().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}"))
            })?;
            if let Some(cache) = cache.as_mut() {
                for (index, &state_group) in state_groups.iter().enumerate() {
                    if let Some(room_prefix) = cache.get(&(namespace.to_owned(), state_group)) {
                        out[index] = Some(room_prefix.to_vec());
                    } else {
                        missing.push((index, state_group));
                    }
                }
            } else {
                missing.extend(state_groups.iter().copied().enumerate());
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        let wanted: HashSet<i64> = missing.iter().map(|&(_, group)| group).collect();
        let shared_records = cached_handle(namespace, false)?.and_then(|file| {
            let length = file.metadata().ok()?.len();
            let length = usize::try_from(length).ok()?;
            let mut bytes = vec![0u8; length];
            let mut offset = 0usize;
            while offset < bytes.len() {
                let count = file.read_at(&mut bytes[offset..], offset as u64).ok()?;
                if count == 0 {
                    bytes.truncate(offset);
                    break;
                }
                offset += count;
            }
            Some(bytes)
        });
        let namespace_digest = namespace_digest(namespace);
        let mut found = HashMap::new();
        if let Some(bytes) = shared_records {
            for record in bytes.chunks_exact(RECORD_LEN) {
                if record[..INDEX_MAGIC.len()] != INDEX_MAGIC
                    || record[INDEX_MAGIC.len()..INDEX_MAGIC.len() + NAMESPACE_DIGEST_LEN]
                        != namespace_digest
                {
                    continue;
                }
                let group_start = INDEX_MAGIC.len() + NAMESPACE_DIGEST_LEN;
                let group =
                    i64::from_be_bytes(record[group_start..group_start + 8].try_into().unwrap());
                if wanted.contains(&group) {
                    let prefix_start = group_start + 8;
                    let mut prefix = [0u8; ROOM_PREFIX_LEN];
                    prefix.copy_from_slice(&record[prefix_start..prefix_start + ROOM_PREFIX_LEN]);
                    if prefix.iter().any(|&b| b != 0) {
                        found.insert(group, prefix);
                    }
                }
            }
        }
        for (index, state_group) in missing {
            if let Some(prefix) = found.get(&state_group) {
                out[index] = Some(prefix.to_vec());
            }
        }
        // Read compatibility for databases written before the shared file
        // format. New writes never create another per-namespace file.
        let legacy = legacy_index_path(namespace)
            .ok()
            .and_then(|path| File::open(path).ok());
        if let Some(file) = legacy {
            for (index, state_group) in state_groups.iter().enumerate() {
                if out[index].is_some() {
                    continue;
                }
                let mut record = [0u8; ROOM_PREFIX_LEN];
                let offset = (*state_group as u64).saturating_mul(ROOM_PREFIX_LEN as u64);
                if file.read_exact_at(&mut record, offset).is_ok() && record.iter().any(|&b| b != 0)
                {
                    out[index] = Some(record.to_vec());
                }
            }
        }
        if out.iter().any(Option::is_some) {
            let mut cache = PREFIX_CACHE.lock().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}"))
            })?;
            let cache = cache.get_or_insert_with(|| {
                LruCache::new(NonZeroUsize::new(PREFIX_CACHE_CAPACITY).expect("nonzero capacity"))
            });
            for (state_group, value) in state_groups.iter().zip(out.iter()) {
                if let Some(value) = value {
                    let mut record = [0u8; ROOM_PREFIX_LEN];
                    record.copy_from_slice(value);
                    cache.put((namespace.to_owned(), *state_group), record);
                }
            }
        }
        Ok(out)
    }

    /// Flush the shared room-index handle to disk. Mirrors the bounded,
    /// periodic (not per-write) durability window the rest of the embedded
    /// engine uses -- see `_periodic_embedded_sync` on the Python side,
    /// which calls this via the top-level `sync()` pyfunction. Without
    /// this, room-index writes had no fsync point at all (unbounded loss
    /// window on crash, not just the same ~1s one as everything else); a
    /// miss still falls through to the SQL fallback in
    /// `_fetch_hamt_roots_for_embedded_txn`, so this narrows an existing
    /// gap rather than being the only thing standing between a crash and
    /// data loss.
    pub fn sync() -> PyResult<()> {
        let mut dirty: Vec<u8> = DIRTY
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}")))?
            .take()
            .unwrap_or_default()
            .into_iter()
            .collect();
        if dirty.is_empty() {
            return Ok(());
        }
        // Sync in ascending shard order. The index files are `index-00`..
        // `index-3f`, but `DIRTY` is a `HashSet` (and `std`'s hasher is
        // randomly seeded per process), so iterating it directly makes a
        // rotational disk seek across the platter on every `sync_data` in an
        // unpredictable order. mtxdb-core's pack-shard sync sorts by `pack_id`
        // for exactly this reason.
        dirty.sort_unstable();
        let guard = HANDLES
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}")))?;
        if let Some(map) = guard.as_ref() {
            for shard in dirty {
                if let Some(file) = map.peek(&(shard, true)) {
                    file.sync_data().map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "room_index sync failed: {e}"
                        ))
                    })?;
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn shared_handle_is_reused_across_namespaces() {
            super::super::auth_chain_closure_tests::ensure_open();
            let ns_kept = "ns-lru-kept";
            let prefix: Vec<u8> = b"ROOM1ABC".to_vec();
            put(ns_kept, &[(1, prefix.clone())]).expect("put kept");

            // Many namespaces share the same physical index file and handle.
            for i in 0..256 {
                put(&format!("ns-lru-rot-{}", i), &[(1, b"ROOM2XYZ".to_vec())]).expect("put rot");
            }

            {
                let guard = HANDLES.lock().expect("no poison");
                assert!(
                    guard.as_ref().map_or(0, LruCache::len) <= HANDLE_CACHE_CAPACITY,
                    "room-index handles must remain bounded by the shard count"
                );
            }

            // Clear the in-memory prefix cache so the read below exercises
            // the shared index file.
            *PREFIX_CACHE.lock().expect("no poison") = None;

            let got = get_many(ns_kept, &[1]).expect("get kept");
            assert_eq!(got, vec![Some(prefix)]);
        }

        #[test]
        fn read_handle_does_not_poison_later_write() {
            super::super::auth_chain_closure_tests::ensure_open();
            let namespace = "ns-read-before-write";

            put(namespace, &[(10_001, b"ROOM4GHI".to_vec())]).expect("initial put");
            *HANDLES.lock().expect("no poison") = None;
            *PREFIX_CACHE.lock().expect("no poison") = None;

            // This opens the shard read-only because the file already exists.
            assert_eq!(
                get_many(namespace, &[10_001]).expect("read"),
                vec![Some(b"ROOM4GHI".to_vec())]
            );

            // A later write must open a separate writable handle, rather than
            // reusing the read-only handle cached above.
            put(namespace, &[(10_002, b"ROOM5JKL".to_vec())]).expect("write after read");
        }

        #[test]
        fn sync_only_touches_dirty_namespaces_and_is_idempotent() {
            super::super::auth_chain_closure_tests::ensure_open();
            let ns = "ns-dirty-sync";
            put(ns, &[(1, b"ROOM3DEF".to_vec())]).expect("put");

            // A namespace `put` wrote to is dirty-marked and must sync
            // without error.
            sync().expect("sync after write");

            // Nothing was written since the prior sync -- must still
            // succeed (empty dirty set is a no-op, not an error). This
            // only proves the dirty-set bookkeeping doesn't panic/error on
            // an empty set; it does not measure the fsync count actually
            // skipped in production (a syscall count isn't something a
            // portable unit test can assert on).
            sync().expect("sync with nothing dirty");

            // The write is still durably readable either way.
            *PREFIX_CACHE.lock().expect("no poison") = None;
            assert_eq!(
                get_many(ns, &[1]).expect("get after sync"),
                vec![Some(b"ROOM3DEF".to_vec())]
            );
        }
    }
}

#[pyfunction]
pub fn put_room_index(
    py: Python<'_>,
    namespace: String,
    entries: Vec<(i64, Vec<u8>)>,
) -> PyResult<()> {
    assert_writable()?;
    // `room_index::put` does blocking local I/O and takes its own handle
    // cache mutex. Release the GIL while it runs so concurrent Python threads
    // (including test-harness Postgres dispatchers) are not serialized behind
    // this write.
    py.detach(|| room_index::put(&namespace, &entries))
}

#[pyfunction]
pub fn get_room_index(
    py: Python<'_>,
    namespace: String,
    state_groups: Vec<i64>,
) -> PyResult<Vec<Option<Vec<u8>>>> {
    py.detach(|| room_index::get_many(&namespace, &state_groups))
}

pub struct MtxdbStore {
    pub engine: Arc<PackfileStorage>,
}

impl NodeStore for MtxdbStore {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if let Some((room_prefix, structural_hash)) = parse_node_key(key) {
            // The writer routes nodes through the canonical room/member
            // collection derived by room_id_from_prefix.  The prefix is only
            // part of the logical node key; it is not the pack collection ID.
            let room_id = room_id_from_prefix(&room_prefix);

            let mut node_id = [0u8; 16];
            node_id.copy_from_slice(&structural_hash[..16]);

            let result = self
                .engine
                .get_read_committed(&room_id, &[node_id])
                .map_err(storage_error_to_hamt_string)?
                .into_iter()
                .next()
                .flatten();
            // Treat empty-byte tombstones (from batch_delete) as absent.
            Ok(result.and_then(|data| {
                if data.bytes.is_empty() {
                    None
                } else {
                    Some(data.bytes.to_vec())
                }
            }))
        } else {
            // Hamt roots live in the state pool's flat-KV namespace.
            let room_id = kv_room_id();
            let node_id = kv_node_id(key);
            let result = self
                .engine
                .get_read_committed(&room_id, &[node_id])
                .map_err(storage_error_to_hamt_string)?
                .into_iter()
                .next()
                .flatten();
            match result {
                None => Ok(None),
                Some(data) if data.bytes.is_empty() => Ok(None),
                Some(data) => Ok(Some(data.bytes.to_vec())),
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Auth Chain Manifests
// -----------------------------------------------------------------------------

fn namespace_room_id(namespace: &str) -> [u8; 16] {
    let hash = Sha256::digest(namespace.as_bytes());
    let mut room_id = [0u8; 16];
    room_id.copy_from_slice(&hash[..16]);
    room_id
}

fn chain_node_id(chain_id: i64) -> [u8; 16] {
    let hash = Sha256::digest(chain_id.to_be_bytes());
    let mut node_id = [0u8; 16];
    node_id.copy_from_slice(&hash[..16]);
    node_id
}

fn serialize_manifest(edges: &[(i64, i64, i64)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + edges.len() * 24);
    buf.extend_from_slice(&(edges.len() as u32).to_be_bytes());
    for &(o_seq, t_chain, t_seq) in edges {
        buf.extend_from_slice(&o_seq.to_be_bytes());
        buf.extend_from_slice(&t_chain.to_be_bytes());
        buf.extend_from_slice(&t_seq.to_be_bytes());
    }
    buf
}

fn deserialize_manifest(bytes: &[u8]) -> Vec<(i64, i64, i64)> {
    if bytes.len() < 4 {
        return Vec::new();
    }
    let count = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut edges = Vec::with_capacity(count);
    let mut offset = 4;
    for _ in 0..count {
        if offset + 24 > bytes.len() {
            break;
        }
        let o_seq = i64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let t_chain = i64::from_be_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        let t_seq = i64::from_be_bytes(bytes[offset + 16..offset + 24].try_into().unwrap());
        edges.push((o_seq, t_chain, t_seq));
        offset += 24;
    }
    edges
}

// -----------------------------------------------------------------------------
// Auth Chain Closures (short event ids + direct auth edges)
// -----------------------------------------------------------------------------
//
// See docs/docs/auth-chain-closures-plan.md (mdb repo) for the full design.
// Every key family here lives in one mtxdb collection derived from
// `(namespace, room_id)` -- unlike `namespace_room_id` above (which is
// namespace-only and shared by every room in that namespace via the
// chain_id-keyed auth-chain-links manifest), this feature needs a real
// per-room collection so `auth_chain_purge_room` can drop exactly one
// room's data with a single `delete_collection` call.

/// A room-scoped collection id for the short-id/edge closure skeleton,
/// derived as the member collection with the `AUTH` tag under the room's group identity.
pub(crate) fn auth_chain_closure_room_id(_namespace: &str, room_id: &str) -> [u8; 16] {
    let group_digest = group_full_logical_id(room_id.as_bytes());
    member_collection_id(*b"AUTH", &group_digest)
}

/// A room-scoped collection id for previous edges (DAG topology),
/// derived as the member collection with the `PREV` tag under the room's group identity.
#[must_use]
pub(crate) fn prev_edges_room_id(_namespace: &str, room_id: &str) -> [u8; 16] {
    let group_digest = group_full_logical_id(room_id.as_bytes());
    member_collection_id(*b"PREV", &group_digest)
}

/// Distinct tag prefixes keep the counter, forward mapping, reverse
/// mapping, and edge-list key spaces from colliding within one room's
/// collection (all four share the same 16-byte `NodeId` space there).
fn short_id_counter_node_id() -> NodeId {
    let hash = Sha256::digest(b"authchain:counter");
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn short_id_forward_node_id(event_id: &str) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(b"authchain:fwd:");
    hasher.update(event_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn short_id_reverse_node_id(short_id: u32) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(b"authchain:rev:");
    hasher.update(short_id.to_be_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn auth_chain_edge_node_id(short_id: u32) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(b"authchain:edges:");
    hasher.update(short_id.to_be_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

/// Inbound direction of the same graph `auth_chain_edge_node_id` stores
/// outbound: for parent short id `P`, the set of child short ids that
/// directly name `P` as one of their auth events. Needed for the V2.1
/// conflicted-subgraph algorithm's forward-reachability walk (see
/// `docs/docs/auth-chain-closures-plan.md`'s V2.1 addendum) -- ancestor
/// bitmaps alone can't answer "what's reachable *from* this event",
/// only "what's reachable *to* it."
fn auth_chain_child_node_id(short_id: u32) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(b"authchain:children:");
    hasher.update(short_id.to_be_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

/// Short ids are `u32`, per room (see plan §1) -- chosen so the Python-side
/// closure cache can use 32-bit `pyroaring.BitMap` rather than `BitMap64`.
/// 0 is never assigned (the counter starts at 0 meaning "none allocated
/// yet" and is pre-incremented before use), so it stays free as a sentinel
/// if ever needed.
const AUTH_CHAIN_SHORT_ID_MAX: u64 = u32::MAX as u64;

const AUTH_CHAIN_EDGE_ENCODING_VERSION: u8 = 1;

/// `[u8 version][u32 count]` + `count` x `u32` auth short ids. `count = 0`
/// is still 5 real bytes -- mtxdb treats an empty value as absent/tombstone
/// (see `get_raw`), so a genuinely-leaf event (zero auth events) must not
/// be stored as an empty value, or it becomes indistinguishable from
/// "edge data missing, fall back to SQL."
fn encode_auth_edges(auth_short_ids: &[u32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + auth_short_ids.len() * 4);
    buf.push(AUTH_CHAIN_EDGE_ENCODING_VERSION);
    buf.extend_from_slice(&(auth_short_ids.len() as u32).to_be_bytes());
    for &id in auth_short_ids {
        buf.extend_from_slice(&id.to_be_bytes());
    }
    buf
}

fn decode_auth_edges(bytes: &[u8]) -> PyResult<Vec<u32>> {
    if bytes.len() < 5 {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "corrupt auth-chain edge record (too short)",
        ));
    }
    if bytes[0] != AUTH_CHAIN_EDGE_ENCODING_VERSION {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "unsupported auth-chain edge record version: {}",
            bytes[0]
        )));
    }
    let count = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
    let expected_len = 5 + count * 4;
    if bytes.len() != expected_len {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "corrupt auth-chain edge record (length mismatch)",
        ));
    }
    let mut out = Vec::with_capacity(count);
    let mut offset = 5;
    for _ in 0..count {
        out.push(u32::from_be_bytes(
            bytes[offset..offset + 4].try_into().unwrap(),
        ));
        offset += 4;
    }
    Ok(out)
}

// -----------------------------------------------------------------------------
// PyO3 Bindings
// -----------------------------------------------------------------------------

/// Whether the write-ahead journal is enabled for writable opens.
///
/// Off by default. The journal changes which target receives the sync fsync
/// (the WAL segment instead of the packfile shards); it does not change when a
/// `put`'s frame reaches the packfile (eager under the default append policy)
/// or when the index checkpoint/delta is persisted. It stays opt-in because its
/// read-committed overlay only helps once the writer publishes committed
/// mutations at the transaction boundary, which is not wired yet; until then
/// the overlay would only see groups at the same sync that advances the durable
/// fingerprint.
///
/// `SYNAPSE_MTXDB_WAL` set to a truthy value (anything but 0/false/no/off/
/// empty) opts in, e.g. to exercise the overlay lane. Positive polarity: the
/// default is "off", so the name matches the variable's own meaning (unlike
/// `SYNAPSE_MTXDB_NO_SYNC`, whose safe default is "on"). When the default is
/// flipped on later it should become `SYNAPSE_MTXDB_NO_WAL`.
fn wal_enabled() -> bool {
    wal_enabled_from(std::env::var("SYNAPSE_MTXDB_WAL").ok().as_deref())
}

/// Explicitly allow a read-only worker to use a durable snapshot when the
/// database has a shared WAL. Without this override, a worker whose WAL mode
/// differs from the writer fails closed instead of silently serving stale data.
fn snapshot_workers_enabled() -> bool {
    wal_enabled_from(
        std::env::var("SYNAPSE_TEST_MTXDB_SNAPSHOT_WORKERS")
            .ok()
            .as_deref(),
    )
}

/// Pure form of [`wal_enabled`] over an already-read value, so the parsing is
/// unit-testable without mutating the process environment.
///
/// Default-off: the journal only moves the sync fsync target, and its reader
/// overlay has no benefit until the writer publishes at the transaction
/// boundary. A positive variable keeps the name matching its polarity; when
/// the default is flipped on later it should become `SYNAPSE_MTXDB_NO_WAL`.
fn wal_enabled_from(wal: Option<&str>) -> bool {
    wal.is_some_and(|value| {
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "no" | "off"
        )
    })
}

#[cfg(test)]
mod wal_env_tests {
    use super::wal_enabled_from;

    #[test]
    fn wal_defaults_off_and_only_opts_in_on_a_truthy() {
        assert!(!wal_enabled_from(None));
        for falsey in ["", "0", "false", "no", "off", " OFF "] {
            assert!(
                !wal_enabled_from(Some(falsey)),
                "{falsey:?} must not enable WAL"
            );
        }
        for truthy in ["1", "true", "yes", "on", "enabled"] {
            assert!(wal_enabled_from(Some(truthy)), "{truthy:?} must enable WAL");
        }
    }

    #[test]
    fn wal_matrix_pool_policies_disables_state_compression() {
        let policies = mtxdb::matrix_pool_policies();
        assert!(!policies.state.compress);
        assert!(policies.event_dag.compress);
        assert!(policies.edges.compress);
    }
}

#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| {
        if check_pid_guard()? {
            return Ok(());
        }
        let layout = DatabaseLayout::open(std::path::PathBuf::from(&path)).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to open mtxdb layout: {}", e))
        })?;
        let state_dir = layout.pool_dir(ShardType::State)?;
        let event_dag_dir = layout.pool_dir(ShardType::EventDag)?;
        let auth_chain_dir = layout.pool_dir(ShardType::Edges)?;
        // State pool holds HAMT nodes, roots, and state-group sidecars --
        // dense structural hashes, not text. zstd never shrinks them (see
        // mtxdb's own compression bench), so every write there was still
        // paying the compressor's full match-finding pass for nothing.
        // open_with_compression(.., false) skips the attempt entirely; the
        // event-dag/auth-chain pools (JSON-ish payloads) keep compression on.
        let shared_database = if wal_enabled() {
            Some(
                SharedDatabase::open_with_policies(
                    std::path::PathBuf::from(&path),
                    mtxdb::matrix_pool_policies(),
                )
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to open shared mtxdb database: {}",
                        e
                    ))
                })?,
            )
        } else {
            None
        };

        let (state, event_dag, auth_chain) = if let Some(database) = shared_database.as_ref() {
            (
                database.pool(ShardType::State).clone(),
                database.pool(ShardType::EventDag).clone(),
                database.pool(ShardType::Edges).clone(),
            )
        } else {
            (
                Arc::new(
                    PackfileStorage::open_with_compression(state_dir.clone(), false).map_err(
                        |e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "failed to open mtxdb state pool: {}",
                                e
                            ))
                        },
                    )?,
                ),
                Arc::new(PackfileStorage::open(event_dag_dir.clone()).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to open mtxdb event-dag pool: {}",
                        e
                    ))
                })?),
                Arc::new(PackfileStorage::open(auth_chain_dir.clone()).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to open mtxdb auth-chain pool: {}",
                        e
                    ))
                })?),
            )
        };
        // A single writer's in-memory index is authoritative for every key it
        // has written, so a negative lookup is a true miss: refreshing would
        // only spend a durable-fingerprint probe (and, after each checkpoint,
        // a full rescan) to rediscover nothing. Read-only workers keep the
        // default (refresh on) to observe records this writer appends. See
        // mtxdb-core's `PackfileStorage::set_refresh_on_miss`.
        for store in [&state, &event_dag, &auth_chain] {
            store.set_refresh_on_miss(false);
        }
        // The journal is the read-committed overlay's source of truth for
        // read-only workers: a committed group is visible to a worker's
        // `get_read_committed` before the coalescer fsyncs it, which is the
        // cross-process read-after-write path. Every root uses one tagged
        // root-level segment; the layout marker is historical metadata.
        //
        // Still gated on `wal_enabled()`: enabling it by default moves the
        // sync fsync target onto the journal, which has not been
        // A/B-verified on the writer + read-only-worker lane. Set
        // SYNAPSE_MTXDB_WAL=1 to exercise the overlay.
        let min_interval_secs = std::env::var("SYNAPSE_MTXDB_CHECKPOINT_MIN_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(30);
        let max_bytes = std::env::var("SYNAPSE_MTXDB_CHECKPOINT_MAX_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(32 * 1024 * 1024);
        if min_interval_secs > 0 || max_bytes > 0 {
            let interval = std::time::Duration::from_secs(min_interval_secs);
            state.set_checkpoint_rewrite_budget(interval, max_bytes);
            event_dag.set_checkpoint_rewrite_budget(interval, max_bytes);
            auth_chain.set_checkpoint_rewrite_budget(interval, max_bytes);
        }
        let _ = DBS.set(MtxdbPools {
            state,
            event_dag,
            auth_chain,
            _shared_database: shared_database,
        });
        let _ = OPENER_PID.set(std::process::id());
        let _ = WRITE_MODE.set(true);
        let _ = ROOM_INDEX_DIR.set(std::path::PathBuf::from(&path).join("room_index"));
        Ok(())
    })
}

/// Opens the mtxdb store read-only: no exclusive write lock is taken, so
/// this can coexist with a concurrent writer process on the same
/// directory (see `PackfileStorage::open_read_committed`'s doc comment and
/// `ShardPool::open_read_only`'s locking contract). Intended for read-only
/// worker processes in a multi-worker deployment, paired with exactly one
/// process opening the store writable via `open_client`.
///
/// A read-only-opened store's packfile index is a snapshot from open time
/// (or the last `refresh_state_hamt_collections_for_groups` call). FFI reads
/// use `get_read_committed`, which overlays complete groups from the writer's
/// journal; direct durable reads retain snapshot semantics.
///
/// Any write attempted through a read-only-opened `PackfileStorage`
/// fails at the OS level (the underlying files are opened without write
/// access) rather than corrupting anything -- this binding doesn't need
/// to add its own guard against that.
#[pyfunction]
pub fn open_client_read_only(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| {
        if check_pid_guard()? {
            return Ok(());
        }
        let layout = DatabaseLayout::open(std::path::PathBuf::from(&path)).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to open mtxdb layout: {}", e))
        })?;
        if !wal_enabled()
            && layout.shared_wal_path().is_file()
            && !snapshot_workers_enabled()
        {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "mtxdb WAL mode mismatch: shared wal.bin exists but this read-only worker has SYNAPSE_MTXDB_WAL disabled; set SYNAPSE_MTXDB_WAL=1 to use read-committed mode, or explicitly opt into a stale snapshot with SYNAPSE_TEST_MTXDB_SNAPSHOT_WORKERS=1",
            ));
        }
        let open_pool = |pool, name: &str| -> PyResult<Arc<PackfileStorage>> {
            let pool_dir = layout.pool_dir(pool).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to resolve mtxdb {name} pool directory: {e}"
                ))
            })?;
            let store = if wal_enabled() {
                PackfileStorage::open_read_committed_shared(
                    pool_dir.clone(),
                    layout.shared_wal_path(),
                    pool,
                )
            } else {
                // Without WAL, workers read the durable pack/index snapshot.
                // They intentionally do not observe uncheckpointed writer
                // mutations; this keeps WAL-disabled operation independent of
                // a root wal.bin file.
                PackfileStorage::open_read_only(pool_dir.clone()).map_err(StorageError::Io)
            }
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to open mtxdb {name} pool read-only: {e}"
                ))
            })?;
            Ok(Arc::new(store))
        };
        let state = open_pool(ShardType::State, "state")?;
        let event_dag = open_pool(ShardType::EventDag, "event-dag")?;
        let auth_chain = open_pool(ShardType::Edges, "edges")?;
        let _ = DBS.set(MtxdbPools {
            state,
            event_dag,
            auth_chain,
            _shared_database: None,
        });
        let _ = OPENER_PID.set(std::process::id());
        let _ = WRITE_MODE.set(false);
        let _ = ROOM_INDEX_DIR.set(std::path::PathBuf::from(&path).join("room_index"));
        Ok(())
    })
}

#[pyfunction]
pub fn put_state_hamt_nodes(
    py: Python<'_>,
    _namespace: String,
    room_prefix: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<()> {
    assert_writable()?;
    let room_id = room_id_from_prefix(&room_prefix);

    let t0 = std::time::Instant::now();
    let pairs: Vec<(NodeId, NodeData)> = nodes
        .into_iter()
        .filter_map(|(key_or_hash, bytes)| {
            let mut node_id = [0u8; 16];
            if key_or_hash.len() == 124 {
                if let Some((_, structural_hash)) = parse_node_key(&key_or_hash) {
                    node_id.copy_from_slice(&structural_hash[..16]);
                } else {
                    return None;
                }
            } else if key_or_hash.len() == 32 {
                node_id.copy_from_slice(&key_or_hash[..16]);
            } else if key_or_hash.len() == 16 {
                node_id.copy_from_slice(&key_or_hash);
            } else {
                return None;
            }
            Some((node_id, NodeData::new(bytes::Bytes::from(bytes))))
        })
        .collect();
    let convert_us = t0.elapsed().as_micros();

    let t1 = std::time::Instant::now();
    py.detach(|| {
        let engine = state_db()?;
        engine.put_many(&room_id, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })
    })?;
    let put_many_us = t1.elapsed().as_micros();

    // Log the split when >1ms total (avoids overhead on fast calls).
    if convert_us + put_many_us > 1000 {
        log::debug!(
            "put_state_hamt_nodes: {} nodes, convert={}us, put_many={}us",
            pairs.len(),
            convert_us,
            put_many_us,
        );
    }

    Ok(())
}

pub type AuthChainLinksForChain = (i64, Vec<(i64, i64, i64)>);

#[pyfunction]
pub fn get_auth_chain_links_batch(
    py: Python<'_>,
    namespace: String,
    chain_ids: Vec<i64>,
) -> PyResult<Vec<AuthChainLinksForChain>> {
    py.detach(|| {
        let engine = auth_chain_db()?;
        let room_id = namespace_room_id(&namespace);
        let node_ids: Vec<NodeId> = chain_ids.iter().map(|&c| chain_node_id(c)).collect();

        // Read-only workers hold an open-time collection index; refresh on a
        // miss so auth-chain links the writer appended after this worker
        // opened are visible (same pattern as `auth_chain_edges_get`).
        let results = engine
            .get_read_committed(&room_id, &node_ids)
            .map_err(map_read_storage_error)?;

        let mut out = Vec::with_capacity(node_ids.len());
        for (chain_id, res) in chain_ids.into_iter().zip(results) {
            if let Some(data) = res {
                let edges = deserialize_manifest(&data.bytes);
                if !edges.is_empty() {
                    out.push((chain_id, edges));
                }
            }
        }
        Ok(out)
    })
}

#[pyfunction]
pub fn put_auth_chain_links_batch(
    namespace: String,
    links: Vec<(i64, i64, i64, i64)>,
) -> PyResult<()> {
    assert_writable()?;
    Python::attach(|py| {
        py.detach(|| {
            // Acquire the RMW lock after releasing the GIL. The lock remains held for
            // the whole read-modify-write operation, but must not be captured by the
            // Ungil closure (MutexGuard is !Send).
            let _guard = RMW_LOCK.lock().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e))
            })?;
            let engine = auth_chain_db()?;
            let room_id = namespace_room_id(&namespace);

            let mut grouped: HashMap<i64, Vec<(i64, i64, i64)>> = HashMap::new();
            for (o_chain, o_seq, t_chain, t_seq) in links {
                grouped
                    .entry(o_chain)
                    .or_default()
                    .push((o_seq, t_chain, t_seq));
            }

            let mut pairs_to_put = Vec::with_capacity(grouped.len());

            for (chain_id, new_edges) in grouped {
                let node_id = chain_node_id(chain_id);
                let mut edges = match engine.get(&room_id, &node_id) {
                    Ok(Some(data)) => deserialize_manifest(&data.bytes),
                    Ok(None) => Vec::new(),
                    Err(e) => {
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "mtxdb get error reading chain {}: {}",
                            chain_id, e
                        )))
                    }
                };
                // Dedup against what's already stored: a retried caller (e.g. a
                // retried persist_events transaction, or a re-run background
                // migration batch) recomputes the same edges and calls this again
                // -- these writes aren't part of the SQL transaction's rollback,
                // so a retry after a partial success would otherwise duplicate
                // edges here forever. Existing edges are deduped first so an edge
                // already present twice from before this fix existed collapses
                // down rather than being preserved.
                let mut seen: HashSet<(i64, i64, i64)> = HashSet::with_capacity(edges.len());
                edges.retain(|edge| seen.insert(*edge));
                for edge in new_edges {
                    if seen.insert(edge) {
                        edges.push(edge);
                    }
                }
                let bytes = serialize_manifest(&edges);
                pairs_to_put.push((node_id, NodeData::new(bytes::Bytes::from(bytes))));
            }

            engine.put_many(&room_id, &pairs_to_put).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
            })?;
            Ok(())
        })
    })
}

#[pyfunction]
pub fn delete_auth_chain_links_batch(namespace: String, pairs: Vec<(i64, i64)>) -> PyResult<()> {
    assert_writable()?;
    Python::attach(|py| {
        py.detach(|| {
            let _guard = RMW_LOCK.lock().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e))
            })?;
            let engine = auth_chain_db()?;
            let room_id = namespace_room_id(&namespace);

            let mut grouped: HashMap<i64, HashSet<i64>> = HashMap::new();
            for (o_chain, o_seq) in pairs {
                grouped.entry(o_chain).or_default().insert(o_seq);
            }

            let mut pairs_to_put = Vec::with_capacity(grouped.len());

            for (chain_id, seqs_to_delete) in grouped {
                let node_id = chain_node_id(chain_id);
                let data = engine.get(&room_id, &node_id).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get error: {}", e))
                })?;
                if let Some(data) = data {
                    let edges = deserialize_manifest(&data.bytes);
                    let filtered: Vec<_> = edges
                        .into_iter()
                        .filter(|(seq, _, _)| !seqs_to_delete.contains(seq))
                        .collect();

                    let bytes = serialize_manifest(&filtered);
                    pairs_to_put.push((node_id, NodeData::new(bytes::Bytes::from(bytes))));
                }
            }

            if !pairs_to_put.is_empty() {
                engine.put_many(&room_id, &pairs_to_put).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
                })?;
            }
            Ok(())
        })
    })
}

/// Room-scoped: for each `event_id`, returns its `u32` short id, allocating
/// one if it doesn't exist yet. See plan §0(1) for the recovery protocol
/// this implements -- the counter, forward mapping, and reverse mapping
/// are a single three-record invariant maintained here, not sequenced from
/// Python.
///
/// Crash-induced inconsistencies this tolerates (both self-healing):
///   - counter incremented, forward write never happened: the short id is
///     just never assigned to anything (ids need not be dense).
///   - forward written, reverse write never happened: repaired the next
///     time *any* caller resolves this same event id through this
///     function (see the "verify and restore reverse" branch below) --
///     not only when the forward mapping happened to be absent.
/// No ordering can produce a reverse mapping without a matching forward
/// one, which is the only inconsistency this design cannot tolerate.
#[pyfunction]
pub fn get_or_create_short_ids(
    namespace: String,
    room_id: String,
    event_ids: Vec<String>,
) -> PyResult<Vec<u32>> {
    assert_writable()?;
    Python::attach(|py| {
        py.detach(|| {
            let _guard = RMW_LOCK.lock().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e))
            })?;
            let engine = db_for_shard_type(ShardType::Edges)?;
            let collection = auth_chain_closure_room_id(&namespace, &room_id);

            let mut out = Vec::with_capacity(event_ids.len());
            let mut counter_value: Option<u64> = None;

            for event_id in event_ids {
                let fwd_id = short_id_forward_node_id(&event_id);
                let existing = engine.get(&collection, &fwd_id).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get error: {}", e))
                })?;

                if let Some(data) = existing {
                    if data.bytes.len() != 4 {
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(
                            "corrupt auth-chain short-id forward record",
                        ));
                    }
                    let short_id = u32::from_be_bytes(data.bytes.as_ref().try_into().unwrap());

                    // Verify + repair the reverse mapping even though the forward
                    // mapping already exists (see docstring above).
                    let rev_id = short_id_reverse_node_id(short_id);
                    let rev_present = engine.get(&collection, &rev_id).map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get error: {}", e))
                    })?;
                    if rev_present.is_none() {
                        engine
                            .put(
                                &collection,
                                &rev_id,
                                &NodeData::new(bytes::Bytes::from(event_id.into_bytes())),
                            )
                            .map_err(|e| {
                                pyo3::exceptions::PyRuntimeError::new_err(format!(
                                    "mtxdb put error repairing reverse mapping: {}",
                                    e
                                ))
                            })?;
                    }
                    out.push(short_id);
                    continue;
                }

                let counter_id = short_id_counter_node_id();
                let current = match counter_value {
                    Some(v) => v,
                    None => match engine.get(&collection, &counter_id) {
                        Ok(Some(data)) if data.bytes.len() == 8 => {
                            u64::from_be_bytes(data.bytes.as_ref().try_into().unwrap())
                        }
                        Ok(None) => 0,
                        Ok(Some(_)) => {
                            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                                "corrupt auth-chain short-id counter record",
                            ));
                        }
                        Err(e) => {
                            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "mtxdb get error reading counter: {}",
                                e
                            )))
                        }
                    },
                };
                let next = current.checked_add(1).ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "auth-chain short-id space exhausted for room",
                    )
                })?;
                if next > AUTH_CHAIN_SHORT_ID_MAX {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "auth-chain short-id space exhausted for room",
                    ));
                }
                let short_id = next as u32;
                counter_value = Some(next);

                engine
                    .put(
                        &collection,
                        &counter_id,
                        &NodeData::new(bytes::Bytes::copy_from_slice(&next.to_be_bytes())),
                    )
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "mtxdb put error updating counter: {}",
                            e
                        ))
                    })?;

                // Forward mapping first (authoritative), then reverse -- see
                // docstring above for why this order matters for crash recovery.
                engine
                    .put(
                        &collection,
                        &fwd_id,
                        &NodeData::new(bytes::Bytes::copy_from_slice(&short_id.to_be_bytes())),
                    )
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "mtxdb put error writing forward mapping: {}",
                            e
                        ))
                    })?;

                let rev_id = short_id_reverse_node_id(short_id);
                engine
                    .put(
                        &collection,
                        &rev_id,
                        &NodeData::new(bytes::Bytes::from(event_id.into_bytes())),
                    )
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "mtxdb put error writing reverse mapping: {}",
                            e
                        ))
                    })?;

                out.push(short_id);
            }

            Ok(out)
        })
    })
}

/// Room-scoped batch reverse lookup: `short_id -> event_id`, `None` for an
/// unresolved/dangling short id (never an error -- see plan §4, downstream
/// consumers must filter these, matching congruent's own precedent).
#[pyfunction]
pub fn resolve_short_ids_to_event_ids(
    py: Python<'_>,
    namespace: String,
    room_id: String,
    short_ids: Vec<u32>,
) -> PyResult<Vec<Option<String>>> {
    py.detach(|| {
        let engine = db_for_shard_type(ShardType::Edges)?;
        let collection = auth_chain_closure_room_id(&namespace, &room_id);
        let node_ids: Vec<NodeId> = short_ids
            .iter()
            .map(|&s| short_id_reverse_node_id(s))
            .collect();
        // Refresh on a miss so ids the writer appended after this worker
        // opened are resolved instead of reported absent.
        let results = engine
            .get_read_committed(&collection, &node_ids)
            .map_err(map_read_storage_error)?;
        results
            .into_iter()
            .map(|opt| match opt {
                Some(data) => String::from_utf8(data.bytes.to_vec())
                    .map(Some)
                    .map_err(|_| {
                        pyo3::exceptions::PyRuntimeError::new_err(
                            "corrupt auth-chain reverse mapping (invalid utf8)",
                        )
                    }),
                None => Ok(None),
            })
            .collect()
    })
}

/// Room-scoped batch read of direct auth-edge lists, keyed by
/// `event_short_id`. `None` means "not embedded yet, go fetch SQL"
/// (plan §2/§3's cold-import signal); `Some(vec![])` means "genuinely a
/// leaf event with zero auth events, stop here."
#[pyfunction]
pub fn auth_chain_edges_get(
    py: Python<'_>,
    namespace: String,
    room_id: String,
    short_ids: Vec<u32>,
) -> PyResult<Vec<Option<Vec<u32>>>> {
    py.detach(|| {
        let engine = db_for_shard_type(ShardType::Edges)?;
        let collection = auth_chain_closure_room_id(&namespace, &room_id);
        let node_ids: Vec<NodeId> = short_ids
            .iter()
            .map(|&s| auth_chain_edge_node_id(s))
            .collect();
        let results = engine
            .get_read_committed(&collection, &node_ids)
            .map_err(map_read_storage_error)?;

        results
            .into_iter()
            .map(|opt| match opt {
                Some(data) => decode_auth_edges(&data.bytes).map(Some),
                None => Ok(None),
            })
            .collect()
    })
}

/// Room-scoped batch write of direct auth-edge lists. `rows`:
/// `(event_short_id, [auth_short_id, ...])`. Idempotent -- writing the same
/// key with the same value again is a no-op in effect, so a dropped/retried
/// post-commit dual-write (plan §2) is always safe to redo.
#[pyfunction]
pub fn auth_chain_edges_put(
    py: Python<'_>,
    namespace: String,
    room_id: String,
    rows: Vec<(u32, Vec<u32>)>,
) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        let engine = db_for_shard_type(ShardType::Edges)?;
        let collection = auth_chain_closure_room_id(&namespace, &room_id);
        let pairs: Vec<(NodeId, NodeData)> = rows
            .into_iter()
            .map(|(short_id, auth_short_ids)| {
                let node_id = auth_chain_edge_node_id(short_id);
                let bytes = encode_auth_edges(&auth_short_ids);
                (node_id, NodeData::new(bytes::Bytes::from(bytes)))
            })
            .collect();
        let committed = engine.put_many(&collection, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })?;
        debug_assert_eq!(committed, pairs.len());
        Ok(())
    })
}

/// Room-scoped batch read of inbound (child) edge lists, keyed by
/// `parent_short_id`. `None` means "no known children recorded yet" --
/// unlike the outbound `auth_chain_edges_get`, this is *not* a cold-import
/// signal: a parent can validly have zero known children forever (a
/// forward extremity), and children are appended incrementally as they're
/// each persisted, so "not found" here just means "none seen so far",
/// same information as `Some(vec![])` would carry. Both are treated
/// identically by callers.
#[pyfunction]
pub fn auth_chain_children_get(
    py: Python<'_>,
    namespace: String,
    room_id: String,
    short_ids: Vec<u32>,
) -> PyResult<Vec<Option<Vec<u32>>>> {
    py.detach(|| {
        let engine = db_for_shard_type(ShardType::Edges)?;
        let collection = auth_chain_closure_room_id(&namespace, &room_id);
        let node_ids: Vec<NodeId> = short_ids
            .iter()
            .map(|&s| auth_chain_child_node_id(s))
            .collect();
        // Refresh on a miss so auth-chain children the writer appended after
        // this worker opened are visible (see `auth_chain_edges_get`).
        let results = engine
            .get_read_committed(&collection, &node_ids)
            .map_err(map_read_storage_error)?;
        results
            .into_iter()
            .map(|opt| match opt {
                Some(data) => decode_auth_edges(&data.bytes).map(Some),
                None => Ok(None),
            })
            .collect()
    })
}

/// Room-scoped, idempotent, deduplicating append of new children to each
/// listed parent's inbound edge list. `rows`: `(parent_short_id,
/// [new_child_short_id, ...])`. Unlike `auth_chain_edges_put` (one
/// full overwrite per event, written once), a parent's child list grows
/// incrementally as new children are persisted over the room's lifetime,
/// so this must read-modify-write and dedupe -- the same shape as
/// `put_auth_chain_links_batch`'s existing dedup-on-append pattern.
#[pyfunction]
pub fn auth_chain_children_append(
    namespace: String,
    room_id: String,
    rows: Vec<(u32, Vec<u32>)>,
) -> PyResult<()> {
    assert_writable()?;
    Python::attach(|py| {
        py.detach(|| {
            let _guard = RMW_LOCK.lock().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e))
            })?;
            let engine = db_for_shard_type(ShardType::Edges)?;
            let collection = auth_chain_closure_room_id(&namespace, &room_id);

            // Aggregate by parent first: the batched put below only applies at the
            // end, so two rows for the same parent in one call would otherwise each
            // read the same *un-put* state and both emit a put for the same key --
            // last-write-wins silently dropping the first row's children.
            let mut by_parent: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for (parent_short_id, new_children) in rows {
                if new_children.is_empty() {
                    continue;
                }
                by_parent
                    .entry(parent_short_id)
                    .or_default()
                    .extend(new_children);
            }

            let mut pairs_to_put = Vec::with_capacity(by_parent.len());
            for (parent_short_id, new_children) in by_parent {
                let node_id = auth_chain_child_node_id(parent_short_id);
                let mut children = match engine.get(&collection, &node_id) {
                    Ok(Some(data)) => decode_auth_edges(&data.bytes)?,
                    Ok(None) => Vec::new(),
                    Err(e) => {
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "mtxdb get error reading children of {}: {}",
                            parent_short_id, e
                        )))
                    }
                };
                let mut seen: HashSet<u32> = children.iter().copied().collect();
                let mut changed = false;
                for child in new_children {
                    if seen.insert(child) {
                        children.push(child);
                        changed = true;
                    }
                }
                if changed {
                    let bytes = encode_auth_edges(&children);
                    pairs_to_put.push((node_id, NodeData::new(bytes::Bytes::from(bytes))));
                }
            }

            if !pairs_to_put.is_empty() {
                engine.put_many(&collection, &pairs_to_put).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
                })?;
            }
            Ok(())
        })
    })
}

/// Room-scoped full purge of the closure skeleton (counter, forward/reverse
/// short-id mappings, and edge lists) for one room -- everything written
/// under `auth_chain_closure_room_id(namespace, room_id)`. Does *not*
/// manage any cache-generation record: RAM-closure invalidation is handled
/// purely in-process on the Python side (plan §3/§4), and the caller must
/// bump that generation *before* calling this, not after.
#[pyfunction]
pub fn auth_chain_purge_room(py: Python<'_>, namespace: String, room_id: String) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        let engine = db_for_shard_type(ShardType::Edges)?;
        let collection = auth_chain_closure_room_id(&namespace, &room_id);
        engine.delete_collection(&collection).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "mtxdb delete_collection error: {}",
                e
            ))
        })
    })
}

// -----------------------------------------------------------------------------
// Generic KV (Event JSON / Event-to-State-Group)
// -----------------------------------------------------------------------------

pub(crate) fn kv_room_id() -> [u8; 16] {
    derive_collection_id(Some(MEMBER_NAMESPACE_INTL), b"sys:flat-kv")
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn state_group_aux_collection_id() -> [u8; 16] {
    derive_collection_id(Some(MEMBER_NAMESPACE_INTL), b"sys:matrix-state-groups")
}

fn kv_node_id(key: &[u8]) -> [u8; 16] {
    let hash = Sha256::digest(key);
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

/// Fetch flat-KV records, routing each key to its shard type internally.
#[pyfunction]
pub fn batch_get(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    py.detach(|| {
        let mut state_ids = Vec::new();
        let mut event_ids = Vec::new();
        for (position, key) in keys.iter().enumerate() {
            let entry = (position, kv_node_id(key));
            match shard_type_for_key(key) {
                ShardType::State => state_ids.push(entry),
                ShardType::EventDag => event_ids.push(entry),
                ShardType::Edges => unreachable!("flat KV never routes to edges"),
            }
        }
        let mut values = vec![None; keys.len()];
        for (shard_type, ids) in [
            (ShardType::State, state_ids),
            (ShardType::EventDag, event_ids),
        ] {
            if ids.is_empty() {
                continue;
            }
            let node_ids: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
            let engine = db_for_shard_type(shard_type)?;
            let room_id = kv_room_id();
            // Read-only workers keep an in-process collection index. If the
            // writer published a mapping after this worker opened the store,
            // `get_read_committed` retries the *missing* keys once after
            // refreshing the generic-KV collection, and suppresses repeat
            // refreshes for confirmed negatives against an unchanged
            // collection (see mtxdb-core's `get_read_committed`).
            let found = engine
                .get_read_committed(&room_id, &node_ids)
                .map_err(map_read_storage_error)?;
            for ((position, _), value) in ids.into_iter().zip(found) {
                values[position] = value;
            }
        }
        Ok(keys
            .into_iter()
            .zip(values)
            .filter_map(|(key, value)| {
                value
                    .filter(|data| !data.bytes.is_empty())
                    .map(|data| (key, data.bytes.to_vec()))
            })
            .collect())
    })
}

/// Shared implementation for `batch_put`/`batch_delete`: routes each key to
/// its shard type and writes the (possibly empty, for tombstones) value.
/// Does NOT reject empty values itself -- `batch_delete` relies on writing
/// empty bytes as its deletion marker. Only the public `batch_put` entrypoint
/// enforces the empty-value ban, to stop external callers from accidentally
/// colliding with that tombstone encoding.
fn batch_put_impl(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    let mut state_puts = Vec::new();
    let mut event_puts = Vec::new();
    for (key, value) in pairs {
        let entry = (kv_node_id(&key), NodeData::new(bytes::Bytes::from(value)));
        match shard_type_for_key(&key) {
            ShardType::State => state_puts.push(entry),
            ShardType::EventDag => event_puts.push(entry),
            ShardType::Edges => unreachable!("flat KV never routes to edges"),
        }
    }
    for (shard_type, puts) in [
        (ShardType::State, state_puts),
        (ShardType::EventDag, event_puts),
    ] {
        if !puts.is_empty() {
            db_for_shard_type(shard_type)?
                .put_many(&kv_room_id(), &puts)
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
                })?;
        }
    }
    Ok(())
}

/// Store flat-KV records, routing each key to its shard type internally.
/// Rejects empty values to avoid collision with the tombstone encoding
/// used by `batch_delete` (which writes empty bytes as a deletion marker).
#[pyfunction]
pub fn batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        for (_, value) in &pairs {
            if value.is_empty() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "batch_put does not accept empty values (used as tombstones)",
                ));
            }
        }
        batch_put_impl(pairs)
    })
}

/// Tombstone flat-KV records in the shard type selected from each key.
#[pyfunction]
pub fn batch_delete(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        let pairs = keys.into_iter().map(|key| (key, Vec::new())).collect();
        batch_put_impl(pairs)
    })
}

// -----------------------------------------------------------------------------
// Event JSON mirror (room-aware)
// -----------------------------------------------------------------------------

/// Number of deterministic locator collections the event_id -> room_mapping is
/// sharded across. A single global locator would accumulate every event id in
/// one index -- the exact unbounded-growth problem the old global
/// `kv_room_id()` collection had (a whole server's worth of event ids parked
/// in one collection, so every index write/rewrite/delta cost scaled with the
/// total). Bucketing keeps any one locator collection's index cost proportional
/// to 1/N of the server's event ids.
const EVENT_LOCATOR_BUCKETS: u32 = 256;

/// One `event_json_get` result row: `(event_id, internal_metadata, body)`.
/// `body` is `format_version(4B) + json`; either half is `None` on a miss
/// (including a partial mirror write, which counts as a miss rather than a
/// mismatched pair -- see `event_json_get`'s doc comment).
type EventJsonGetRow = (String, Option<Vec<u8>>, Option<Vec<u8>>);

/// 128-bit identity for an event inside the mirror, derived exactly the way
/// every other `NodeId` in this adapter is (first 16 bytes of SHA-256 over a
/// domain-separated key -- see `kv_node_id`/`chain_node_id`/`root_node_id`).
/// No u64 truncation of the event id anywhere; the id is hashed in full.
pub(crate) fn event_node_id(namespace: &str, event_id: &str) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(b"event_json:event:");
    hasher.update(namespace.as_bytes());
    hasher.update(b"\0");
    hasher.update(event_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

/// Domain-separated sibling of `event_node_id`: same event, distinct node,
/// holding `internal_metadata` as its own physically separate record instead
/// of packed into the body blob. Lives in the same room EventDag collection
/// as the body (same locator resolves both), so no extra locator lookup is
/// needed to find it -- see `event_json_put`/`event_json_get`.
fn event_meta_node_id(namespace: &str, event_id: &str) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(b"event_json:meta:");
    hasher.update(namespace.as_bytes());
    hasher.update(b"\0");
    hasher.update(event_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

/// Deterministic locator collection for `node_id`: one of
/// `EVENT_LOCATOR_BUCKETS` collections, picked from low bits of the event's
/// own node id so a server's event ids spread evenly across canonical locator collections
/// (`sys:event-locator:{bucket}`).
pub(crate) fn event_locator_collection_id(_namespace: &str, node_id: &NodeId) -> [u8; 16] {
    let bucket = u32::from_le_bytes([node_id[0], node_id[1], node_id[2], node_id[3]])
        % EVENT_LOCATOR_BUCKETS;
    let canonical = format!("sys:event-locator:{bucket}");
    derive_collection_id(Some(*b"EVNT"), canonical.as_bytes())
}

/// Room-scoped EventDag collection id: derived as the member collection
/// with the `EVNT` tag under the room's group identity (`!room:server`).
pub(crate) fn event_dag_room_id(_namespace: &str, room_id: &str) -> [u8; 16] {
    let group_digest = group_full_logical_id(room_id.as_bytes());
    member_collection_id(*b"EVNT", &group_digest)
}

// -----------------------------------------------------------------------------
// Driver-Owned Collection Metadata Constructors
// -----------------------------------------------------------------------------
// Sithnapse supplies the Matrix-specific group canonical ID (`!room:server` or
// `sys:*`), the member namespace (`STAT`, `EVNT`, `PREV`, `AUTH`), and the
// schema/role for each collection family. mtxdb recomputes the 128-bit member
// ID and cross-checks the metadata to detect 128-bit truncation collisions.

#[allow(dead_code)]
#[must_use]
pub(crate) fn flat_kv_metadata() -> CollectionMetadata {
    CollectionMetadata {
        member_namespace: Some(MEMBER_NAMESPACE_INTL),
        collection_canonical_id: b"sys:flat-kv".to_vec(),
        record_id_rule: RecordIdentityRule {
            policy: FrameIdPolicy::ExternalCanonicalIdToCrosscheck,
            digest_algorithm: DigestAlgorithm::Sha256,
        },
        payload: PayloadPolicy::Source,
        extension: None,
        role: Some("system_auxiliary".to_owned()),
        schema: Some("sithnapse.flat-kv.v1".to_owned()),
    }
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn state_group_aux_metadata() -> CollectionMetadata {
    CollectionMetadata {
        member_namespace: Some(MEMBER_NAMESPACE_INTL),
        collection_canonical_id: b"sys:matrix-state-groups".to_vec(),
        record_id_rule: RecordIdentityRule {
            policy: FrameIdPolicy::ExternalCanonicalIdToCrosscheck,
            digest_algorithm: DigestAlgorithm::Sha256,
        },
        payload: PayloadPolicy::Source,
        extension: None,
        role: Some("system_auxiliary".to_owned()),
        schema: Some("sithnapse.state-groups.v1".to_owned()),
    }
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn event_locator_metadata(bucket: u32) -> CollectionMetadata {
    CollectionMetadata {
        member_namespace: Some(*b"EVNT"),
        collection_canonical_id: format!("sys:event-locator:{bucket}").into_bytes(),
        record_id_rule: RecordIdentityRule {
            policy: FrameIdPolicy::ExternalCanonicalIdToCrosscheck,
            digest_algorithm: DigestAlgorithm::Sha256,
        },
        payload: PayloadPolicy::Source,
        extension: None,
        role: Some("event_locator".to_owned()),
        schema: Some("sithnapse.event-locator.v1".to_owned()),
    }
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn event_dag_metadata(room_id: &str) -> CollectionMetadata {
    CollectionMetadata {
        member_namespace: Some(*b"EVNT"),
        collection_canonical_id: room_id.as_bytes().to_vec(),
        record_id_rule: RecordIdentityRule {
            policy: FrameIdPolicy::ExternalCanonicalIdToCrosscheck,
            digest_algorithm: DigestAlgorithm::Sha256,
        },
        payload: PayloadPolicy::Source,
        extension: None,
        role: Some("event_dag".to_owned()),
        schema: Some("sithnapse.event-dag.v1".to_owned()),
    }
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn auth_chain_metadata(room_id: &str) -> CollectionMetadata {
    CollectionMetadata {
        member_namespace: Some(*b"AUTH"),
        collection_canonical_id: room_id.as_bytes().to_vec(),
        record_id_rule: RecordIdentityRule {
            policy: FrameIdPolicy::ExternalCanonicalIdToCrosscheck,
            digest_algorithm: DigestAlgorithm::Sha256,
        },
        payload: PayloadPolicy::Source,
        extension: None,
        role: Some("auth_chain".to_owned()),
        schema: Some("sithnapse.auth-chain.v1".to_owned()),
    }
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn prev_edges_metadata(room_id: &str) -> CollectionMetadata {
    CollectionMetadata {
        member_namespace: Some(*b"PREV"),
        collection_canonical_id: room_id.as_bytes().to_vec(),
        record_id_rule: RecordIdentityRule {
            policy: FrameIdPolicy::ExternalCanonicalIdToCrosscheck,
            digest_algorithm: DigestAlgorithm::Sha256,
        },
        payload: PayloadPolicy::Source,
        extension: None,
        role: Some("previous_edges".to_owned()),
        schema: Some("sithnapse.previous-edges.v1".to_owned()),
    }
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn state_hamt_metadata(room_id: &str) -> CollectionMetadata {
    CollectionMetadata {
        member_namespace: Some(*b"STAT"),
        collection_canonical_id: room_id.as_bytes().to_vec(),
        record_id_rule: RecordIdentityRule {
            policy: FrameIdPolicy::ExternalCanonicalIdToCrosscheck,
            digest_algorithm: DigestAlgorithm::Sha256,
        },
        payload: PayloadPolicy::Source,
        extension: None,
        role: Some("state_hamt".to_owned()),
        schema: Some("sithnapse.state-hamt.v1".to_owned()),
    }
}

/// Put split event_json records into the room-aware mirror: a locator entry
/// (`event_locator_collection_id` bucket -> room EventDag collection id) and
/// two physically separate records in the room's own EventDag collection --
/// the body (`event_node_id`) and `internal_metadata` (`event_meta_node_id`).
/// Splitting these means a raw dump of one node (e.g. via `mtxdb get`) never
/// mixes the two together, and mirrors SQL's own `event_json` table, which
/// has always carried `internal_metadata` and `json` as separate columns
/// (see `full_schemas/72/full.sql.sqlite`) -- this collapses back to that
/// same separation instead of the single length-prefixed blob the mirror
/// used before. Both share one locator: they live in the same collection, so
/// resolving it once finds both.
///
/// `rows` are `(room_id, event_id, internal_metadata_bytes, body_bytes)`.
/// `body_bytes` is `format_version(4B, signed, -1 = NULL) + json`; encoded on
/// the Python side (`_encode_event_json_body`) and preserved byte-for-byte.
/// `internal_metadata_bytes` is the utf-8 metadata JSON, stored as-is.
///
/// Writes are plain `put_many` (idempotent overwrite -- censoring, expiry, and
/// re-signing replace the body in place through this same call). Room bodies
/// are persisted BEFORE their locators are published: a reader that races
/// between the two sees a miss and falls back to SQL, never a locator
/// pointing at a not-yet-written body.
///
/// Like the old `batch_put` path, writes are deliberately not fsynced by
/// default -- the Python caller decides via `maybe_sync`, and a lost unflushed
/// write only costs a slower SQL-fallback read, never silent data loss.
/// Generic KV (`batch_put`) remains for flat data and no longer carries
/// event_json.
#[pyfunction]
pub fn event_json_put(
    py: Python<'_>,
    namespace: String,
    rows: Vec<(String, String, Vec<u8>, Vec<u8>)>,
) -> PyResult<()> {
    assert_writable()?;
    for (_, _, metadata, body) in &rows {
        if metadata.is_empty() || body.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "event_json_put does not accept empty metadata/body (used as tombstones)",
            ));
        }
    }
    py.detach(|| {
        let engine = event_dag_db()?;
        // Group by collection so each locator bucket / room dag collection is
        // written with a single put_many.
        let mut locator_puts: HashMap<[u8; 16], Vec<(NodeId, NodeData)>> = HashMap::new();
        let mut dag_puts: HashMap<[u8; 16], Vec<(NodeId, NodeData)>> = HashMap::new();
        for (room_id, event_id, metadata, body) in rows {
            let identity = event_node_id(&namespace, &event_id);
            let meta_identity = event_meta_node_id(&namespace, &event_id);
            let room_collection = event_dag_room_id(&namespace, &room_id);
            let locator_collection = event_locator_collection_id(&namespace, &identity);
            let puts = dag_puts.entry(room_collection).or_default();
            puts.push((identity, NodeData::new(bytes::Bytes::from(body))));
            puts.push((meta_identity, NodeData::new(bytes::Bytes::from(metadata))));
            locator_puts.entry(locator_collection).or_default().push((
                identity,
                NodeData::new(bytes::Bytes::copy_from_slice(&room_collection)),
            ));
        }
        // Body writes first, locator publication last: a stale locator may
        // only ever be a miss, never a pointer to a body that isn't there.
        for (collection, pairs) in dag_puts {
            engine.put_many(&collection, &pairs).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
            })?;
        }
        for (collection, pairs) in locator_puts {
            engine.put_many(&collection, &pairs).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
            })?;
        }
        Ok(())
    })
}

/// Read split event_json records by event id: resolve each id's locator to
/// its room EventDag collection, then read the metadata and body records
/// from that room collection. Ids with no locator entry are simply a miss
/// (the caller's SQL `event_json` fallback takes over) -- no legacy combined-
/// blob format is supported; this mirror predates any released version, so
/// there's nothing to stay compatible with. Empty-byte values are treated as
/// absent everywhere.
///
/// Returns `(event_id, metadata, body)` in input order; ids the mirror has
/// nowhere return `(None, None)` and the caller's SQL `event_json` fallback
/// takes over, exactly as before.
#[pyfunction]
pub fn event_json_get(
    py: Python<'_>,
    namespace: String,
    event_ids: Vec<String>,
) -> PyResult<Vec<EventJsonGetRow>> {
    py.detach(|| {
        let engine = event_dag_db()?;
        let node_ids: Vec<NodeId> = event_ids
            .iter()
            .map(|id| event_node_id(&namespace, id))
            .collect();

        // Phase 1 -- resolve room collections via the locator, grouped by
        // bucket collection so each is fetched with a single get_many.
        let mut locator_ids: HashMap<[u8; 16], Vec<(usize, NodeId)>> = HashMap::new();
        for (position, node_id) in node_ids.iter().enumerate() {
            locator_ids
                .entry(event_locator_collection_id(&namespace, node_id))
                .or_default()
                .push((position, *node_id));
        }
        let mut room_collections: Vec<Option<[u8; 16]>> = vec![None; event_ids.len()];
        for (collection, ids) in locator_ids {
            let node_ids_only: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
            // Read-only workers hold an open-time index; refresh on a miss so
            // locators for events the writer appended after this worker opened
            // resolve instead of reporting the event absent.
            let found = engine
                .get_read_committed(&collection, &node_ids_only)
                .map_err(map_read_storage_error)?;
            for ((position, _), value) in ids.into_iter().zip(found) {
                if let Some(data) = value {
                    if !data.bytes.is_empty() {
                        if let Ok(room) = <[u8; 16]>::try_from(data.bytes.as_ref()) {
                            room_collections[position] = Some(room);
                        }
                    }
                }
            }
        }

        // Phase 2 -- read metadata + body from the resolved room collections,
        // grouped the same way (both node ids per position, tagged so the
        // results can be told apart after one combined get_many); ids
        // without a locator are simply a miss.
        #[derive(Clone, Copy)]
        enum Kind {
            Body,
            Meta,
        }
        let mut dag_ids: HashMap<[u8; 16], Vec<(usize, Kind, NodeId)>> = HashMap::new();
        for (position, room_collection) in room_collections.iter().enumerate() {
            if let Some(room_collection) = room_collection {
                let entries = dag_ids.entry(*room_collection).or_default();
                entries.push((position, Kind::Body, node_ids[position]));
                entries.push((
                    position,
                    Kind::Meta,
                    event_meta_node_id(&namespace, &event_ids[position]),
                ));
            }
        }
        let mut bodies: Vec<Option<Vec<u8>>> = vec![None; event_ids.len()];
        let mut metadatas: Vec<Option<Vec<u8>>> = vec![None; event_ids.len()];
        for (collection, ids) in dag_ids {
            let node_ids_only: Vec<NodeId> = ids.iter().map(|(_, _, id)| *id).collect();
            // Refresh on a miss so event bodies/metadata the writer appended
            // after this worker opened are visible to the read.
            let found = engine
                .get_read_committed(&collection, &node_ids_only)
                .map_err(map_read_storage_error)?;
            for ((position, kind, _), value) in ids.into_iter().zip(found) {
                if let Some(data) = value {
                    if !data.bytes.is_empty() {
                        match kind {
                            Kind::Body => bodies[position] = Some(data.bytes.to_vec()),
                            Kind::Meta => metadatas[position] = Some(data.bytes.to_vec()),
                        }
                    }
                }
            }
        }

        Ok(event_ids
            .into_iter()
            .zip(metadatas)
            .zip(bodies)
            .map(|((id, metadata), body)| (id, metadata, body))
            .collect())
    })
}

/// Tombstone event_json mirror records: resolve each event's locator (if
/// any), then tombstone the room-local body, metadata, and locator entry.
/// Locator-driven: no room_id needed, so history purge, which only has event
/// ids, can use it directly.
#[pyfunction]
pub fn event_json_delete(
    py: Python<'_>,
    namespace: String,
    event_ids: Vec<String>,
) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        let engine = event_dag_db()?;
        let node_ids: Vec<NodeId> = event_ids
            .iter()
            .map(|id| event_node_id(&namespace, id))
            .collect();

        // Resolve locators first (grouped by bucket collection).
        let mut locator_ids: HashMap<[u8; 16], Vec<(usize, NodeId)>> = HashMap::new();
        for (position, node_id) in node_ids.iter().enumerate() {
            locator_ids
                .entry(event_locator_collection_id(&namespace, node_id))
                .or_default()
                .push((position, *node_id));
        }
        let mut room_collections: Vec<Option<[u8; 16]>> = vec![None; event_ids.len()];
        for (collection, ids) in locator_ids {
            let node_ids_only: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
            let found = engine.get_many(&collection, &node_ids_only).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {e}"))
            })?;
            for ((position, _), value) in ids.into_iter().zip(found) {
                if let Some(data) = value {
                    if !data.bytes.is_empty() {
                        if let Ok(room) = <[u8; 16]>::try_from(data.bytes.as_ref()) {
                            room_collections[position] = Some(room);
                        }
                    }
                }
            }
        }

        // Tombstone locators (always) and room-local bodies+metadata (when a
        // room was resolved). Empty values are the deletion marker -- same
        // encoding `batch_delete` already uses.
        let mut locator_tombs: HashMap<[u8; 16], Vec<(NodeId, NodeData)>> = HashMap::new();
        let mut dag_tombs: HashMap<[u8; 16], Vec<(NodeId, NodeData)>> = HashMap::new();
        for (position, room_collection) in room_collections.iter().enumerate() {
            let identity = node_ids[position];
            let locator_collection = event_locator_collection_id(&namespace, &identity);
            locator_tombs
                .entry(locator_collection)
                .or_default()
                .push((identity, NodeData::new(bytes::Bytes::new())));
            if let Some(room_collection) = room_collection {
                let tombs = dag_tombs.entry(*room_collection).or_default();
                tombs.push((identity, NodeData::new(bytes::Bytes::new())));
                tombs.push((
                    event_meta_node_id(&namespace, &event_ids[position]),
                    NodeData::new(bytes::Bytes::new()),
                ));
            }
        }
        for (collection, pairs) in locator_tombs {
            engine.put_many(&collection, &pairs).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
            })?;
        }
        for (collection, pairs) in dag_tombs {
            engine.put_many(&collection, &pairs).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
            })?;
        }
        Ok(())
    })
}

/// Whole-room purge of the event mirror: drops the room's EventDag collection
/// outright with `delete_collection`. Locator entries for the room's events
/// live in the bucketed locator collections and are NOT enumerated here --
/// `purge_events.py`'s room purge still runs `event_json_delete` over the
/// room's known event ids first, which tombstones those; any locator left
/// behind resolves to a now-empty collection and its read falls back to SQL,
/// so it is never served stale data.
#[pyfunction]
pub fn event_json_purge_room(py: Python<'_>, namespace: String, room_id: String) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        let engine = event_dag_db()?;
        let collection = event_dag_room_id(&namespace, &room_id);
        engine.delete_collection(&collection).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "mtxdb delete_collection error: {}",
                e
            ))
        })
    })
}

// -----------------------------------------------------------------------------
// HAMT Materialize / Lookup Wrappers
// -----------------------------------------------------------------------------

use rezzy::hamt::StructuralHash;

use crate::database::hamt_store::{self, NodeCache, StateEntries};
use crate::state_hamt::room_structural_key_raw;

static NODE_CACHE: OnceCell<NodeCache> = OnceCell::new();

fn node_cache() -> &'static NodeCache {
    NODE_CACHE.get_or_init(hamt_store::new_node_cache)
}

#[pyfunction]
pub fn materialize_state_hamt(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    root_structural_hash: Vec<u8>,
    room_id: &str,
) -> PyResult<Option<StateEntries>> {
    let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "room_prefix must be {} bytes",
            ROOM_PREFIX_LEN
        ))
    })?;
    let root_structural_hash: StructuralHash = root_structural_hash.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
    })?;
    let structural_key = room_structural_key_raw(room_id);

    py.detach(|| {
        let engine = state_db()?;
        let store = MtxdbStore {
            engine: Arc::clone(engine),
        };

        hamt_store::materialize_state_hamt(
            &store,
            node_cache(),
            &namespace,
            &room_prefix,
            root_structural_hash,
            &structural_key,
        )
        .map(Some)
        .map_err(map_hamt_read_error)
    })
}

#[pyfunction]
pub fn materialize_state_hamts(
    py: Python<'_>,
    namespace: String,
    roots: Vec<(Vec<u8>, Vec<u8>, String)>,
) -> PyResult<Vec<StateEntries>> {
    let roots = roots
        .into_iter()
        .map(|(room_prefix, root_hash, room_id)| {
            let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "room_prefix must be {} bytes",
                    ROOM_PREFIX_LEN
                ))
            })?;
            let root_hash: StructuralHash = root_hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
            })?;
            let structural_key = room_structural_key_raw(&room_id);
            Ok((room_prefix, structural_key, root_hash))
        })
        .collect::<PyResult<Vec<_>>>()?;

    py.detach(|| {
        let engine = state_db()?;
        let store = MtxdbStore {
            engine: Arc::clone(engine),
        };

        hamt_store::materialize_state_hamts(&store, node_cache(), &namespace, roots)
            .map_err(map_hamt_read_error)
    })
}

pub type PySelectiveQuery = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<(String, String)>);

#[pyfunction]
pub fn lookup_state_hamts(
    py: Python<'_>,
    namespace: String,
    queries: Vec<PySelectiveQuery>,
) -> PyResult<Vec<StateEntries>> {
    let parsed_queries = queries
        .into_iter()
        .map(|(room_prefix, root_hash, structural_key, keys)| {
            let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "room_prefix must be {} bytes",
                    ROOM_PREFIX_LEN
                ))
            })?;
            let root_hash: StructuralHash = root_hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
            })?;
            let structural_key: [u8; 32] = structural_key.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("structural_key must be 32 bytes")
            })?;
            Ok((room_prefix, root_hash, structural_key, keys))
        })
        .collect::<PyResult<Vec<_>>>()?;

    py.detach(|| {
        let engine = state_db()?;
        let store = MtxdbStore {
            engine: Arc::clone(engine),
        };

        hamt_store::lookup_state_hamts(&store, node_cache(), &namespace, parsed_queries)
            .map_err(map_hamt_read_error)
    })
}

pub type PyRootRecord = (i64, Vec<u8>, Vec<u8>, String, Vec<u8>);

#[pyfunction]
pub fn increment_counters_batch(pairs: Vec<(Vec<u8>, i64)>) -> PyResult<Vec<i64>> {
    assert_writable()?;
    Python::attach(|py| {
        py.detach(|| {
            let _guard = RMW_LOCK.lock().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e))
            })?;
            let engine = state_db()?;
            let room_id = kv_room_id();
            let mut results = Vec::with_capacity(pairs.len());
            let mut puts = Vec::with_capacity(pairs.len());

            for (key, delta) in pairs {
                let node_id = kv_node_id(&key);
                let current = match engine.get(&room_id, &node_id) {
                    Ok(Some(data)) => {
                        if key.starts_with(b"state_group_refcount:") {
                            REFCOUNT_EXISTING.fetch_add(1, Ordering::Relaxed);
                        }
                        if data.bytes.len() == 8 {
                            i64::from_be_bytes(data.bytes.as_ref().try_into().unwrap())
                        } else {
                            0
                        }
                    }
                    Ok(None) => {
                        if key.starts_with(b"state_group_refcount:") {
                            REFCOUNT_INITS.fetch_add(1, Ordering::Relaxed);
                        }
                        0
                    }
                    Err(e) => {
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "mtxdb get error reading counter: {}",
                            e
                        )))
                    }
                };
                let new_value = current + delta;
                results.push(new_value);
                puts.push((
                    node_id,
                    NodeData::new(bytes::Bytes::copy_from_slice(&new_value.to_be_bytes())),
                ));
            }

            if !puts.is_empty() {
                engine.put_many(&room_id, &puts).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
                })?;
            }

            Ok(results)
        })
    })
}

/// Batch-read HAMT nodes from the native mtxdb store using the same
/// `(room_prefix, structural_hash)` key encoding that `put_state_hamt_nodes`
/// uses.  Returns one `Option<Vec<u8>>` per requested hash (`None` for misses).
#[pyfunction]
pub fn get_state_hamt_nodes_batch(
    py: Python<'_>,
    _namespace: String,
    room_prefix: Vec<u8>,
    hashes: Vec<Vec<u8>>,
) -> PyResult<Vec<Option<Vec<u8>>>> {
    // Keep reads on the same room-scoped collection as put_state_hamt_nodes.
    // The old reader truncated room_prefix directly while the writer derived
    // the collection ID, so every node written by the new path became
    // invisible to incremental state persistence.
    let room_id = room_id_from_prefix(&room_prefix);

    let node_ids: Vec<NodeId> = hashes
        .iter()
        .map(|h| {
            let mut node_id = [0u8; 16];
            let copy_len = std::cmp::min(h.len(), 16);
            node_id[..copy_len].copy_from_slice(&h[..copy_len]);
            node_id
        })
        .collect();

    py.detach(|| {
        let engine = state_db()?;
        // Refresh on a miss so HAMT nodes the writer appended after this
        // worker opened are visible to the read.
        let results = engine
            .get_read_committed(&room_id, &node_ids)
            .map_err(map_read_storage_error)?;
        Ok(results
            .into_iter()
            .map(|opt| opt.map(|d| d.bytes.to_vec()))
            .collect())
    })
}

#[pyfunction]
pub fn sync(py: Python<'_>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        // Sync mtxdb pools before publishing the room index. A durable
        // index entry must never point at a root that was not yet synced.
        for (name, engine) in [
            ("state", state_db()?),
            ("event-dag", event_dag_db()?),
            ("auth-chain", auth_chain_db()?),
        ] {
            sync_one(name, engine)?;
        }
        room_index::sync()?;
        Ok(())
    })
}

/// Sync only the `state` pool (plus the room index, which is published
/// off state-group root writes -- see `room_index::sync`'s doc comment on
/// why an index entry must never be synced ahead of the root it points
/// at). Callers should prefer this (or `sync_event_dag`/`sync_auth_chain`)
/// over the blanket `sync()`: a DURABLE write only needs its own pool
/// durable, not every other pool's currently-dirty shards along with it.
#[pyfunction]
pub fn sync_state(py: Python<'_>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        sync_one("state", state_db()?)?;
        room_index::sync()
    })
}

/// Sync only the `event_dag` pool (event_json bodies + locator buckets,
/// prev_event_edges).
#[pyfunction]
pub fn sync_event_dag(py: Python<'_>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| sync_one("event-dag", event_dag_db()?))
}

/// Sync only the `auth_chain` pool (auth-chain links, manifests, short-id
/// index).
#[pyfunction]
pub fn sync_auth_chain(py: Python<'_>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| sync_one("auth-chain", auth_chain_db()?))
}

/// Publish all queued mutations without fsyncing them.
///
/// In WAL mode the pools share one JournalCoordinator, so the first call drains
/// the shared queue and the remaining calls are no-ops. In non-WAL mode each
/// pool has its own journal, so all three must be published. Durability is
/// provided separately by the coalesced sync path.
///
/// This is not transaction-scoped: a pending mutation from another concurrent
/// SQL transaction can be published by this call. Non-WAL publication is also
/// sequential across the three journals, not atomic. Callers must treat this
/// as a visibility optimization until the storage layer provides transaction-
/// scoped publication or the caller serializes embedded writes through SQL
/// commit.
#[pyfunction]
pub fn publish_pending(py: Python<'_>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        publish_journal("state", state_db()?)?;
        publish_journal("event-dag", event_dag_db()?)?;
        publish_journal("auth-chain", auth_chain_db()?)?;
        Ok(())
    })
}

fn publish_journal(name: &str, engine: &Arc<PackfileStorage>) -> PyResult<()> {
    let journal = engine.journal().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "mtxdb publish error for {name} pool: journal is unavailable"
        ))
    })?;
    journal.publish_pending().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "mtxdb publish error for {name} pool: {e}"
        ))
    })?;
    Ok(())
}

fn sync_one(name: &str, engine: &Arc<PackfileStorage>) -> PyResult<()> {
    engine.sync().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb sync error for {name} pool: {e}"))
    })
}

// ---------------------------------------------------------------------------
// Runtime stats
// ---------------------------------------------------------------------------

use pyo3::types::PyDict;

fn stats_to_dict(
    py: Python<'_>,
    name: &str,
    s: &mtxdb::packfile::storage::RuntimeStats,
    sync_diagnostics: &mtxdb::packfile::storage::SyncDiagnosticsSnapshot,
) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("pool", name)?;
    d.set_item("open_count", s.open_count)?;
    d.set_item("get_calls", s.get_calls)?;
    d.set_item("get_misses", s.get_misses)?;
    d.set_item("get_many_calls", s.get_many_calls)?;
    d.set_item("get_many_records", s.get_many_records)?;
    d.set_item("get_many_misses", s.get_many_misses)?;
    d.set_item("miss_refreshes", s.miss_refreshes)?;
    d.set_item("miss_refresh_skips", s.miss_refresh_skips)?;
    d.set_item("miss_refresh_recovered", s.miss_refresh_recovered)?;
    d.set_item("miss_refresh_retry_ids", s.miss_refresh_retry_ids)?;
    d.set_item("index_candidates", s.index_candidates)?;
    d.set_item("candidate_reads", s.candidate_reads)?;
    d.set_item("candidate_hash_mismatches", s.candidate_hash_mismatches)?;
    d.set_item("candidate_frame_bytes", s.candidate_frame_bytes)?;
    d.set_item("put_calls", s.put_calls)?;
    d.set_item("put_bytes", s.put_bytes)?;
    d.set_item("put_many_calls", s.put_many_calls)?;
    d.set_item("put_many_records", s.put_many_records)?;
    d.set_item("put_many_bytes", s.put_many_bytes)?;
    d.set_item("put_many_fast_path_calls", s.put_many_fast_path_calls)?;
    d.set_item("put_many_clone_path_calls", s.put_many_clone_path_calls)?;
    d.set_item("index_clone_time_us", s.index_clone_time.as_micros() as u64)?;
    d.set_item("index_grow_count", s.index_grow_count)?;
    d.set_item("index_rebuild_count", s.index_rebuild_count)?;
    d.set_item("sync_calls", s.sync_calls)?;
    d.set_item("checkpoint_writes", s.checkpoint_writes)?;
    d.set_item("checkpoint_skips", s.checkpoint_skips)?;
    d.set_item("delta_appends", s.delta_appends)?;
    d.set_item("read_reloads", s.read_reloads)?;
    d.set_item("read_reload_failures", s.read_reload_failures)?;
    d.set_item("delta_invalidations", s.delta_invalidations)?;
    d.set_item("cache_hits", s.cache.hits)?;
    d.set_item("cache_misses", s.cache.misses)?;
    d.set_item("cache_hit_rate", s.cache.hit_rate)?;
    d.set_item("repack_count", s.repack.repack_count)?;
    d.set_item("repack_kept", s.repack.kept_total)?;
    d.set_item("repack_dropped", s.repack.dropped_total)?;
    d.set_item("index_bytes", s.index_bytes)?;
    d.set_item("collection_count", s.collection_count)?;
    d.set_item("shard_count", s.shards.len())?;
    if name == "state" {
        d.set_item("refcount_inits", REFCOUNT_INITS.load(Ordering::Relaxed))?;
        d.set_item(
            "refcount_existing",
            REFCOUNT_EXISTING.load(Ordering::Relaxed),
        )?;
    }
    let mut shard_writes: u64 = 0;
    let mut shard_bytes: u64 = 0;
    let mut shard_syncs: u64 = 0;
    for (_, ss) in &s.shards {
        shard_writes += ss.write_count;
        shard_bytes += ss.bytes_written;
        shard_syncs += ss.sync_count;
    }
    d.set_item("shard_write_count", shard_writes)?;
    d.set_item("shard_bytes_written", shard_bytes)?;
    d.set_item("shard_sync_count", shard_syncs)?;
    if let Some(ref ot) = s.last_open_timings {
        let od = PyDict::new(py);
        od.set_item("shard_open_us", ot.shard_open.as_micros() as u64)?;
        od.set_item("shard_discovery_us", ot.shard_discovery.as_micros() as u64)?;
        od.set_item("writer_lock_us", ot.writer_lock.as_micros() as u64)?;
        od.set_item(
            "packfile_recovery_us",
            ot.packfile_recovery.as_micros() as u64,
        )?;
        od.set_item("packfile_recovery_calls", ot.packfile_recovery_calls)?;
        od.set_item("packfile_open_us", ot.packfile_open.as_micros() as u64)?;
        od.set_item("packfile_open_calls", ot.packfile_open_calls)?;
        od.set_item(
            "metadata_restore_us",
            ot.metadata_restore.as_micros() as u64,
        )?;
        od.set_item(
            "pool_meta_restore_us",
            ot.pool_meta_restore.as_micros() as u64,
        )?;
        od.set_item(
            "persisted_stats_restore_us",
            ot.persisted_stats_restore.as_micros() as u64,
        )?;
        od.set_item(
            "store_meta_write_us",
            ot.store_meta_write.as_micros() as u64,
        )?;
        od.set_item(
            "pool_meta_persist_us",
            ot.pool_meta_persist.as_micros() as u64,
        )?;
        od.set_item(
            "initial_pack_create_us",
            ot.initial_pack_create.as_micros() as u64,
        )?;
        od.set_item(
            "metadata_unattributed_us",
            ot.metadata_unattributed.as_micros() as u64,
        )?;
        od.set_item(
            "shard_open_unattributed_us",
            ot.shard_open_unattributed.as_micros() as u64,
        )?;
        od.set_item("metadata_load_us", ot.metadata_load.as_micros() as u64)?;
        od.set_item(
            "checkpoint_decode_us",
            ot.checkpoint_decode.as_micros() as u64,
        )?;
        od.set_item("fingerprint_us", ot.fingerprint.as_micros() as u64)?;
        od.set_item(
            "index_materialization_us",
            ot.index_materialization.as_micros() as u64,
        )?;
        od.set_item("delta_replay_us", ot.delta_replay.as_micros() as u64)?;
        od.set_item("full_scan_us", ot.full_scan.as_micros() as u64)?;
        od.set_item("total_us", ot.total.as_micros() as u64)?;
        d.set_item("last_open_timings", od)?;
    }
    if let Some(ref st) = s.last_sync_timings {
        let sd = PyDict::new(py);
        sd.set_item("pack_flush_us", st.pack_flush.as_micros() as u64)?;
        sd.set_item("pack_fsync_us", st.pack_fsync.as_micros() as u64)?;
        sd.set_item("sidecar_us", st.sidecar.as_micros() as u64)?;
        sd.set_item("delta_log_us", st.delta_log.as_micros() as u64)?;
        sd.set_item("checkpoint_us", st.checkpoint.as_micros() as u64)?;
        sd.set_item("wal_us", st.wal.as_micros() as u64)?;
        sd.set_item(
            "journal_lock_wait_us",
            st.journal_lock_wait.as_micros() as u64,
        )?;
        sd.set_item(
            "journal_pending_wait_us",
            st.journal_pending_wait.as_micros() as u64,
        )?;
        sd.set_item("journal_append_us", st.journal_append.as_micros() as u64)?;
        sd.set_item("journal_fsync_us", st.journal_fsync.as_micros() as u64)?;
        sd.set_item("journal_sync_calls", st.journal_sync_calls)?;
        sd.set_item("journal_bytes", st.journal_bytes)?;
        sd.set_item("journal_records", st.journal_records)?;
        sd.set_item("journal_waiters", st.journal_waiters)?;
        sd.set_item("journal_coalesced", st.journal_coalesced)?;
        sd.set_item("journal_in_flight", st.journal_in_flight)?;
        sd.set_item("dirty_lock_wait_us", st.dirty_lock_wait.as_micros() as u64)?;
        sd.set_item(
            "pending_publish_age_us",
            st.pending_publish_age.as_micros() as u64,
        )?;
        sd.set_item("total_us", st.total.as_micros() as u64)?;
        d.set_item("last_sync_timings", sd)?;
    }
    let st = s.sync_totals;
    let sd = PyDict::new(py);
    sd.set_item("calls", st.calls)?;
    sd.set_item("pack_flush_us", st.pack_flush.as_micros() as u64)?;
    sd.set_item("pack_fsync_us", st.pack_fsync.as_micros() as u64)?;
    sd.set_item("sidecar_us", st.sidecar.as_micros() as u64)?;
    sd.set_item("delta_log_us", st.delta_log.as_micros() as u64)?;
    sd.set_item("checkpoint_us", st.checkpoint.as_micros() as u64)?;
    sd.set_item("wal_us", st.wal.as_micros() as u64)?;
    sd.set_item(
        "journal_lock_wait_us",
        st.journal_lock_wait.as_micros() as u64,
    )?;
    sd.set_item(
        "journal_pending_wait_us",
        st.journal_pending_wait.as_micros() as u64,
    )?;
    sd.set_item("journal_append_us", st.journal_append.as_micros() as u64)?;
    sd.set_item("journal_fsync_us", st.journal_fsync.as_micros() as u64)?;
    sd.set_item("journal_sync_calls", st.journal_sync_calls)?;
    sd.set_item("journal_bytes", st.journal_bytes)?;
    sd.set_item("journal_records", st.journal_records)?;
    sd.set_item("journal_waiters", st.journal_waiters)?;
    sd.set_item("journal_coalesced", st.journal_coalesced)?;
    sd.set_item("dirty_lock_wait_us", st.dirty_lock_wait.as_micros() as u64)?;
    sd.set_item(
        "pending_publish_age_us",
        st.pending_publish_age.as_micros() as u64,
    )?;
    sd.set_item(
        "max_journal_lock_wait_us",
        st.max_journal_lock_wait.as_micros() as u64,
    )?;
    sd.set_item(
        "max_journal_fsync_us",
        st.max_journal_fsync.as_micros() as u64,
    )?;
    sd.set_item("total_us", st.total.as_micros() as u64)?;
    d.set_item("sync_totals", sd)?;
    let diagnostics = PyDict::new(py);
    diagnostics.set_item(
        "peak_journal_in_flight",
        sync_diagnostics.peak_journal_in_flight,
    )?;
    d.set_item("sync_diagnostics", diagnostics)?;
    Ok(d.unbind())
}

/// Return a dict-of-dicts with runtime stats for all three pools.
///
/// Read counters are only meaningful after `set_stats_enabled(true)` was
/// called; write/batch/sync counters are always-on.
#[pyfunction]
pub fn stats(py: Python<'_>) -> PyResult<Py<PyDict>> {
    stats_impl(py, false)
}

/// Return runtime stats and take/reset per-interval diagnostics for each pool.
#[pyfunction]
pub fn stats_snapshot(py: Python<'_>) -> PyResult<Py<PyDict>> {
    stats_impl(py, true)
}

fn stats_impl(py: Python<'_>, take_diagnostics: bool) -> PyResult<Py<PyDict>> {
    let snapshots = py.detach(
        || -> Result<
            Vec<(
                &str,
                mtxdb::packfile::storage::RuntimeStats,
                mtxdb::packfile::storage::SyncDiagnosticsSnapshot,
            )>,
            pyo3::PyErr,
        > {
            let pools = pools()?;
            let state_stats = pools.state.stats();
            let event_dag_stats = pools.event_dag.stats();
            let auth_chain_stats = pools.auth_chain.stats();
            Ok(vec![
                (
                    "state",
                    state_stats.clone(),
                    if take_diagnostics {
                        pools.state.take_sync_diagnostics()
                    } else {
                        state_stats.sync_diagnostics.clone()
                    },
                ),
                (
                    "event_dag",
                    event_dag_stats.clone(),
                    if take_diagnostics {
                        pools.event_dag.take_sync_diagnostics()
                    } else {
                        event_dag_stats.sync_diagnostics.clone()
                    },
                ),
                (
                    "auth_chain",
                    auth_chain_stats.clone(),
                    if take_diagnostics {
                        pools.auth_chain.take_sync_diagnostics()
                    } else {
                        auth_chain_stats.sync_diagnostics.clone()
                    },
                ),
            ])
        },
    )?;
    let out = PyDict::new(py);
    for (name, s, diagnostics) in &snapshots {
        out.set_item(*name, stats_to_dict(py, name, s, diagnostics)?)?;
    }
    Ok(out.unbind())
}

/// Zero all runtime counters (except open_count and persisted pool stats).
#[pyfunction]
pub fn reset_stats(py: Python<'_>) -> PyResult<()> {
    py.detach(|| {
        let pools = pools()?;
        pools.state.reset_stats();
        pools.event_dag.reset_stats();
        pools.auth_chain.reset_stats();
        REFCOUNT_INITS.store(0, Ordering::Relaxed);
        REFCOUNT_EXISTING.store(0, Ordering::Relaxed);
        Ok(())
    })
}

/// Enable or disable logical read-path counters (get/get_many).
/// Write/batch/sync counters are always-on.
#[pyfunction]
pub fn set_stats_enabled(py: Python<'_>, enabled: bool) -> PyResult<()> {
    py.detach(|| {
        let pools = pools()?;
        pools.state.set_stats_enabled(enabled);
        pools.event_dag.set_stats_enabled(enabled);
        pools.auth_chain.set_stats_enabled(enabled);
        Ok(())
    })
}

/// Bound how often a structurally-needed full checkpoint rewrite (the delta
/// log being invalidated) may run, across all pools. Both budgets are
/// unlimited at zero and the pair is disabled when both are zero (the
/// default). A deferred rewrite is write-neutral: packfiles are still synced
/// first, so only the index acceleration file stays stale, costing the next
/// open a rescan. See `mtxdb`'s `set_checkpoint_rewrite_budget` and the
/// `checkpoint_skips` stat.
#[pyfunction]
pub fn set_checkpoint_rewrite_budget(
    py: Python<'_>,
    min_interval_secs: f64,
    max_bytes: u64,
) -> PyResult<()> {
    let interval = std::time::Duration::try_from_secs_f64(min_interval_secs)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("invalid interval: {e}")))?;
    py.detach(|| {
        let pools = pools()?;
        for store in [&pools.state, &pools.event_dag, &pools.auth_chain] {
            store.set_checkpoint_rewrite_budget(interval, max_bytes);
        }
        Ok(())
    })
}

/// Repack every collection in every pool, retaining every indexed record.
///
/// This is exposed for test diagnostics: it removes physical garbage and
/// rewrites live indexes without applying application-level reachability rules.
#[pyfunction]
pub fn repack(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let summaries = py.detach(|| -> PyResult<Vec<(&'static str, usize, usize, usize)>> {
        let Some(pools) = DBS.get() else {
            return Ok(Vec::new());
        };
        assert_writable()?;

        let mut out = Vec::new();
        for (name, store) in [
            ("state", &pools.state),
            ("event_dag", &pools.event_dag),
            ("auth_chain", &pools.auth_chain),
        ] {
            let collection_ids = store.collection_ids();
            let results = store
                .repack_collections_reachable(&collection_ids, |_hash, _data| Vec::new())
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "mtxdb repack error for {name} pool: {e}"
                    ))
                })?;
            let kept = results.iter().map(|(_, kept, _)| *kept).sum();
            let dropped = results.iter().map(|(_, _, dropped)| *dropped).sum();
            out.push((name, results.len(), kept, dropped));
        }
        Ok(out)
    })?;

    let out = PyDict::new(py);
    for (name, collections, kept, dropped) in summaries {
        let pool = PyDict::new(py);
        pool.set_item("collections", collections)?;
        pool.set_item("kept", kept)?;
        pool.set_item("dropped", dropped)?;
        out.set_item(name, pool)?;
    }
    Ok(out.unbind())
}

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open_client, m)?)?;
    m.add_function(wrap_pyfunction!(open_client_read_only, m)?)?;
    m.add_function(wrap_pyfunction!(put_state_hamt_nodes, m)?)?;
    m.add_function(wrap_pyfunction!(get_state_hamt_nodes_batch, m)?)?;
    m.add_function(wrap_pyfunction!(get_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(put_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(delete_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(get_or_create_short_ids, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_short_ids_to_event_ids, m)?)?;
    m.add_function(wrap_pyfunction!(auth_chain_edges_get, m)?)?;
    m.add_function(wrap_pyfunction!(auth_chain_edges_put, m)?)?;
    m.add_function(wrap_pyfunction!(auth_chain_children_get, m)?)?;
    m.add_function(wrap_pyfunction!(auth_chain_children_append, m)?)?;
    m.add_function(wrap_pyfunction!(auth_chain_purge_room, m)?)?;
    m.add_function(wrap_pyfunction!(batch_get, m)?)?;
    m.add_function(wrap_pyfunction!(batch_put, m)?)?;
    m.add_function(wrap_pyfunction!(batch_delete, m)?)?;
    m.add_function(wrap_pyfunction!(event_json_put, m)?)?;
    m.add_function(wrap_pyfunction!(event_json_get, m)?)?;
    m.add_function(wrap_pyfunction!(event_json_delete, m)?)?;
    m.add_function(wrap_pyfunction!(event_json_purge_room, m)?)?;
    m.add_function(wrap_pyfunction!(materialize_state_hamt, m)?)?;
    m.add_function(wrap_pyfunction!(materialize_state_hamts, m)?)?;
    m.add_function(wrap_pyfunction!(lookup_state_hamts, m)?)?;
    m.add_function(wrap_pyfunction!(put_state_hamt_roots, m)?)?;
    m.add_function(wrap_pyfunction!(get_state_hamt_roots_for_room, m)?)?;
    m.add_function(wrap_pyfunction!(get_state_hamt_roots_bulk, m)?)?;
    m.add_function(wrap_pyfunction!(
        refresh_state_hamt_collections_for_groups,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(delete_state_hamt_roots_for_room, m)?)?;
    m.add_function(wrap_pyfunction!(put_room_index, m)?)?;
    m.add_function(wrap_pyfunction!(get_room_index, m)?)?;
    m.add_function(wrap_pyfunction!(increment_counters_batch, m)?)?;
    m.add_function(wrap_pyfunction!(sync, m)?)?;
    m.add_function(wrap_pyfunction!(sync_state, m)?)?;
    m.add_function(wrap_pyfunction!(sync_event_dag, m)?)?;
    m.add_function(wrap_pyfunction!(sync_auth_chain, m)?)?;
    m.add_function(wrap_pyfunction!(publish_pending, m)?)?;
    m.add_function(wrap_pyfunction!(stats, m)?)?;
    m.add_function(wrap_pyfunction!(stats_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(reset_stats, m)?)?;
    m.add_function(wrap_pyfunction!(set_stats_enabled, m)?)?;
    m.add_function(wrap_pyfunction!(set_checkpoint_rewrite_budget, m)?)?;
    m.add_function(wrap_pyfunction!(repack, m)?)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod auth_chain_closure_tests {
    //! `DBS` is a process-global `OnceCell` (see `open_client`): only the
    //! first call in this test binary actually opens a store, and every
    //! test after that silently reuses it. Isolation between tests
    //! therefore comes from each test using its own unique namespace/room
    //! id, not from separate storage -- exactly the same assumption the
    //! feature makes in production (one mtxdb file, many rooms).
    use std::sync::Once;

    use super::*;

    static INIT: Once = Once::new();

    pub(crate) fn ensure_open() {
        INIT.call_once(|| {
            // This crate is normally loaded as a cdylib into an
            // already-running CPython process; a standalone `cargo test`
            // binary has no interpreter of its own yet.
            pyo3::Python::initialize();
            let dir = tempfile::tempdir().expect("tempdir");
            // Leak the TempDir so it isn't cleaned up while DBS still
            // holds paths into it for the rest of the test binary's life.
            let path = dir.keep();
            pyo3::Python::attach(|py| {
                open_client(py, path.to_string_lossy().into_owned())
                    .expect("open_client should succeed");
            });
        });
    }

    #[test]
    fn room_derived_ids_never_collide_across_rooms() {
        ensure_open();
        let a = auth_chain_closure_room_id("ns-collision", "!roomA:example.org");
        let b = auth_chain_closure_room_id("ns-collision", "!roomB:example.org");
        let c = auth_chain_closure_room_id("other-ns", "!roomA:example.org");
        // Distinct rooms produce distinct member collection IDs (probabilistic domain separation).
        assert_ne!(a, b);
        // The authoritative entity canonical ID is the room ID itself:
        assert_eq!(a, c);
        // Member collections within the same room are domain-separated by their tags:
        let ev = event_dag_room_id("ns-collision", "!roomA:example.org");
        let prev = prev_edges_room_id("ns-collision", "!roomA:example.org");
        let stat = state_hamt_room_id("!roomA:example.org");
        assert_ne!(a, ev);
        assert_ne!(a, prev);
        assert_ne!(a, stat);
        assert_ne!(ev, prev);
        assert_ne!(ev, stat);
        assert_ne!(prev, stat);
    }

    #[test]
    fn get_or_create_short_ids_is_stable_and_room_scoped() {
        ensure_open();
        let ns = "ns-shortid";
        let room_a = "!room-a:example.org";
        let room_b = "!room-b:example.org";

        let first = get_or_create_short_ids(
            ns.to_string(),
            room_a.to_string(),
            vec!["$e1".to_string(), "$e2".to_string()],
        )
        .expect("alloc");
        assert_eq!(first.len(), 2);
        assert_ne!(first[0], first[1], "distinct events get distinct short ids");

        // Same event ids, same room -> identical short ids (idempotent).
        let second = get_or_create_short_ids(
            ns.to_string(),
            room_a.to_string(),
            vec!["$e1".to_string(), "$e2".to_string()],
        )
        .expect("re-fetch");
        assert_eq!(first, second);

        // Same event id string, different room -> unrelated short id
        // space (no cross-room collision guarantee implied by equal
        // values, but the rooms must not share the same underlying
        // collection).
        let other_room =
            get_or_create_short_ids(ns.to_string(), room_b.to_string(), vec!["$e1".to_string()])
                .expect("alloc in other room");
        // Reverse lookup in room_a must not resolve room_b's mapping and
        // vice versa.
        let resolved_in_a = pyo3::Python::attach(|py| {
            resolve_short_ids_to_event_ids(
                py,
                ns.to_string(),
                room_a.to_string(),
                vec![other_room[0]],
            )
        });
        // (room_a likely never allocated this exact short id to "$e1";
        // this call must not panic and must return a well-formed result
        // either way.)
        assert!(resolved_in_a.is_ok());
    }

    #[test]
    fn get_or_create_short_ids_reverse_mapping_stays_resolvable_across_repeat_calls() {
        // The trait has no single-key delete, so a genuine crash-between-
        // writes can't be injected from a test at this level; what's
        // testable here is the invariant get_or_create_short_ids must
        // uphold regardless: calling it again for an event whose forward
        // mapping already exists must never leave the reverse mapping
        // unresolvable.
        ensure_open();
        let ns = "ns-repair";
        let room = "!room-repair:example.org";

        let ids =
            get_or_create_short_ids(ns.to_string(), room.to_string(), vec!["$e1".to_string()])
                .expect("alloc");
        let short_id = ids[0];

        let _ = get_or_create_short_ids(ns.to_string(), room.to_string(), vec!["$e1".to_string()])
            .expect("re-fetch (forward already exists)");
        let resolved = pyo3::Python::attach(|py| {
            resolve_short_ids_to_event_ids(py, ns.to_string(), room.to_string(), vec![short_id])
                .expect("resolve")
        });
        assert_eq!(resolved, vec![Some("$e1".to_string())]);
    }

    #[test]
    fn auth_chain_edges_round_trip_including_zero_count_leaf() {
        ensure_open();
        let ns = "ns-edges";
        let room = "!room-edges:example.org";
        let ids = get_or_create_short_ids(
            ns.to_string(),
            room.to_string(),
            vec!["$leaf".to_string(), "$a1".to_string(), "$a2".to_string()],
        )
        .expect("alloc");
        let (leaf, a1, a2) = (ids[0], ids[1], ids[2]);

        pyo3::Python::attach(|py| {
            auth_chain_edges_put(
                py,
                ns.to_string(),
                room.to_string(),
                vec![(leaf, vec![]), (a1, vec![a2])],
            )
            .expect("put edges");

            let fetched =
                auth_chain_edges_get(py, ns.to_string(), room.to_string(), vec![leaf, a1, a2])
                    .expect("get edges");

            // Leaf: present, zero edges -- distinguishable from "missing".
            assert_eq!(fetched[0], Some(vec![]));
            // a1: present, one edge.
            assert_eq!(fetched[1], Some(vec![a2]));
            // a2: never written -- missing, the cold-import signal.
            assert_eq!(fetched[2], None);
        });
    }

    #[test]
    fn auth_chain_children_append_dedupes_and_is_idempotent() {
        ensure_open();
        let ns = "ns-children";
        let room = "!room-children:example.org";
        let ids = get_or_create_short_ids(
            ns.to_string(),
            room.to_string(),
            vec!["$parent".to_string(), "$c1".to_string(), "$c2".to_string()],
        )
        .expect("alloc");
        let (parent, c1, c2) = (ids[0], ids[1], ids[2]);

        // No children recorded yet.
        let none_yet = pyo3::Python::attach(|py| {
            auth_chain_children_get(py, ns.to_string(), room.to_string(), vec![parent])
        })
        .expect("get children (empty)");
        assert_eq!(none_yet, vec![None]);

        auth_chain_children_append(ns.to_string(), room.to_string(), vec![(parent, vec![c1])])
            .expect("append c1");
        // Appending c1 again, plus a new child c2, must dedupe c1 and add c2
        // exactly once -- both idempotency and accumulation in one append.
        auth_chain_children_append(
            ns.to_string(),
            room.to_string(),
            vec![(parent, vec![c1, c2])],
        )
        .expect("append c1 (dup) + c2");

        let children = pyo3::Python::attach(|py| {
            auth_chain_children_get(py, ns.to_string(), room.to_string(), vec![parent])
                .expect("get children")
        });
        let mut got = children[0].clone().expect("children present");
        got.sort_unstable();
        let mut want = vec![c1, c2];
        want.sort_unstable();
        assert_eq!(got, want);
    }

    #[test]
    fn auth_chain_children_append_same_parent_twice_in_one_call() {
        // Two rows for the same parent in a single call must both survive:
        // the append batches its puts, so without per-parent aggregation the
        // second row would read stale (un-put) children and its later put for
        // the same key would clobber the first row's.
        ensure_open();
        let ns = "ns-children-twice";
        let room = "!room-children-twice:example.org";
        let ids = get_or_create_short_ids(
            ns.to_string(),
            room.to_string(),
            vec!["$parent".to_string(), "$c1".to_string(), "$c2".to_string()],
        )
        .expect("alloc");
        let (parent, c1, c2) = (ids[0], ids[1], ids[2]);

        auth_chain_children_append(
            ns.to_string(),
            room.to_string(),
            vec![
                (parent, vec![c1]),
                (parent, vec![c2]),
                (parent, vec![c1]), // dup across rows, must dedupe
            ],
        )
        .expect("append two rows for one parent");

        let children = pyo3::Python::attach(|py| {
            auth_chain_children_get(py, ns.to_string(), room.to_string(), vec![parent])
                .expect("get children")
        });
        let mut got = children[0].clone().expect("children present");
        got.sort_unstable();
        let mut want = vec![c1, c2];
        want.sort_unstable();
        assert_eq!(got, want);
    }

    #[test]
    fn auth_chain_purge_room_removes_only_that_room() {
        ensure_open();
        let ns = "ns-purge";
        let room_keep = "!room-keep:example.org";
        let room_gone = "!room-gone:example.org";

        let keep_ids = get_or_create_short_ids(
            ns.to_string(),
            room_keep.to_string(),
            vec!["$k1".to_string()],
        )
        .expect("alloc keep");
        let gone_ids = get_or_create_short_ids(
            ns.to_string(),
            room_gone.to_string(),
            vec!["$g1".to_string()],
        )
        .expect("alloc gone");

        pyo3::Python::attach(|py| {
            auth_chain_purge_room(py, ns.to_string(), room_gone.to_string()).expect("purge");

            let still_resolves = resolve_short_ids_to_event_ids(
                py,
                ns.to_string(),
                room_keep.to_string(),
                vec![keep_ids[0]],
            )
            .expect("resolve keep");
            assert_eq!(still_resolves, vec![Some("$k1".to_string())]);

            let purged_resolves = resolve_short_ids_to_event_ids(
                py,
                ns.to_string(),
                room_gone.to_string(),
                vec![gone_ids[0]],
            )
            .expect("resolve gone (post-purge)");
            assert_eq!(purged_resolves, vec![None]);
        });
    }
}

#[cfg(test)]
mod event_json_mirror_tests {
    //! Same isolation contract as `auth_chain_closure_tests`: `DBS` is
    //! process-global, so engine-backed tests use their own unique
    //! namespace/room ids.

    use super::*;

    /// Raw engine view of a node: non-empty value stored?
    fn node_present(collection: &[u8; 16], node: &NodeId) -> bool {
        let engine = event_dag_db().expect("db");
        matches!(engine.get(collection, node), Ok(Some(data)) if !data.bytes.is_empty())
    }

    #[test]
    fn event_node_id_is_16_bytes_and_domain_separated() {
        let a = event_node_id("ns-ev", "$e1");
        let b = event_node_id("ns-ev", "$e2");
        let c = event_node_id("other-ns", "$e1");
        assert_eq!(a.len(), 16);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(event_node_id("ns-ev", "$e1"), a, "deterministic");
    }

    #[test]
    fn event_dag_room_id_is_domain_separated_and_distinct() {
        let a = event_dag_room_id("ns-ev", "!ra:example.org");
        let b = event_dag_room_id("ns-ev", "!rb:example.org");
        let c = event_dag_room_id("other-ns", "!ra:example.org");
        // Distinct room canonical IDs produce distinct member collection IDs:
        assert_ne!(a, b);
        // Room ID is the authoritative entity canonical identity:
        assert_eq!(a, c);

        // Cross-domain tags within the same room entity produce distinct physical collection IDs:
        let prev = prev_edges_room_id("ns-ev", "!ra:example.org");
        let auth = auth_chain_closure_room_id("ns-ev", "!ra:example.org");
        let stat = state_hamt_room_id("!ra:example.org");
        assert_ne!(a, prev);
        assert_ne!(a, auth);
        assert_ne!(a, stat);
        assert_ne!(prev, auth);
    }

    #[test]
    fn group_and_member_derivations_are_deterministic() {
        let room = "!canonical-room:example.org";
        let group_id_1 = group_full_logical_id(room.as_bytes());
        let group_id_2 = group_full_logical_id(room.as_bytes());
        assert_eq!(group_id_1, group_id_2);

        let evnt_col = member_collection_id(*b"EVNT", &group_id_1);
        let prev_col = member_collection_id(*b"PREV", &group_id_1);
        let auth_col = member_collection_id(*b"AUTH", &group_id_1);
        let stat_col = member_collection_id(*b"STAT", &group_id_1);

        assert_ne!(evnt_col, prev_col);
        assert_ne!(evnt_col, auth_col);
        assert_ne!(evnt_col, stat_col);
        assert_ne!(prev_col, auth_col);
        assert_ne!(prev_col, stat_col);
        assert_ne!(auth_col, stat_col);

        // Verification against constructor helpers:
        assert_eq!(evnt_col, event_dag_room_id("", room));
        assert_eq!(prev_col, prev_edges_room_id("", room));
        assert_eq!(auth_col, auth_chain_closure_room_id("", room));
        assert_eq!(stat_col, state_hamt_room_id(room));
    }

    #[test]
    fn locator_collections_spread_and_are_deterministic() {
        let ns = "ns-ev-loc";
        let mut seen: HashSet<[u8; 16]> = HashSet::new();
        for i in 0..512u32 {
            let node = event_node_id(ns, &format!("$ev-{i}"));
            seen.insert(event_locator_collection_id(ns, &node));
        }
        assert!(
            seen.len() >= 8,
            "512 ids must spread across many of the {} locator buckets",
            EVENT_LOCATOR_BUCKETS
        );
        for i in 0..256u32 {
            let key = format!("$ev-{i}");
            let node = event_node_id(ns, &key);
            let again = event_locator_collection_id(ns, &event_node_id(ns, &key));
            assert_eq!(
                again,
                event_locator_collection_id(ns, &node),
                "deterministic"
            );
        }
    }

    #[test]
    fn put_get_cross_bucket_delete_and_purge_round_trip() {
        super::auth_chain_closure_tests::ensure_open();
        let ns = "ns-ev-roundtrip";
        let room = "!room-ev-roundtrip:example.org";
        let meta_a: Vec<u8> = b"META-A".to_vec();
        let body_a: Vec<u8> = b"BODY-A".to_vec();
        let meta_b: Vec<u8> = b"META-B".to_vec();
        let body_b: Vec<u8> = b"BODY-B".to_vec();
        let id_a = event_node_id(ns, "$ev-a");
        let id_b = event_node_id(ns, "$ev-b");
        assert_ne!(
            event_locator_collection_id(ns, &id_a),
            event_locator_collection_id(ns, &id_b),
            "the two ids must land in different locator buckets"
        );
        let dag = event_dag_room_id(ns, room);

        pyo3::Python::attach(|py| {
            event_json_put(
                py,
                ns.to_string(),
                vec![
                    (
                        room.to_string(),
                        "$ev-a".to_string(),
                        meta_a.clone(),
                        body_a.clone(),
                    ),
                    (
                        room.to_string(),
                        "$ev-b".to_string(),
                        meta_b.clone(),
                        body_b.clone(),
                    ),
                ],
            )
            .expect("put");

            let got = event_json_get(
                py,
                ns.to_string(),
                vec![
                    "$ev-a".to_string(),
                    "$ev-b".to_string(),
                    "$ev-none".to_string(),
                ],
            )
            .expect("get");
            assert_eq!(got[0].1.as_deref(), Some(&meta_a[..]));
            assert_eq!(got[0].2.as_deref(), Some(&body_a[..]));
            assert_eq!(got[1].1.as_deref(), Some(&meta_b[..]));
            assert_eq!(got[1].2.as_deref(), Some(&body_b[..]));
            assert_eq!(got[2].1, None);
            assert_eq!(got[2].2, None);

            // Point deletion removes the payload (both records) AND the locator.
            event_json_delete(py, ns.to_string(), vec!["$ev-a".to_string()]).expect("delete");
            let after_delete = event_json_get(
                py,
                ns.to_string(),
                vec!["$ev-a".to_string(), "$ev-b".to_string()],
            )
            .expect("get after delete");
            assert_eq!(after_delete[0].1, None);
            assert_eq!(after_delete[0].2, None);
            assert_eq!(after_delete[1].1.as_deref(), Some(&meta_b[..]));
            assert_eq!(after_delete[1].2.as_deref(), Some(&body_b[..]));
            assert!(
                !node_present(&event_locator_collection_id(ns, &id_a), &id_a),
                "locator must be tombstoned too"
            );
            assert!(
                !node_present(&dag, &event_node_id(ns, "$ev-a")),
                "room-local body must be tombstoned too"
            );
            assert!(
                !node_present(&dag, &event_meta_node_id(ns, "$ev-a")),
                "room-local metadata must be tombstoned too"
            );

            event_json_purge_room(py, ns.to_string(), room.to_string()).expect("purge room");
            let after_purge = event_json_get(py, ns.to_string(), vec!["$ev-b".to_string()])
                .expect("get after purge");
            assert_eq!(after_purge[0].1, None);
            assert_eq!(after_purge[0].2, None);
        });
    }

    #[test]
    fn overwrite_replaces_body_in_place() {
        super::auth_chain_closure_tests::ensure_open();
        let ns = "ns-ev-overwrite";
        let room = "!room-ev-overwrite:example.org";
        pyo3::Python::attach(|py| {
            event_json_put(
                py,
                ns.to_string(),
                vec![(
                    room.to_string(),
                    "$e1".to_string(),
                    b"META-1".to_vec(),
                    b"BODY-1".to_vec(),
                )],
            )
            .expect("put");
            // Censoring/expiry/re-signing replace the body in place through
            // the same call: one record per event, latest wins.
            event_json_put(
                py,
                ns.to_string(),
                vec![(
                    room.to_string(),
                    "$e1".to_string(),
                    b"META-2".to_vec(),
                    b"BODY-2".to_vec(),
                )],
            )
            .expect("overwrite");

            let got = event_json_get(py, ns.to_string(), vec!["$e1".to_string()]).expect("get");
            assert_eq!(got[0].1.as_deref(), Some(&b"META-2"[..]));
            assert_eq!(got[0].2.as_deref(), Some(&b"BODY-2"[..]));
            let dag = event_dag_room_id(ns, room);
            assert!(
                node_present(&dag, &event_node_id(ns, "$e1")),
                "a single body record"
            );
            assert!(
                node_present(&dag, &event_meta_node_id(ns, "$e1")),
                "a single metadata record"
            );
        });
    }

    #[test]
    fn room_purge_is_isolated_and_stale_locator_misses() {
        super::auth_chain_closure_tests::ensure_open();
        let ns = "ns-ev-isolation";
        let room_gone = "!room-gone:example.org";
        let room_keep = "!room-keep:example.org";
        let id_gone = event_node_id(ns, "$gone");
        let dag_gone = event_dag_room_id(ns, room_gone);
        let dag_keep = event_dag_room_id(ns, room_keep);
        let locator_gone = event_locator_collection_id(ns, &id_gone);

        pyo3::Python::attach(|py| {
            event_json_put(
                py,
                ns.to_string(),
                vec![(
                    room_gone.to_string(),
                    "$gone".to_string(),
                    b"META-GONE".to_vec(),
                    b"BODY-GONE".to_vec(),
                )],
            )
            .expect("put gone");
            event_json_put(
                py,
                ns.to_string(),
                vec![(
                    room_keep.to_string(),
                    "$keep".to_string(),
                    b"META-KEEP".to_vec(),
                    b"BODY-KEEP".to_vec(),
                )],
            )
            .expect("put keep");

            event_json_purge_room(py, ns.to_string(), room_gone.to_string()).expect("purge");

            // The surviving room is untouched.
            let keep =
                event_json_get(py, ns.to_string(), vec!["$keep".to_string()]).expect("get keep");
            assert_eq!(keep[0].1.as_deref(), Some(&b"META-KEEP"[..]));
            assert_eq!(keep[0].2.as_deref(), Some(&b"BODY-KEEP"[..]));

            // The purged room misses even though its locator still points at
            // the now-empty collection -- a stale locator is a miss, not an
            // error, and the caller's SQL fallback takes over.
            assert!(
                node_present(&locator_gone, &id_gone),
                "purge alone leaves the locator (Python removes it with point deletes)"
            );
            assert!(dag_gone != dag_keep);
            let gone =
                event_json_get(py, ns.to_string(), vec!["$gone".to_string()]).expect("get gone");
            assert_eq!(gone[0].1, None);
            assert_eq!(gone[0].2, None);
        });
    }

    #[test]
    fn get_with_no_locator_is_a_plain_miss() {
        super::auth_chain_closure_tests::ensure_open();
        let ns = "ns-ev-no-locator";
        // No writes at all for this id: no legacy fallback exists any more,
        // so an id with no locator entry is just (None, None).
        pyo3::Python::attach(|py| {
            let got = event_json_get(py, ns.to_string(), vec!["$never-written".to_string()])
                .expect("get");
            assert_eq!(got[0].1, None);
            assert_eq!(got[0].2, None);
        });
    }
}
