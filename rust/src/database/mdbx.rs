//! Embedded, single-process (but natively multi-process-mmap-safe) HAMT
//! node/root storage backed by [`libmdbx`] (a Rust wrapper over the real C
//! libmdbx library). Every worker process can open this database directly
//! -- mdbx supports concurrent multi-process readers/writer via mmap and
//! its own file locking, so no bridge daemon is needed as long as all
//! processes share a filesystem (see the module-level architecture doc for
//! the single-host assumption). This was the deciding advantage over
//! fjall (a pure-Rust LSM engine also benchmarked here, since dropped --
//! see `database/mod.rs`'s doc comment): fjall's single-writer-process
//! design would have needed a worker RPC bridge; mdbx needs none.
//!
//! The BFS materialize/selective-lookup walk and key encoding live in
//! [`crate::database::core`]; this module only implements
//! [`core::NodeStore`] over an mdbx read transaction.

use std::fs;
use std::sync::Mutex;

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, Table, Transaction, WriteFlags,
    RO,
};
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use rezzy::hamt::StructuralHash;

use crate::database::core::{self, NodeCache, NodeStore, StateEntries, ROOM_PREFIX_LEN};
use crate::state_hamt::room_structural_key_raw;

static DB: OnceCell<Database<NoWriteMap>> = OnceCell::new();
// libmdbx tables are borrowed from a txn's lifetime in this crate version;
// the simplest correct approach is to open a table handle per-txn via
// `begin_*_txn().open_table(None)` on the default unnamed table, rather
// than caching a `Table<'static>`.
static WRITE_LOCK: Mutex<()> = Mutex::new(());

static NODE_CACHE: OnceCell<NodeCache> = OnceCell::new();

fn node_cache() -> &'static NodeCache {
    NODE_CACHE.get_or_init(core::new_node_cache)
}

fn db() -> PyResult<&'static Database<NoWriteMap>> {
    DB.get()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mdbx not opened"))
}

/// Same as `db()`, but with a plain `String` error instead of `PyErr` --
/// for the `_sync` helper functions that don't touch a `Python<'_>` token
/// at all (so their tests can run without an initialized interpreter).
fn db_sync() -> Result<&'static Database<NoWriteMap>, String> {
    DB.get().ok_or_else(|| "mdbx not opened".to_owned())
}

fn map_mdbx_err(e: impl ToString) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
}

/// Adapter implementing the generic [`core::NodeStore`] surface over one
/// mdbx read transaction + table, for the shared BFS walk in
/// `database::core`. A single read txn is held for the whole walk, which
/// gives it a consistent mmap snapshot (mdbx's usual MVCC read-view).
struct MdbxStore<'a> {
    txn: &'a Transaction<'a, RO, NoWriteMap>,
    table: &'a Table<'a>,
}

impl NodeStore for MdbxStore<'_> {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.txn.get(self.table, key).map_err(|e| e.to_string())
    }
}

fn open_client_sync(path: &str) -> Result<(), String> {
    if DB.get().is_some() {
        return Ok(());
    }
    // mdbx opens (or creates) the data files inside `path`, but doesn't
    // create `path` itself -- a config pointing at a not-yet-existing
    // directory (the common case for a fresh deployment or a default like
    // `/data/embedded_hamt`) would otherwise fail here instead of just
    // working, crashing the whole server at startup on a plain ENOENT.
    fs::create_dir_all(path)
        .map_err(|e| format!("failed to create embedded mdbx directory {path:?}: {e}"))?;
    let opts = DatabaseOptions {
        // Big enough ceiling for a real HAMT corpus; mdbx grows the mmap
        // lazily so this isn't pre-allocated disk usage.
        mode: Mode::ReadWrite(ReadWriteOptions {
            max_size: Some(64isize * 1024 * 1024 * 1024),
            ..Default::default()
        }),
        ..Default::default()
    };
    let database =
        Database::<NoWriteMap>::open_with_options(path, opts).map_err(|e| e.to_string())?;
    let _ = DB.set(database);
    Ok(())
}

/// Opens (or creates) the mdbx database at `path`. Safe to call from every
/// worker process concurrently, unlike `fjall::open_client` -- mdbx's mmap
/// + file locking supports multiple processes attaching to the same
/// database directory, one writer at a time, many concurrent readers.
#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| open_client_sync(&path)).map_err(map_mdbx_err)
}

#[pyfunction]
pub fn put(py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
    batch_put(py, vec![(key, value)])
}

#[pyfunction]
pub fn get(py: Python<'_>, key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
    py.detach(|| {
        let database = db()?;
        let txn = database.begin_ro_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        txn.get(&table, &key).map_err(map_mdbx_err)
    })
}

#[pyfunction]
pub fn batch_get(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    py.detach(|| {
        let database = db()?;
        let txn = database.begin_ro_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        let mut out = Vec::with_capacity(keys.len());
        for k in &keys {
            let v: Option<Vec<u8>> = txn.get(&table, k).map_err(map_mdbx_err)?;
            if let Some(v) = v {
                out.push((k.clone(), v));
            }
        }
        Ok(out)
    })
}

#[pyfunction]
pub fn batch_put(py: Python<'_>, mut pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    py.detach(|| {
        // Our keys are content-addressed (structural hashes / event ids),
        // so a batch arrives in essentially random order. mdbx's B-tree
        // insert cost is dominated by how much the cursor has to jump
        // around the tree; sorting first turns that into a mostly-local,
        // mostly-sequential walk (each insert near the last one), which is
        // the standard mdbx/LMDB bulk-load optimization. This is safe
        // regardless of what's already in the table (UPSERT still handles
        // pre-existing keys correctly) -- unlike `WriteFlags::APPEND`,
        // which would additionally require every key in this batch to sort
        // above every key already in the table, a guarantee we don't have
        // for a re-used/non-empty database, so APPEND isn't used here.
        pairs.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        let _guard = WRITE_LOCK.lock().unwrap();
        let database = db()?;
        let txn = database.begin_rw_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        for (k, v) in &pairs {
            txn.put(&table, k, v, WriteFlags::UPSERT)
                .map_err(map_mdbx_err)?;
        }
        txn.commit().map_err(map_mdbx_err)?;
        Ok(())
    })
}

/// Atomically applies `delta` to each key's big-endian i64 counter value (a
/// missing key starts from 0), all within one held write transaction, and
/// returns the resulting values in the same order as `pairs`. Unlike
/// `batch_put`, a plain read-then-write from Python would race a concurrent
/// caller incrementing the same key between the read and the write and lose
/// an update -- doing the read-modify-write here, inside the single rw_txn
/// this module already serializes via `WRITE_LOCK`, is what makes this safe
/// against that.
#[pyfunction]
pub fn increment_counters_batch(py: Python<'_>, pairs: Vec<(Vec<u8>, i64)>) -> PyResult<Vec<i64>> {
    py.detach(|| {
        let _guard = WRITE_LOCK.lock().unwrap();
        let database = db()?;
        let txn = database.begin_rw_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        let mut results = Vec::with_capacity(pairs.len());
        for (key, delta) in &pairs {
            let existing: Option<Vec<u8>> = txn.get(&table, key).map_err(map_mdbx_err)?;
            let current = match existing {
                Some(bytes) => {
                    let arr: [u8; 8] = bytes.as_slice().try_into().map_err(|_| {
                        pyo3::exceptions::PyRuntimeError::new_err(
                            "invalid counter record (expected 8 bytes)",
                        )
                    })?;
                    i64::from_be_bytes(arr)
                }
                None => 0,
            };
            let new_value = current + delta;
            txn.put(&table, key, new_value.to_be_bytes(), WriteFlags::UPSERT)
                .map_err(map_mdbx_err)?;
            results.push(new_value);
        }
        txn.commit().map_err(map_mdbx_err)?;
        Ok(results)
    })
}

/// Same as batch_put -- there is no separate optimistic-retry path needed
/// here (unlike the old TiKV engine): mdbx's single-writer-per-txn commit
/// already serializes concurrent writers at the mmap/file-lock level.
#[pyfunction]
pub fn transactional_batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    batch_put(py, pairs)
}

#[pyfunction]
pub fn delete(py: Python<'_>, key: Vec<u8>) -> PyResult<()> {
    batch_delete(py, vec![key])
}

#[pyfunction]
pub fn batch_delete(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<()> {
    py.detach(|| {
        let _guard = WRITE_LOCK.lock().unwrap();
        let database = db()?;
        let txn = database.begin_rw_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        for k in &keys {
            txn.del(&table, k, None).map_err(map_mdbx_err)?;
        }
        txn.commit().map_err(map_mdbx_err)?;
        Ok(())
    })
}

/// Scans keys starting with `prefix`, in key order, up to `limit` entries.
///
/// `limit` is not a hint -- a prefix can cover far more rows than any
/// caller wants in one FFI round trip (e.g. an auth chain's outgoing
/// links), so this always stops at `limit` even mid-prefix. That means a
/// caller MUST be able to tell "there may be more" from "this was
/// everything": when exactly `limit` rows come back, the last key in the
/// result is returned as `Some(...)` alongside the rows so Python can
/// pass it back in as `after` (an exclusive lower bound: the scan resumes
/// strictly after this key, not from it again) to page through the rest,
/// and get `None` once a page comes back short of `limit`, meaning the
/// prefix is exhausted. Silently treating a full page as "that's all of
/// them" would truncate the result with no error -- for auth chain links
/// that means an incomplete chain and a wrong auth-difference computation
/// with nothing to indicate the mistake, so this is deliberately not
/// optional. The signal is the returned cursor, not the page length: a
/// page can legitimately come back with exactly `limit` rows AND `None`
/// (the prefix had exactly that many left) -- check the second return
/// value, not `len(rows) == limit`.
type ScanPrefixResult = (Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>);

#[pyfunction]
pub fn scan_prefix(
    py: Python<'_>,
    prefix: Vec<u8>,
    limit: u32,
    after: Option<Vec<u8>>,
) -> PyResult<ScanPrefixResult> {
    py.detach(|| {
        let database = db()?;
        let txn = database.begin_ro_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        let mut cursor = txn.cursor(&table).map_err(map_mdbx_err)?;
        let mut results = Vec::new();
        let start = after.as_ref().unwrap_or(&prefix);
        let iter = cursor.iter_from::<Vec<u8>, Vec<u8>>(start);
        for entry in iter {
            let (k, v) = entry.map_err(map_mdbx_err)?;
            // `iter_from` is inclusive of `start`; when resuming after a
            // previous page, skip the boundary key itself so it isn't
            // returned twice.
            if let Some(after_key) = &after {
                if &k == after_key {
                    continue;
                }
            }
            if !k.starts_with(&prefix) {
                break;
            }
            if results.len() >= limit as usize {
                let last_key = results.last().map(|(k, _): &(Vec<u8>, Vec<u8>)| k.clone());
                return Ok((results, last_key));
            }
            results.push((k, v));
        }
        Ok((results, None))
    })
}

/// Writes auth chain link edges: `(origin_chain_id, origin_sequence_number,
/// target_chain_id, target_sequence_number)` per edge, value always empty.
/// No read-modify-write (unlike the state_group refcount) -- two writers
/// adding different edges for the same origin chain never touch the same
/// key, so this is a plain batch insert. Key encoding lives in
/// `database::core` so it's shared with the read/delete paths below and
/// stays byte-for-byte in sync with them by construction (rather than by
/// convention with a hand-duplicated Python encoder).
/// Pure logic behind `put_auth_chain_links_batch`, without a `Python<'_>`
/// token -- shared by the pyfunction (via `py.detach`) and this module's
/// tests, which run without an initialized Python interpreter.
fn put_auth_chain_links_sync(
    namespace: &str,
    links: &[(i64, i64, i64, i64)],
) -> Result<(), String> {
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = links
        .iter()
        .map(
            |&(
                origin_chain_id,
                origin_sequence_number,
                target_chain_id,
                target_sequence_number,
            )| {
                (
                    core::auth_chain_link_key(
                        namespace,
                        origin_chain_id,
                        origin_sequence_number,
                        target_chain_id,
                        target_sequence_number,
                    ),
                    Vec::new(),
                )
            },
        )
        .collect();
    let _guard = WRITE_LOCK.lock().unwrap();
    let database = db_sync()?;
    let txn = database.begin_rw_txn().map_err(|e| e.to_string())?;
    let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
    for (k, v) in &pairs {
        txn.put(&table, k, v, WriteFlags::UPSERT)
            .map_err(|e| e.to_string())?;
    }
    txn.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Writes auth chain link edges: `(origin_chain_id, origin_sequence_number,
/// target_chain_id, target_sequence_number)` per edge, value always empty.
/// No read-modify-write (unlike the state_group refcount) -- two writers
/// adding different edges for the same origin chain never touch the same
/// key, so this is a plain batch insert. Key encoding lives in
/// `database::core` so it's shared with the read/delete paths below and
/// stays byte-for-byte in sync with them by construction (rather than by
/// convention with a hand-duplicated Python encoder).
#[pyfunction]
pub fn put_auth_chain_links_batch(
    py: Python<'_>,
    namespace: String,
    links: Vec<(i64, i64, i64, i64)>,
) -> PyResult<()> {
    py.detach(|| put_auth_chain_links_sync(&namespace, &links))
        .map_err(map_mdbx_err)
}

/// Pure logic behind `delete_auth_chain_links_batch`, without a
/// `Python<'_>` token -- see `put_auth_chain_links_sync`.
fn delete_auth_chain_links_sync(namespace: &str, pairs: &[(i64, i64)]) -> Result<(), String> {
    let _guard = WRITE_LOCK.lock().unwrap();
    let database = db_sync()?;
    let txn = database.begin_rw_txn().map_err(|e| e.to_string())?;
    let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
    for &(origin_chain_id, origin_sequence_number) in pairs {
        let prefix =
            core::auth_chain_origin_seq_prefix(namespace, origin_chain_id, origin_sequence_number);
        let mut cursor = txn.cursor(&table).map_err(|e| e.to_string())?;
        let iter = cursor.iter_from::<Vec<u8>, Vec<u8>>(&prefix);
        let mut keys_to_delete = Vec::new();
        for entry in iter {
            let (k, _v) = entry.map_err(|e| e.to_string())?;
            if !k.starts_with(&prefix) {
                break;
            }
            keys_to_delete.push(k);
        }
        drop(cursor);
        for k in keys_to_delete {
            txn.del(&table, &k, None).map_err(|e| e.to_string())?;
        }
    }
    txn.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Deletes every edge whose `(origin_chain_id, origin_sequence_number)`
/// matches one of `pairs` -- the embedded-engine equivalent of `DELETE
/// FROM event_auth_chain_links WHERE origin_chain_id = ? AND
/// origin_sequence_number = ?`. Each pair may cover several edges (one
/// origin can link to multiple targets), so this scans each pair's key
/// range for the matching keys before deleting them, all within one
/// held write transaction.
#[pyfunction]
pub fn delete_auth_chain_links_batch(
    py: Python<'_>,
    namespace: String,
    pairs: Vec<(i64, i64)>,
) -> PyResult<()> {
    py.detach(|| delete_auth_chain_links_sync(&namespace, &pairs))
        .map_err(map_mdbx_err)
}

/// `(origin_chain_id, edges)`, `edges` being `(origin_seq, target_chain_id,
/// target_seq)` triples.
type AuthChainLinksForChain = (i64, Vec<(i64, i64, i64)>);

/// Pure logic behind `get_auth_chain_links_batch`, without a `Python<'_>`
/// token -- see `put_auth_chain_links_sync`.
fn get_auth_chain_links_sync(
    namespace: &str,
    chain_ids: Vec<i64>,
) -> Result<Vec<AuthChainLinksForChain>, String> {
    let database = db_sync()?;
    let txn = database.begin_ro_txn().map_err(|e| e.to_string())?;
    let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;

    let mut visited: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut frontier: Vec<i64> = chain_ids;
    let mut links: Vec<AuthChainLinksForChain> = Vec::new();

    while let Some(chain_id) = frontier.pop() {
        if !visited.insert(chain_id) {
            continue;
        }

        let prefix = core::auth_chain_prefix(namespace, chain_id);
        let mut cursor = txn.cursor(&table).map_err(|e| e.to_string())?;
        let iter = cursor.iter_from::<Vec<u8>, Vec<u8>>(&prefix);
        let mut edges = Vec::new();
        for entry in iter {
            let (k, _v) = entry.map_err(|e| e.to_string())?;
            if !k.starts_with(&prefix) {
                break;
            }
            let decoded = core::decode_auth_chain_link_suffix(&k, &prefix)?;
            edges.push(decoded);
        }
        drop(cursor);

        if edges.is_empty() {
            continue;
        }
        for &(_origin_seq, target_chain_id, _target_seq) in &edges {
            if !visited.contains(&target_chain_id) {
                frontier.push(target_chain_id);
            }
        }
        links.push((chain_id, edges));
    }

    Ok(links)
}

/// Fetches every edge out of every chain transitively reachable from
/// `chain_ids` (following `target_chain_id`), the hot-path replacement for
/// `_get_chain_links`'s `WITH RECURSIVE` walk (see
/// `embedded_event_auth_chain_links.py`'s module docstring for why mdbx
/// needs a Python/Rust BFS instead of a recursive query at all). Done
/// entirely inside one read transaction/one FFI call -- state resolution
/// calls this on every conflict, so paying per-chain FFI round trips here
/// (as a naive `scan_prefix`-per-chain-from-Python loop would) is the kind
/// of overhead this project keeps out of its hot paths.
///
/// Returns `(origin_chain_id, edges)` pairs -- the same shape
/// `_get_chain_links` yields per batch in the SQL implementation, just
/// materialized rather than a generator (the BFS below already visits
/// every reachable chain in one pass, so there's no equivalent of SQL's
/// per-1000-chain batching to preserve -- the 1000-chain batching in the
/// Python caller only bounds how many *starting* chains one call covers,
/// which this still respects since `chain_ids` is that same batch).
#[pyfunction]
pub fn get_auth_chain_links_batch(
    py: Python<'_>,
    namespace: String,
    chain_ids: Vec<i64>,
) -> PyResult<Vec<AuthChainLinksForChain>> {
    py.detach(|| get_auth_chain_links_sync(&namespace, chain_ids))
        .map_err(map_mdbx_err)
}

type PyRootRecord = (i64, Vec<u8>, Vec<u8>, String, Vec<u8>);

/// Batched HAMT root lookup: one FFI call instead of an N-iteration Python
/// `for` loop each paying its own round trip. Returns one entry per input
/// group, `None` where this engine has no root record for it.
#[pyfunction]
#[pyo3(text_signature = "(namespace, groups, /)")]
pub fn batch_get_state_hamt_roots(
    py: Python<'_>,
    namespace: String,
    groups: Vec<i64>,
) -> PyResult<Vec<Option<PyRootRecord>>> {
    py.detach(|| {
        let database = db().map_err(|e| e.to_string())?;
        let txn = database.begin_ro_txn().map_err(|e| e.to_string())?;
        let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        core::batch_get_state_hamt_roots(&store, &namespace, &groups).map(|records| {
            records
                .into_iter()
                .zip(groups)
                .map(|(record, group)| {
                    record.map(|r| {
                        (
                            group,
                            r.room_prefix,
                            r.root_hash.to_vec(),
                            r.room_id,
                            r.lattice,
                        )
                    })
                })
                .collect()
        })
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

/// Persists a batch of `(structural_hash, node_bytes)` pairs under their
/// namespaced, room-prefixed keys (`core::node_key`) -- the only correct
/// way to write nodes this engine's materialize/lookup walk can later
/// find; writing under a raw `structural_hash` key (as a naive `batch_put`
/// call would) is invisible to the BFS walk.
#[pyfunction]
#[pyo3(text_signature = "(namespace, room_prefix, nodes, /)")]
pub fn put_state_hamt_nodes(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<()> {
    let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "room_prefix must be {ROOM_PREFIX_LEN} bytes"
        ))
    })?;
    let nodes = nodes
        .into_iter()
        .map(|(hash, bytes)| {
            let hash: StructuralHash = hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("structural_hash must be 32 bytes")
            })?;
            Ok((hash, bytes))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let pairs = core::encode_node_writes(&namespace, &room_prefix, nodes);
    batch_put(py, pairs)
}

#[pyfunction]
#[pyo3(text_signature = "(namespace, room_prefix, root_structural_hash, room_id, /)")]
pub fn materialize_state_hamt(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    root_structural_hash: Vec<u8>,
    room_id: &str,
) -> PyResult<Option<StateEntries>> {
    let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "room_prefix must be {ROOM_PREFIX_LEN} bytes"
        ))
    })?;
    let root_structural_hash: StructuralHash = root_structural_hash.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
    })?;
    let structural_key = room_structural_key_raw(room_id);
    py.detach(|| {
        let database = db().map_err(|e| e.to_string())?;
        let txn = database.begin_ro_txn().map_err(|e| e.to_string())?;
        let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        core::materialize_state_hamt(
            &store,
            node_cache(),
            &namespace,
            &room_prefix,
            root_structural_hash,
            &structural_key,
        )
    })
    .map(Some)
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

#[pyfunction]
#[pyo3(text_signature = "(namespace, roots, /)")]
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
                    "room_prefix must be {ROOM_PREFIX_LEN} bytes"
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
        let database = db().map_err(|e| e.to_string())?;
        let txn = database.begin_ro_txn().map_err(|e| e.to_string())?;
        let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        core::materialize_state_hamts(&store, node_cache(), &namespace, roots)
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

type PySelectiveQuery = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<(String, String)>);

#[pyfunction]
#[pyo3(text_signature = "(namespace, queries, /)")]
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
                    "room_prefix must be {ROOM_PREFIX_LEN} bytes"
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
        let database = db().map_err(|e| e.to_string())?;
        let txn = database.begin_ro_txn().map_err(|e| e.to_string())?;
        let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        core::lookup_state_hamts(&store, node_cache(), &namespace, parsed_queries)
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

pub fn register_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let child = PyModule::new(py, "mdbx_engine")?;
    child.add_function(wrap_pyfunction!(open_client, &child)?)?;
    child.add_function(wrap_pyfunction!(put, &child)?)?;
    child.add_function(wrap_pyfunction!(get, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_get, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_put, &child)?)?;
    child.add_function(wrap_pyfunction!(transactional_batch_put, &child)?)?;
    child.add_function(wrap_pyfunction!(delete, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_delete, &child)?)?;
    child.add_function(wrap_pyfunction!(increment_counters_batch, &child)?)?;
    child.add_function(wrap_pyfunction!(scan_prefix, &child)?)?;
    child.add_function(wrap_pyfunction!(put_state_hamt_nodes, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_get_state_hamt_roots, &child)?)?;
    child.add_function(wrap_pyfunction!(materialize_state_hamt, &child)?)?;
    child.add_function(wrap_pyfunction!(materialize_state_hamts, &child)?)?;
    child.add_function(wrap_pyfunction!(lookup_state_hamts, &child)?)?;
    child.add_function(wrap_pyfunction!(put_auth_chain_links_batch, &child)?)?;
    child.add_function(wrap_pyfunction!(delete_auth_chain_links_batch, &child)?)?;
    child.add_function(wrap_pyfunction!(get_auth_chain_links_batch, &child)?)?;
    parent.add_submodule(&child)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.mdbx_engine", &child)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::core::node_key;
    use crate::state_hamt::build_root_handle_and_nodes;

    fn ensure_open() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.keep();
            open_client_sync(path.to_str().unwrap()).expect("open mdbx database");
        });
    }

    #[test]
    fn put_get_roundtrip() {
        ensure_open();
        let database = DB.get().unwrap();
        let txn = database.begin_rw_txn().unwrap();
        let table: Table = txn.open_table(None).unwrap();
        txn.put(&table, b"mdbx:test:roundtrip", b"hello", WriteFlags::UPSERT)
            .unwrap();
        txn.commit().unwrap();

        let txn = database.begin_ro_txn().unwrap();
        let table: Table = txn.open_table(None).unwrap();
        let v: Option<Vec<u8>> = txn.get(&table, b"mdbx:test:roundtrip").unwrap();
        assert_eq!(v, Some(b"hello".to_vec()));
    }

    /// Exercises the same read-modify-write logic `increment_counters_batch`
    /// runs inside its held rw_txn, directly (bypassing the pyfunction
    /// wrapper, matching this module's other tests). Covers: starting from
    /// missing (0), repeated increments accumulating, and a negative delta
    /// (decrement) working the same way.
    #[test]
    fn counter_increment_accumulates_and_starts_from_zero() {
        ensure_open();
        let database = DB.get().unwrap();
        let key = b"mdbx:test:counter:accumulates";

        let apply = |delta: i64| -> i64 {
            let txn = database.begin_rw_txn().unwrap();
            let table: Table = txn.open_table(None).unwrap();
            let existing: Option<Vec<u8>> = txn.get(&table, key).unwrap();
            let current = existing
                .map(|bytes| i64::from_be_bytes(bytes.as_slice().try_into().unwrap()))
                .unwrap_or(0);
            let new_value = current + delta;
            txn.put(&table, key, new_value.to_be_bytes(), WriteFlags::UPSERT)
                .unwrap();
            txn.commit().unwrap();
            new_value
        };

        assert_eq!(apply(1), 1);
        assert_eq!(apply(1), 2);
        assert_eq!(apply(3), 5);
        assert_eq!(apply(-2), 3);
        assert_eq!(apply(-3), 0);
    }

    /// End-to-end: build a real HAMT via `build_root_handle_and_nodes`,
    /// persist its nodes exactly as `put_state_hamt_objects` would, then
    /// materialize it back out via the shared `core::materialize_state_hamt`
    /// walk and check the round trip.
    #[test]
    fn materialize_state_hamt_round_trips_through_mdbx() {
        ensure_open();
        let namespace = "test-namespace-materialize-mdbx";
        let room_id = "!room:example.org";
        let room_prefix: [u8; ROOM_PREFIX_LEN] = [1, 2, 3, 4, 5, 6, 7, 8];

        let entries = vec![
            (
                "m.room.create".to_owned(),
                "".to_owned(),
                "$create:example.org".to_owned(),
            ),
            (
                "m.room.member".to_owned(),
                "@alice:example.org".to_owned(),
                "$join:example.org".to_owned(),
            ),
        ];
        let ((root_hash, _state_group_id), nodes) =
            build_root_handle_and_nodes(room_id, entries.clone()).expect("build HAMT");

        let database = DB.get().unwrap();
        let txn = database.begin_rw_txn().unwrap();
        let table: Table = txn.open_table(None).unwrap();
        for (hash, bytes) in nodes {
            let hash: StructuralHash = hash;
            let key = node_key(namespace, &room_prefix, &hash);
            txn.put(&table, &key, &bytes, WriteFlags::UPSERT).unwrap();
        }
        txn.commit().unwrap();

        let root_hash: StructuralHash = root_hash;
        let structural_key = room_structural_key_raw(room_id);

        let txn = database.begin_ro_txn().unwrap();
        let table: Table = txn.open_table(None).unwrap();
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        let mut materialized = core::materialize_state_hamt(
            &store,
            node_cache(),
            namespace,
            &room_prefix,
            root_hash,
            &structural_key,
        )
        .expect("materialize");
        materialized.sort();

        let mut expected = entries;
        expected.sort();
        assert_eq!(materialized, expected);
    }

    /// Covers `get_auth_chain_links_batch`'s BFS: seeding edges
    /// 1->2->3 (3 has no outgoing edges) plus an unrelated 9->10 edge,
    /// starting the walk from just {1} should return chains 1 and 2 (2's
    /// edge discovered transitively) but not 3 (no outgoing edges of its
    /// own to report) or 9/10 (unreached from the start set).
    #[test]
    fn auth_chain_links_bfs_follows_target_chain_transitively() {
        ensure_open();
        let namespace = "auth-chain-bfs-test-namespace";

        put_auth_chain_links_sync(namespace, &[(1, 1, 2, 1), (2, 1, 3, 1), (9, 1, 10, 1)]).unwrap();

        let links = get_auth_chain_links_sync(namespace, vec![1]).unwrap();
        let mut by_chain: std::collections::HashMap<i64, Vec<(i64, i64, i64)>> =
            links.into_iter().collect();

        assert_eq!(by_chain.remove(&1), Some(vec![(1, 2, 1)]));
        assert_eq!(by_chain.remove(&2), Some(vec![(1, 3, 1)]));
        assert!(!by_chain.contains_key(&3));
        assert!(!by_chain.contains_key(&9));

        delete_auth_chain_links_sync(namespace, &[(1, 1)]).unwrap();
        let links_after_delete = get_auth_chain_links_sync(namespace, vec![1]).unwrap();
        // Chain 1's own edge is gone, but the walk can no longer reach
        // chain 2 from chain 1 either -- matches this module's delete
        // semantics (origin-only, no target-side cleanup, see
        // `delete_auth_chain_links_batch`'s doc comment).
        assert!(links_after_delete.is_empty());

        let links_from_2 = get_auth_chain_links_sync(namespace, vec![2]).unwrap();
        assert_eq!(links_from_2, vec![(2, vec![(1, 3, 1)])]);
    }
}
