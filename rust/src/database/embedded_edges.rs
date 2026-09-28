//! Embedded Event Edges storage in the `edges` mtxdb packfile storage pool.
//!
//! Stores the DAG edges connecting Matrix events:
//! - Backward edges: `event_id -> [(prev_event_id, is_state)]` (immutable, write-once).
//! - Forward edges: `prev_event_id -> [child_event_id]` (appended and
//!   deduplicated under a per-room lock; see `lock_rooms`).
//!
//! Locators are primarily owned by `event_json_put`, but `event_edges_put`
//! re-publishes the locator for every event a row touches (its own id and
//! every prev_event id).  That keeps reads self-sufficient: repaired/backfilled
//! edges for older events whose `event_json` locator was never mirrored can
//! still resolve to their room collection.  The overwrite is idempotent and
//! always carries the same value as `event_json_put`, and repeated locators
//! for the same event within one batch are deduplicated before the
//! `put_many` call.  Deletion deliberately does NOT tombstone locators here
//! -- `event_json_delete` owns that.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, OnceLock};

use mtxdb::{DatabaseTransaction, NodeData, NodeId, ShardType, StorageEngine};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sha2::{Digest, Sha256};

use super::mtxdb_syn::{
    assert_writable, auth_chain_db, begin_internal_transaction, event_locator_collection_id,
    event_node_id, map_read_storage_error, map_transaction_error, prev_edges_room_id,
};

/// The read/write surface `event_edges_put` needs, common to a direct engine
/// write and a staged transaction write, so the merge logic below (batch-read
/// existing forward lists, append, re-encode) is written once and used by
/// both.
///
/// Only `get_many`/`put_many` are needed: `event_edges_put` never deletes.
trait EdgeWriteTarget {
    fn edge_get_many(
        &self,
        collection: &[u8; 16],
        ids: &[NodeId],
    ) -> Result<Vec<Option<NodeData>>, mtxdb::storage::StorageError>;
    fn edge_put_many(
        &self,
        collection: [u8; 16],
        pairs: Vec<(NodeId, NodeData)>,
    ) -> Result<(), mtxdb::storage::StorageError>;
}

impl EdgeWriteTarget for mtxdb::PackfileStorage {
    fn edge_get_many(
        &self,
        collection: &[u8; 16],
        ids: &[NodeId],
    ) -> Result<Vec<Option<NodeData>>, mtxdb::storage::StorageError> {
        StorageEngine::get_many(self, collection, ids)
    }

    fn edge_put_many(
        &self,
        collection: [u8; 16],
        pairs: Vec<(NodeId, NodeData)>,
    ) -> Result<(), mtxdb::storage::StorageError> {
        StorageEngine::put_many(self, &collection, &pairs).map(|_| ())
    }
}

/// Stages into the edges pool instead of writing the live pool: every
/// `put_many` below becomes one `DatabaseTransaction::put` per record, and
/// nothing is visible or durable until the caller commits.
impl EdgeWriteTarget for DatabaseTransaction<'_> {
    fn edge_get_many(
        &self,
        collection: &[u8; 16],
        ids: &[NodeId],
    ) -> Result<Vec<Option<NodeData>>, mtxdb::storage::StorageError> {
        self.get(ShardType::Edges, collection, ids)
    }

    fn edge_put_many(
        &self,
        collection: [u8; 16],
        pairs: Vec<(NodeId, NodeData)>,
    ) -> Result<(), mtxdb::storage::StorageError> {
        for (node, data) in pairs {
            self.put(ShardType::Edges, collection, node, &data)
                .map_err(mtxdb::storage::StorageError::Io)?;
        }
        Ok(())
    }
}

/// Per-room locks replacing the process-wide `RMW_LOCK` for event-edge
/// writes: a `put`/`delete` batch only serializes against other batches that
/// touch the same rooms, not every room in the deployment.
///
/// Locks are never removed once created, so this map grows to one entry per
/// room the process has ever written edges for. That's an accepted tradeoff
/// at Matrix room-count scale (thousands), not an oversight -- it's also what
/// lets `room_lock` hand back a `&'static Mutex<()>` instead of an `Arc`, so
/// `lock_rooms` can return owned guards directly.
static ROOM_LOCKS: OnceLock<Mutex<HashMap<[u8; 16], &'static Mutex<()>>>> = OnceLock::new();

fn room_lock(room_collection: [u8; 16]) -> &'static Mutex<()> {
    let table = ROOM_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut table = table.lock().unwrap_or_else(|poison| poison.into_inner());
    table
        .entry(room_collection)
        .or_insert_with(|| Box::leak(Box::new(Mutex::new(()))))
}

/// Acquire per-room locks for every distinct room collection in
/// `room_collections`, in a stable order (sorted by room-collection bytes),
/// and return the held guards.
///
/// `put` and `delete` both go through this with the same sort key, so a put
/// touching rooms `{A, B}` and a delete touching `{B, A}` can never acquire
/// in opposite orders and deadlock.
///
/// `std::sync::Mutex` poisons on panic. A panic while a room's lock is held
/// (e.g. a decode error) would otherwise permanently wedge that one room, so
/// a poisoned guard is recovered here rather than propagated -- the caller's
/// own `Result` is what signals a failed batch, not lock poisoning.
fn lock_rooms(
    room_collections: impl IntoIterator<Item = [u8; 16]>,
) -> Vec<MutexGuard<'static, ()>> {
    let mut sorted: Vec<[u8; 16]> = room_collections.into_iter().collect();
    sorted.sort_unstable();
    sorted.dedup();
    sorted
        .into_iter()
        .map(room_lock)
        .map(|lock| lock.lock().unwrap_or_else(|poison| poison.into_inner()))
        .collect()
}

fn event_edges_backward_node_id(namespace: &str, event_id: &str) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(b"event_edges:backward:");
    hasher.update(namespace.as_bytes());
    hasher.update(b"\0");
    hasher.update(event_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn event_edges_forward_node_id(namespace: &str, prev_event_id: &str) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(b"event_edges:forward:");
    hasher.update(namespace.as_bytes());
    hasher.update(b"\0");
    hasher.update(prev_event_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn encode_backward_edges(edges: &[(String, bool)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(edges.len() as u16).to_be_bytes());
    for (prev_id, is_state) in edges {
        buf.push(if *is_state { 1 } else { 0 });
        let bytes = prev_id.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(bytes);
    }
    buf
}

fn decode_backward_edges(bytes: &[u8]) -> PyResult<Vec<(String, bool)>> {
    if bytes.len() < 2 {
        return Ok(Vec::new());
    }
    let count = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let mut offset = 2;
    let mut edges = Vec::with_capacity(count);
    for _ in 0..count {
        if offset >= bytes.len() {
            break;
        }
        let is_state = bytes[offset] != 0;
        offset += 1;
        if offset + 2 > bytes.len() {
            break;
        }
        let len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;
        if offset + len > bytes.len() {
            break;
        }
        let id_str = std::str::from_utf8(&bytes[offset..offset + len])
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("utf8 error: {e}")))?;
        edges.push((id_str.to_string(), is_state));
        offset += len;
    }
    Ok(edges)
}

fn encode_forward_edges(children: &[String]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(children.len() as u32).to_be_bytes());
    for child in children {
        let bytes = child.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(bytes);
    }
    buf
}

fn decode_forward_edges(bytes: &[u8]) -> PyResult<Vec<String>> {
    if bytes.len() < 4 {
        return Ok(Vec::new());
    }
    let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut offset = 4;
    let mut children = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 2 > bytes.len() {
            break;
        }
        let len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;
        if offset + len > bytes.len() {
            break;
        }
        let id_str = std::str::from_utf8(&bytes[offset..offset + len])
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("utf8 error: {e}")))?;
        children.push(id_str.to_string());
        offset += len;
    }
    Ok(children)
}

/// Publish an `event_id -> room collection` locator into a batch-local map.
///
/// The value (the owning room collection) is identical for every occurrence
/// of an event within a batch, so the nested map keyed by `NodeId`
/// deduplicates the repeated locators a batch produces -- an event appears
/// once as its own backward row and once as the parent of each of its
/// children -- avoiding redundant `put_many` entries.
fn insert_event_locator(
    locators: &mut HashMap<[u8; 16], HashMap<NodeId, NodeData>>,
    namespace: &str,
    room_id: &str,
    event_id: &str,
) {
    let room_collection = prev_edges_room_id(namespace, room_id);
    let identity = event_node_id(namespace, event_id);
    let locator_collection = event_locator_collection_id(namespace, &identity);
    locators.entry(locator_collection).or_default().insert(
        identity,
        NodeData::new(bytes::Bytes::copy_from_slice(&room_collection)),
    );
}

/// Batch put event edges into the room-aware PREV collection in the Edges pool:
/// 1. Backward edges: `event_id -> [(prev_event_id, is_state)]`
/// 2. Forward edges: `prev_event_id -> [child_event_id]` (appended and deduplicated)
///
/// Locators are ALSO written here: for every event a row mentions (its own
/// `event_id` and every `prev_event_id`), the `event_id -> room collection`
/// locator is published with the same value `event_json_put` writes.  This is
/// idempotent and exists so repaired/backfilled edges for legacy events --
/// whose `event_json` locator may never have been mirrored -- can still be
/// resolved by the read paths.  `event_json_put` remains the primary locator
/// writer for newly mirrored events; edge records land in (and reads resolve
/// through) the same room collection either way.

#[pyfunction]
pub fn event_edges_put(
    py: Python<'_>,
    namespace: String,
    rows: Vec<(String, String, String, bool)>, // (room_id, event_id, prev_event_id, is_state)
) -> PyResult<()> {
    assert_writable()?;
    // Rooms are known from the caller's own rows, before any lock is taken,
    // so lock ordering doesn't depend on anything read under the lock.
    let room_collections: Vec<[u8; 16]> = rows
        .iter()
        .map(|(room_id, _, _, _)| prev_edges_room_id(&namespace, room_id))
        .collect();
    py.detach(|| {
        // Held for the whole function, including the transaction's commit
        // below: releasing it between staging and commit would let another
        // writer's direct read/merge interleave against data this call has
        // already decided to overwrite (mtxdb has no RMW conflict detection
        // to catch that after the fact -- see `res/docs/2026-09-26-
        // priorities.md` §0.4). Keeping stage-through-commit inside one
        // held lock sidesteps needing that.
        let _guards = lock_rooms(room_collections);

        // Stage everything as one transaction when a shared WAL is open, so
        // the three put_many groups below (backward records, forward-list
        // rewrites, locators) publish as a single journal group instead of
        // three -- the same win `event_json_put`'s staging already has, see
        // `res/docs/2026-09-26-priorities.md` §1.4. Without a shared WAL
        // (`begin_internal_transaction` returns `None`) fall back to writing
        // the live pool directly, exactly as before.
        match begin_internal_transaction()? {
            Some(txn) => {
                write_edges(&txn, &namespace, rows)?;
                txn.commit().map_err(|e| map_transaction_error("commit", e))
            }
            None => {
                let engine: &mtxdb::PackfileStorage = auth_chain_db()?;
                write_edges(engine, &namespace, rows)
            }
        }
    })
}

/// The merge logic shared by a direct engine write and a staged transaction
/// write: batch-read existing forward child lists, append/deduplicate new
/// children, and write backward records, forward-list rewrites, and locators
/// through `target`. Caller holds the room locks for `rows`' rooms.
fn write_edges(
    target: &impl EdgeWriteTarget,
    namespace: &str,
    rows: Vec<(String, String, String, bool)>,
) -> PyResult<()> {
    let namespace = namespace.to_string();
    let engine = target;

    let mut backward_map: HashMap<(String, String), Vec<(String, bool)>> = HashMap::new();
    let mut forward_map: HashMap<(String, String), Vec<String>> = HashMap::new();

    for (room_id, event_id, prev_event_id, is_state) in rows {
        backward_map
            .entry((room_id.clone(), event_id.clone()))
            .or_default()
            .push((prev_event_id.clone(), is_state));
        forward_map
            .entry((room_id, prev_event_id))
            .or_default()
            .push(event_id);
    }

    let mut dag_puts: HashMap<[u8; 16], Vec<(NodeId, NodeData)>> = HashMap::new();
    // Nested map deduplicates repeated locators within the batch.
    let mut locator_puts: HashMap<[u8; 16], HashMap<NodeId, NodeData>> = HashMap::new();

    for ((room_id, event_id), edges) in backward_map {
        let room_collection = prev_edges_room_id(&namespace, &room_id);
        let edge_node = event_edges_backward_node_id(&namespace, &event_id);

        let encoded = encode_backward_edges(&edges);
        dag_puts
            .entry(room_collection)
            .or_default()
            .push((edge_node, NodeData::new(bytes::Bytes::from(encoded))));
        insert_event_locator(&mut locator_puts, &namespace, &room_id, &event_id);
    }

    // Batch-read the existing forward child lists for every distinct
    // (room, parent) this batch touches, instead of one `get` per parent.
    // `get_many` groups by shard and orders by offset, so a persist batch
    // touching many parents costs one round trip per room collection.
    let mut forward_by_collection: HashMap<[u8; 16], Vec<NodeId>> = HashMap::new();
    for (room_id, prev_event_id) in forward_map.keys() {
        let room_collection = prev_edges_room_id(&namespace, room_id);
        let forward_node = event_edges_forward_node_id(&namespace, prev_event_id);
        forward_by_collection
            .entry(room_collection)
            .or_default()
            .push(forward_node);
    }
    let mut forward_cache: HashMap<([u8; 16], NodeId), Vec<String>> = HashMap::new();
    for (collection, node_ids) in forward_by_collection.iter_mut() {
        // Distinct nodes only: two logical (room, parent) keys can in
        // principle derive the same collection/node pair, and this makes
        // "one read per distinct node" true.
        node_ids.sort_unstable();
        node_ids.dedup();
        let found = engine
            .edge_get_many(collection, node_ids)
            .map_err(|e| map_transaction_error("get_many", e))?;
        for (forward_node, value) in node_ids.iter().zip(found) {
            let children = match value {
                Some(data) if !data.bytes.is_empty() => decode_forward_edges(&data.bytes)?,
                _ => Vec::new(),
            };
            forward_cache.insert((*collection, *forward_node), children);
        }
    }

    for ((room_id, prev_event_id), new_children) in forward_map {
        let room_collection = prev_edges_room_id(&namespace, &room_id);
        let forward_node = event_edges_forward_node_id(&namespace, &prev_event_id);

        // Every (room, parent) key was read above, so the cached list can
        // be moved out rather than cloned. A miss means two logical keys
        // derived the same collection/node pair (a node-id collision) or
        // the cache was built inconsistently; fail loudly rather than
        // write an empty list over existing children. Nothing has been
        // written yet, so a failure here leaves the store untouched.
        let mut existing_children = forward_cache
            .remove(&(room_collection, forward_node))
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "forward-edge node id collision while batching parent reads \
                         (collection {:02x?}, node {:02x?})",
                    room_collection, forward_node
                ))
            })?;

        let mut seen: HashSet<String> = existing_children.iter().cloned().collect();
        let mut changed = false;
        for child in new_children {
            if seen.insert(child.clone()) {
                existing_children.push(child);
                changed = true;
            }
        }

        if changed {
            let encoded = encode_forward_edges(&existing_children);
            dag_puts
                .entry(room_collection)
                .or_default()
                .push((forward_node, NodeData::new(bytes::Bytes::from(encoded))));
        }
        insert_event_locator(&mut locator_puts, &namespace, &room_id, &prev_event_id);
    }

    // Edge records first, locator publication last: a reader that races
    // between the two sees a miss and falls back to SQL, never a locator
    // pointing at an edge record that isn't there yet.
    for (collection, pairs) in dag_puts {
        engine
            .edge_put_many(collection, pairs)
            .map_err(|e| map_transaction_error("put_many", e))?;
    }
    for (collection, pairs) in locator_puts {
        let pairs: Vec<(NodeId, NodeData)> = pairs.into_iter().collect();
        engine
            .edge_put_many(collection, pairs)
            .map_err(|e| map_transaction_error("put_many", e))?;
    }

    Ok(())
}

/// Read backward edges: given event_ids, returns `(event_id, Option<[(prev_event_id, is_state)]>)`
///
/// Reads via `get_read_committed`, not plain `get_many`: a plain read only
/// sees the read-journal overlay while a transaction is actively publishing
/// (`overlay_reads_active`), so a reader process already open before a
/// writer's commit could otherwise miss it until its own next refresh --
/// see `res/docs/2026-09-28-event-edge-cross-process-read-visibility.md`,
/// which measured and confirmed this for the plain path.
#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn event_edges_get_backward(
    py: Python<'_>,
    namespace: String,
    event_ids: Vec<String>,
) -> PyResult<Vec<(String, Option<Vec<(String, bool)>>)>> {
    py.detach(|| {
        let engine = auth_chain_db()?;
        let node_ids: Vec<NodeId> = event_ids
            .iter()
            .map(|id| event_node_id(&namespace, id))
            .collect();

        // Phase 1: resolve room collections via locator
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

        // Phase 2: fetch edge records from resolved room collections
        let mut dag_ids: HashMap<[u8; 16], Vec<(usize, NodeId)>> = HashMap::new();
        for (position, room_collection) in room_collections.iter().enumerate() {
            if let Some(room_collection) = room_collection {
                let edge_node = event_edges_backward_node_id(&namespace, &event_ids[position]);
                dag_ids
                    .entry(*room_collection)
                    .or_default()
                    .push((position, edge_node));
            }
        }

        let mut results: Vec<Option<Vec<(String, bool)>>> = vec![None; event_ids.len()];
        for (collection, ids) in dag_ids {
            let node_ids_only: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
            let found = engine
                .get_read_committed(&collection, &node_ids_only)
                .map_err(map_read_storage_error)?;
            for ((position, _), value) in ids.into_iter().zip(found) {
                if let Some(data) = value {
                    if !data.bytes.is_empty() {
                        results[position] = Some(decode_backward_edges(&data.bytes)?);
                    }
                }
            }
        }

        Ok(event_ids.into_iter().zip(results).collect())
    })
}

/// Read forward edges: given prev_event_ids, returns `(prev_event_id, Option<[child_event_id]>)`
///
/// Reads via `get_read_committed`, not plain `get_many` -- see
/// `event_edges_get_backward`'s doc comment for why.
#[pyfunction]
pub fn event_edges_get_forward(
    py: Python<'_>,
    namespace: String,
    prev_event_ids: Vec<String>,
) -> PyResult<Vec<(String, Option<Vec<String>>)>> {
    py.detach(|| {
        let engine = auth_chain_db()?;
        let node_ids: Vec<NodeId> = prev_event_ids
            .iter()
            .map(|id| event_node_id(&namespace, id))
            .collect();

        // Phase 1: resolve room collections via locator
        let mut locator_ids: HashMap<[u8; 16], Vec<(usize, NodeId)>> = HashMap::new();
        for (position, node_id) in node_ids.iter().enumerate() {
            locator_ids
                .entry(event_locator_collection_id(&namespace, node_id))
                .or_default()
                .push((position, *node_id));
        }

        let mut room_collections: Vec<Option<[u8; 16]>> = vec![None; prev_event_ids.len()];
        for (collection, ids) in locator_ids {
            let node_ids_only: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
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

        // Phase 2: fetch forward edge records from resolved room collections
        let mut dag_ids: HashMap<[u8; 16], Vec<(usize, NodeId)>> = HashMap::new();
        for (position, room_collection) in room_collections.iter().enumerate() {
            if let Some(room_collection) = room_collection {
                let forward_node =
                    event_edges_forward_node_id(&namespace, &prev_event_ids[position]);
                dag_ids
                    .entry(*room_collection)
                    .or_default()
                    .push((position, forward_node));
            }
        }

        let mut results: Vec<Option<Vec<String>>> = vec![None; prev_event_ids.len()];
        for (collection, ids) in &dag_ids {
            let node_ids_only: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
            let found = engine
                .get_read_committed(collection, &node_ids_only)
                .map_err(map_read_storage_error)?;
            for ((position, _), value) in ids.iter().zip(found) {
                if let Some(data) = value {
                    if !data.bytes.is_empty() {
                        results[*position] = Some(decode_forward_edges(&data.bytes)?);
                    }
                }
            }
        }

        // Forward lists are a lossy cache: `event_edges_delete` tombstones a
        // purged event's backward edge but never rewrites its parents'
        // forward lists, so a returned child may no longer exist. Verify
        // every returned child still has a live (non-tombstoned) backward
        // edge in the same room collection before handing it back, batched
        // per room collection rather than per child.
        let mut child_lookup: HashMap<[u8; 16], Vec<String>> = HashMap::new();
        for (position, room_collection) in room_collections.iter().enumerate() {
            let (Some(room_collection), Some(children)) = (room_collection, &results[position])
            else {
                continue;
            };
            child_lookup
                .entry(*room_collection)
                .or_default()
                .extend(children.iter().cloned());
        }

        // Three states, not two: `Some(true)` -- a present, non-empty
        // backward record -- means the child is live and is kept.
        // `Some(false)` -- present but empty -- is `event_edges_delete`'s
        // explicit tombstone, and the child is dropped. `None` -- no record
        // at all -- is NOT the same as a tombstone: it is a legacy or
        // partially-mirrored event whose backward edge was never written
        // (see the module doc comment on repair and backfill). Silently
        // keeping or dropping that child either hides it or fabricates
        // certainty this code doesn't have, so instead the child's *parent*
        // is treated as an incomplete lookup and returned as `None` --
        // exactly the same "embedded miss" signal `event_edges_get_forward`
        // already returns when the forward node itself doesn't exist.
        // Callers (e.g. `get_successor_events` in event_federation.py)
        // already handle that `None` by falling back to SQL and re-queuing
        // a repair write for the gap via `queue_edge_write`, so this reuses
        // existing self-healing rather than inventing a new contract.
        let mut live: HashMap<([u8; 16], String), Option<bool>> = HashMap::new();
        for (collection, children) in &mut child_lookup {
            children.sort_unstable();
            children.dedup();
            let backward_node_ids: Vec<NodeId> = children
                .iter()
                .map(|child| event_edges_backward_node_id(&namespace, child))
                .collect();
            let found = engine
                .get_read_committed(collection, &backward_node_ids)
                .map_err(map_read_storage_error)?;
            for (child, value) in children.iter().zip(found) {
                let status = value.map(|data| !data.bytes.is_empty());
                live.insert((*collection, child.clone()), status);
            }
        }

        for (position, room_collection) in room_collections.iter().enumerate() {
            let Some(room_collection) = room_collection else {
                continue;
            };
            let Some(children) = results[position].as_ref() else {
                continue;
            };
            // Every status is required to be present, since `child_lookup`
            // (and so `live`) was built from these same `results` above.
            let mut incomplete = false;
            let mut kept = Vec::with_capacity(children.len());
            for child in children {
                match live.get(&(*room_collection, child.clone())) {
                    Some(Some(true)) => kept.push(child.clone()),
                    Some(Some(false)) => {}
                    Some(None) | None => {
                        incomplete = true;
                        break;
                    }
                }
            }
            // Every purged-only or incomplete parent collapses to `None`,
            // matching the pre-lazy-tombstone contract where an
            // all-children-removed (or now, not-fully-resolvable) parent
            // read back as absent rather than `Some(vec![])` or a
            // partially-trustworthy list.
            results[position] = if incomplete || kept.is_empty() {
                None
            } else {
                Some(kept)
            };
        }

        Ok(prev_event_ids.into_iter().zip(results).collect())
    })
}

/// Tombstone backward edges for purged events.
///
/// Deliberately does NOT touch parents' forward lists. Splicing a purged
/// child out of every distinct parent's forward node used to require a
/// read-modify-write per parent (`forward_read` + `mutate_write` phases,
/// dominating delete latency under normal churn) purely to keep those lists
/// exact. Instead, forward lists are left stale and `event_edges_get_forward`
/// filters out any child whose backward edge is now tombstoned (or missing)
/// before returning it — the same backward-edge lookup this function already
/// pays for, just done at read time instead of write time. This trades
/// forward-list exactness (and unbounded list growth until compaction) for a
/// delete that no longer pays for other parents' list sizes.
///
/// NOTE: this makes forward lists a lossy cache that must always be read
/// through the backward-edge filter in `event_edges_get_forward` — never
/// consumed raw. A stale entry is silently dropped at read time, not
/// resurrected; nothing about it can be relied on to reflect current state
/// without that filter.
#[pyfunction]
pub fn event_edges_delete(
    py: Python<'_>,
    namespace: String,
    event_ids: Vec<String>,
) -> PyResult<Py<PyDict>> {
    assert_writable()?;
    // Phase timings returned as a named dict (not a positional tuple) so the
    // Python diagnostics layer can't silently misread a field if one is added
    // or reordered. See `embedded_event_edges.delete_event_edges_batch`.
    let detach_started = std::time::Instant::now();
    let (lock_wait, locator_read, backward_tombstone_write, room_count, closure_duration) = py
        .detach(|| -> PyResult<(f64, f64, f64, usize, f64)> {
            // Timed from inside the closure, under the GIL-released section,
            // so it can be diffed against `detach_started` outside to isolate
            // py.detach's own GIL-reacquisition/return overhead from real
            // work done here -- without that split, a slow call can't be
            // attributed to mtxdb vs. the FFI boundary.
            let closure_started = std::time::Instant::now();
            let engine = auth_chain_db()?;
            // Locator resolution is a plain read, not a read-modify-write, so
            // it happens without any lock (see the module-level doc and
            // `lock_rooms`).
            let locator_started = std::time::Instant::now();
            let node_ids: Vec<NodeId> = event_ids
                .iter()
                .map(|id| event_node_id(&namespace, id))
                .collect();

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
            let locator_read = locator_started.elapsed().as_secs_f64();

            // Tombstone the backward edge for every purged event. This alone
            // is what `event_edges_get_forward` treats as "deleted" — no
            // forward node is read or written here.
            let mut backward_ids: HashMap<[u8; 16], Vec<NodeId>> = HashMap::new();
            for (position, room_collection) in room_collections.iter().enumerate() {
                if let Some(room_collection) = room_collection {
                    let backward_node =
                        event_edges_backward_node_id(&namespace, &event_ids[position]);
                    backward_ids
                        .entry(*room_collection)
                        .or_default()
                        .push(backward_node);
                }
            }

            // The tombstone write below is an unconditional overwrite, not a
            // read-modify-write, so this lock isn't needed for its own
            // correctness. It's taken anyway, in the same order `put` uses,
            // purely so both operations share one lock/ordering contract --
            // otherwise a put touching {A, B} and a delete touching {B, A}
            // could deadlock (see `lock_rooms`).
            let lock_started = std::time::Instant::now();
            let _guards = lock_rooms(backward_ids.keys().copied());
            let lock_wait = lock_started.elapsed().as_secs_f64();

            let tombstone_started = std::time::Instant::now();
            let mut dag_updates: HashMap<[u8; 16], Vec<(NodeId, NodeData)>> = HashMap::new();
            for (collection, ids) in &backward_ids {
                let pairs: Vec<(NodeId, NodeData)> = ids
                    .iter()
                    .map(|id| (*id, NodeData::new(bytes::Bytes::new())))
                    .collect();
                dag_updates.insert(*collection, pairs);
            }
            for (collection, pairs) in &dag_updates {
                engine.put_many(collection, pairs).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
                })?;
            }
            let backward_tombstone_write = tombstone_started.elapsed().as_secs_f64();

            // Distinct room collections this purge resolved a locator into.
            let room_count = backward_ids.len();
            let closure_duration = closure_started.elapsed().as_secs_f64();

            Ok((
                lock_wait,
                locator_read,
                backward_tombstone_write,
                room_count,
                closure_duration,
            ))
        })?;
    let detached_duration = detach_started.elapsed().as_secs_f64();
    let timings = PyDict::new(py);
    timings.set_item("lock_wait", lock_wait)?;
    timings.set_item("locator_read", locator_read)?;
    // No forward node is read or written by delete any more (see the doc
    // comment above): there is no `forward_read`/`mutate_write`/
    // `forward_nodes` phase left to report. `backward_tombstone_write`
    // replaces the old `backward_read` key -- this phase is a write
    // (tombstoning), not a read.
    timings.set_item("backward_tombstone_write", backward_tombstone_write)?;
    // `closure_duration` covers all work done inside `py.detach`, including
    // the three named phases above plus the untimed glue between them (id
    // mapping, HashMap construction). `detached_duration` wraps `py.detach`
    // itself from the outside: `detached_duration - closure_duration` is
    // GIL-reacquisition/return overhead, not mtxdb work. Comparing both to
    // the caller's own wall-clock timing around this whole call isolates a
    // slow delete to mtxdb, to untimed Rust glue, or to the FFI boundary --
    // rather than guessing.
    timings.set_item("closure_duration", closure_duration)?;
    timings.set_item("detached_duration", detached_duration)?;
    timings.set_item("rooms", room_count)?;
    Ok(timings.unbind())
}

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(event_edges_put, m)?)?;
    m.add_function(wrap_pyfunction!(event_edges_get_backward, m)?)?;
    m.add_function(wrap_pyfunction!(event_edges_get_forward, m)?)?;
    m.add_function(wrap_pyfunction!(event_edges_delete, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_backward_and_forward_round_trip() {
        crate::database::mtxdb_syn::auth_chain_closure_tests::ensure_open();
        let ns = "ns-edges-roundtrip";
        let room = "!room-edges:example.org";

        // $child has prev_events: ($p1, true) and ($p2, false)
        //
        // No `event_json_put` seeding: `event_edges_put` publishes the locators
        // for every event a row touches, so edges written out of order with
        // respect to event_json still resolve on read.  This is the legacy /
        // repair case (events whose event_json locator was never mirrored).
        pyo3::Python::attach(|py| {
            event_edges_put(
                py,
                ns.to_string(),
                vec![
                    (
                        room.to_string(),
                        "$child".to_string(),
                        "$p1".to_string(),
                        true,
                    ),
                    (
                        room.to_string(),
                        "$child".to_string(),
                        "$p2".to_string(),
                        false,
                    ),
                ],
            )
            .expect("put edges");

            // Check backward edges for $child
            let got_backward = event_edges_get_backward(
                py,
                ns.to_string(),
                vec!["$child".to_string(), "$nonexistent".to_string()],
            )
            .expect("get backward");
            assert_eq!(got_backward.len(), 2);
            assert_eq!(got_backward[0].0, "$child");
            let child_preds = got_backward[0].1.as_ref().expect("child preds found");
            assert_eq!(child_preds.len(), 2);
            assert_eq!(child_preds[0], ("$p1".to_string(), true));
            assert_eq!(child_preds[1], ("$p2".to_string(), false));
            assert_eq!(got_backward[1].1, None);

            // Check forward edges for $p1 and $p2
            let got_forward = event_edges_get_forward(
                py,
                ns.to_string(),
                vec![
                    "$p1".to_string(),
                    "$p2".to_string(),
                    "$nonexistent".to_string(),
                ],
            )
            .expect("get forward");
            assert_eq!(got_forward.len(), 3);
            assert_eq!(
                got_forward[0].1.as_deref(),
                Some(&["$child".to_string()][..])
            );
            assert_eq!(
                got_forward[1].1.as_deref(),
                Some(&["$child".to_string()][..])
            );
            assert_eq!(got_forward[2].1, None);

            // Add another child ($child2) pointing to $p1
            event_edges_put(
                py,
                ns.to_string(),
                vec![(
                    room.to_string(),
                    "$child2".to_string(),
                    "$p1".to_string(),
                    false,
                )],
            )
            .expect("put second child");

            let p1_forward = event_edges_get_forward(py, ns.to_string(), vec!["$p1".to_string()])
                .expect("get p1 forward");
            let p1_children = p1_forward[0].1.as_ref().expect("p1 children");
            assert_eq!(p1_children.len(), 2);
            assert!(p1_children.contains(&"$child".to_string()));
            assert!(p1_children.contains(&"$child2".to_string()));

            // Deletion tombstones $child and removes $child from parent forward lists
            event_edges_delete(py, ns.to_string(), vec!["$child".to_string()]).expect("delete");
            let after_delete =
                event_edges_get_backward(py, ns.to_string(), vec!["$child".to_string()])
                    .expect("get after delete");
            assert_eq!(after_delete[0].1, None);

            let forward_after_delete = event_edges_get_forward(
                py,
                ns.to_string(),
                vec!["$p1".to_string(), "$p2".to_string()],
            )
            .expect("get forward after delete");
            // $p1 only has $child2 now
            assert_eq!(
                forward_after_delete[0].1.as_deref(),
                Some(&["$child2".to_string()][..])
            );
            // $p2 has no remaining children, so it was tombstoned
            assert_eq!(forward_after_delete[1].1, None);
        });
    }

    #[test]
    fn locator_puts_are_deduplicated_within_a_batch() {
        let ns = "ns-edges-locator-dedup";
        let room = "!room-edges:example.org";
        let mut locators: HashMap<[u8; 16], HashMap<NodeId, NodeData>> = HashMap::new();

        // An event is touched many times in a single batch: as its own
        // backward row and once as the parent of every child.  Each occurrence
        // would otherwise publish the same `event_id -> room` locator.
        insert_event_locator(&mut locators, ns, room, "$shared");
        insert_event_locator(&mut locators, ns, room, "$shared");
        insert_event_locator(&mut locators, ns, room, "$shared");

        // A distinct event in the same room collection still gets its own
        // locator, so deduplication does not drop real entries.
        insert_event_locator(&mut locators, ns, room, "$other");

        let total: usize = locators.values().map(|pairs| pairs.len()).sum();
        assert_eq!(total, 2, "one locator per distinct event id");
    }

    /// `embedded-edge-tombstones.md`: a delete must not read or rewrite any
    /// parent's forward list. Deleting children therefore leaves stale ids
    /// behind, and the raw list grows until a compactor exists; reads stay
    /// correct because the filter drops tombstoned children.
    ///
    /// The invariant is asserted at the raw-node level on purpose: the Python
    /// FFI always applies the backward-edge filter, so it can never observe a
    /// stale id. A raw read is the only way to prove the delete avoided the
    /// RMW *and* that the growth the compaction design must bound is real.
    #[test]
    fn delete_leaves_forward_lists_stale_and_growth_is_real() {
        crate::database::mtxdb_syn::auth_chain_closure_tests::ensure_open();
        let ns = "ns-edges-staleness-growth";
        let room = "!room-edges:example.org";
        let parent = "$stale-parent";
        let child_count = 64usize;

        pyo3::Python::attach(|py| {
            let children: Vec<String> = (0..child_count).map(|i| format!("$sc{i}")).collect();
            let rows: Vec<(String, String, String, bool)> = children
                .iter()
                .map(|child| (room.to_string(), child.clone(), parent.to_string(), false))
                .collect();
            event_edges_put(py, ns.to_string(), rows).expect("put children");

            let engine = auth_chain_db().expect("edge engine");
            let collection = prev_edges_room_id(ns, room);
            let node = event_edges_forward_node_id(ns, parent);

            let before = engine
                .get_many(&collection, std::slice::from_ref(&node))
                .expect("raw get_many before")
                .into_iter()
                .next()
                .expect("one result before")
                .expect("forward node present before")
                .bytes
                .to_vec();
            assert_eq!(
                decode_forward_edges(&before).expect("decode before").len(),
                child_count
            );

            // Purge every other child. `event_edges_delete` tombstones only the
            // backward records; the parent forward list is deliberately left
            // untouched, so the delete cannot pay for the list's size.
            let purged: Vec<String> = children.iter().step_by(2).cloned().collect();
            event_edges_delete(py, ns.to_string(), purged.clone()).expect("delete children");

            let after = engine
                .get_many(&collection, std::slice::from_ref(&node))
                .expect("raw get_many after")
                .into_iter()
                .next()
                .expect("one result after")
                .expect("forward node present after")
                .bytes
                .to_vec();
            assert_eq!(
                before, after,
                "delete must not rewrite the parent's forward list"
            );
            assert_eq!(
                decode_forward_edges(&after).expect("decode after").len(),
                child_count,
                "stale ids remain in the raw forward list until compaction"
            );

            // The filtered read is still correct: only live children survive.
            let forward = event_edges_get_forward(py, ns.to_string(), vec![parent.to_string()])
                .expect("filtered get forward");
            let kept = forward[0].1.as_ref().expect("live children present");
            let expected: Vec<String> = children
                .iter()
                .filter(|child| !purged.contains(*child))
                .cloned()
                .collect();
            assert_eq!(kept.len(), expected.len());
            for child in &expected {
                assert!(kept.contains(child), "live child {child} must be kept");
            }
            for child in &purged {
                assert!(
                    !kept.contains(child),
                    "purged child {child} must be filtered"
                );
            }
        });
    }

    /// The per-room lock (`lock_rooms`) must still make `put`'s forward-list
    /// read-modify-write atomic for concurrent batches in the *same* room:
    /// otherwise two threads both read the empty list, both append their own
    /// child, and one write clobbers the other's.
    #[test]
    fn concurrent_puts_same_room_do_not_lose_children() {
        crate::database::mtxdb_syn::auth_chain_closure_tests::ensure_open();
        let ns = "ns-edges-concurrent-same-room";
        let room = "!room-edges:example.org";
        let parent = "$shared-parent";
        let thread_count = 16usize;

        let handles: Vec<_> = (0..thread_count)
            .map(|i| {
                let child = format!("$concurrent-child{i}");
                std::thread::spawn(move || {
                    pyo3::Python::attach(|py| {
                        event_edges_put(
                            py,
                            ns.to_string(),
                            vec![(room.to_string(), child, parent.to_string(), false)],
                        )
                        .expect("put from concurrent thread")
                    });
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("thread did not panic");
        }

        let forward = pyo3::Python::attach(|py| {
            event_edges_get_forward(py, ns.to_string(), vec![parent.to_string()])
                .expect("get forward after concurrent puts")
        });
        let children = forward[0].1.as_ref().expect("parent has children");
        assert_eq!(
            children.len(),
            thread_count,
            "every concurrent put's child must survive the RMW, none lost to a lost update"
        );
        for i in 0..thread_count {
            assert!(
                children.contains(&format!("$concurrent-child{i}")),
                "child {i} missing from forward list"
            );
        }
    }

    /// Rooms that don't overlap must not serialize against each other: that's
    /// the entire point of replacing the process-wide `RMW_LOCK` with
    /// `lock_rooms`. Hold room A's lock on the main thread and confirm a
    /// `put` touching only room B still completes promptly.
    #[test]
    fn concurrent_puts_independent_rooms_do_not_serialize() {
        crate::database::mtxdb_syn::auth_chain_closure_tests::ensure_open();
        let ns = "ns-edges-concurrent-independent-rooms";
        let room_a = "!room-a-edges:example.org";
        let room_b = "!room-b-edges:example.org";

        let room_a_collection = prev_edges_room_id(ns, room_a);
        let _held = lock_rooms([room_a_collection]);

        let (tx, rx) = std::sync::mpsc::channel();
        let ns_owned = ns.to_string();
        let room_b_owned = room_b.to_string();
        let handle = std::thread::spawn(move || {
            pyo3::Python::attach(|py| {
                event_edges_put(
                    py,
                    ns_owned,
                    vec![(
                        room_b_owned,
                        "$independent-child".to_string(),
                        "$independent-parent".to_string(),
                        false,
                    )],
                )
                .expect("put to independent room");
            });
            let _ = tx.send(());
        });

        rx.recv_timeout(std::time::Duration::from_secs(2))
            .expect("put to an unrelated room must not block behind room A's held lock");
        handle.join().expect("thread did not panic");
        drop(_held);
    }
}
