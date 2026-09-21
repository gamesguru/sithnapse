//! Embedded Event Edges storage in the `event_dag` mtxdb packfile storage pool.
//!
//! Stores the DAG edges connecting Matrix events:
//! - Backward edges: `event_id -> [(prev_event_id, is_state)]` (immutable, write-once).
//! - Forward edges: `prev_event_id -> [child_event_id]` (appended & deduplicated under RMW_LOCK).
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

use mtxdb_core::{NodeData, NodeId, StorageEngine};
use pyo3::prelude::*;
use sha2::{Digest, Sha256};

use super::mtxdb_syn::{
    assert_writable, event_dag_db, event_dag_room_id, event_locator_collection_id, event_node_id,
    RMW_LOCK,
};

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
    let room_collection = event_dag_room_id(namespace, room_id);
    let identity = event_node_id(namespace, event_id);
    let locator_collection = event_locator_collection_id(namespace, &identity);
    locators.entry(locator_collection).or_default().insert(
        identity,
        NodeData::new(bytes::Bytes::copy_from_slice(&room_collection)),
    );
}

/// Batch put event edges into the room-aware event_dag pool:
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
    py.detach(|| {
        let _guard = RMW_LOCK
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}")))?;
        let engine = event_dag_db()?;

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
            let room_collection = event_dag_room_id(&namespace, &room_id);
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
            let room_collection = event_dag_room_id(&namespace, room_id);
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
            let found = engine.get_many(collection, node_ids).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {e}"))
            })?;
            for (forward_node, value) in node_ids.iter().zip(found) {
                let children = match value {
                    Some(data) if !data.bytes.is_empty() => decode_forward_edges(&data.bytes)?,
                    _ => Vec::new(),
                };
                forward_cache.insert((*collection, *forward_node), children);
            }
        }

        for ((room_id, prev_event_id), new_children) in forward_map {
            let room_collection = event_dag_room_id(&namespace, &room_id);
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
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "forward-edge node id collision while batching parent reads",
                    )
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
            engine.put_many(&collection, &pairs).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
            })?;
        }
        for (collection, pairs) in locator_puts {
            let pairs: Vec<(NodeId, NodeData)> = pairs.into_iter().collect();
            engine.put_many(&collection, &pairs).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
            })?;
        }

        Ok(())
    })
}

/// Read backward edges: given event_ids, returns `(event_id, Option<[(prev_event_id, is_state)]>)`
#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn event_edges_get_backward(
    py: Python<'_>,
    namespace: String,
    event_ids: Vec<String>,
) -> PyResult<Vec<(String, Option<Vec<(String, bool)>>)>> {
    py.detach(|| {
        let engine = event_dag_db()?;
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
            let found = engine.get_many(&collection, &node_ids_only).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {e}"))
            })?;
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
#[pyfunction]
pub fn event_edges_get_forward(
    py: Python<'_>,
    namespace: String,
    prev_event_ids: Vec<String>,
) -> PyResult<Vec<(String, Option<Vec<String>>)>> {
    py.detach(|| {
        let engine = event_dag_db()?;
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
        for (collection, ids) in dag_ids {
            let node_ids_only: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
            let found = engine.get_many(&collection, &node_ids_only).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {e}"))
            })?;
            for ((position, _), value) in ids.into_iter().zip(found) {
                if let Some(data) = value {
                    if !data.bytes.is_empty() {
                        results[position] = Some(decode_forward_edges(&data.bytes)?);
                    }
                }
            }
        }

        Ok(prev_event_ids.into_iter().zip(results).collect())
    })
}

/// Tombstone backward edges for purged events and remove them from parent forward lists
#[pyfunction]
pub fn event_edges_delete(
    py: Python<'_>,
    namespace: String,
    event_ids: Vec<String>,
) -> PyResult<()> {
    assert_writable()?;
    let _guard = RMW_LOCK.lock().unwrap();
    py.detach(|| {
        let engine = event_dag_db()?;
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

        let mut dag_updates: HashMap<[u8; 16], Vec<(NodeId, NodeData)>> = HashMap::new();

        // 1. Batch-read backward edges for every purged event to find parents.
        let mut backward_ids: HashMap<[u8; 16], Vec<(usize, NodeId)>> = HashMap::new();
        for (position, room_collection) in room_collections.iter().enumerate() {
            if let Some(room_collection) = room_collection {
                let backward_node = event_edges_backward_node_id(&namespace, &event_ids[position]);
                backward_ids
                    .entry(*room_collection)
                    .or_default()
                    .push((position, backward_node));
            }
        }

        let mut backward_preds: Vec<Option<Vec<(String, bool)>>> = vec![None; event_ids.len()];
        for (collection, ids) in &backward_ids {
            let node_ids_only: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
            let found = engine.get_many(collection, &node_ids_only).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {e}"))
            })?;
            for ((position, _), value) in ids.iter().zip(found) {
                if let Some(data) = value {
                    if !data.bytes.is_empty() {
                        backward_preds[*position] = Some(decode_backward_edges(&data.bytes)?);
                    }
                }
            }
        }

        // 2. Collect the distinct (room, parent) forward nodes touched by
        // those backward edges and batch-read them in one pass per room
        // collection.
        let mut forward_positions: HashMap<([u8; 16], NodeId), Vec<usize>> = HashMap::new();
        for (position, room_collection) in room_collections.iter().enumerate() {
            let (Some(room_collection), Some(preds)) = (room_collection, &backward_preds[position])
            else {
                continue;
            };
            for (parent_id, _) in preds {
                let forward_node = event_edges_forward_node_id(&namespace, parent_id);
                forward_positions
                    .entry((*room_collection, forward_node))
                    .or_default()
                    .push(position);
            }
        }

        let mut forward_by_collection: HashMap<[u8; 16], Vec<NodeId>> = HashMap::new();
        for (room_collection, forward_node) in forward_positions.keys() {
            forward_by_collection
                .entry(*room_collection)
                .or_default()
                .push(*forward_node);
        }

        let mut forward_cache: HashMap<([u8; 16], NodeId), Vec<String>> = HashMap::new();
        for (collection, node_ids_only) in &forward_by_collection {
            let found = engine.get_many(collection, node_ids_only).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {e}"))
            })?;
            for (forward_node, value) in node_ids_only.iter().zip(found) {
                let children = match value {
                    Some(d) if !d.bytes.is_empty() => decode_forward_edges(&d.bytes)?,
                    _ => Vec::new(),
                };
                forward_cache.insert((*collection, *forward_node), children);
            }
        }

        // 3. Remove the purged events from their parents' cached forward lists.
        for ((room_collection, forward_node), positions) in &forward_positions {
            if let Some(children) = forward_cache.get_mut(&(*room_collection, *forward_node)) {
                for &position in positions {
                    let event_id = &event_ids[position];
                    children.retain(|c| c != event_id);
                }
            }
        }

        // 4. Tombstone backward edges for the purged events.
        // NOTE: locators are owned by event_json_put; do not tombstone here.
        for (collection, ids) in &backward_ids {
            for (_, backward_node) in ids {
                dag_updates
                    .entry(*collection)
                    .or_default()
                    .push((*backward_node, NodeData::new(bytes::Bytes::new())));
            }
        }

        // 5. Write updated or tombstoned forward edges.
        for ((room_col, forward_node), children) in forward_cache {
            let data = if children.is_empty() {
                NodeData::new(bytes::Bytes::new())
            } else {
                NodeData::new(bytes::Bytes::from(encode_forward_edges(&children)))
            };
            dag_updates
                .entry(room_col)
                .or_default()
                .push((forward_node, data));
        }

        for (collection, pairs) in dag_updates {
            engine.put_many(&collection, &pairs).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
            })?;
        }

        Ok(())
    })
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
}
