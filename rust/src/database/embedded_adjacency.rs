//! Event adjacency (`prev_events`, `auth_events`) in mtxdb's CSR store.
//!
//! One `CsrAdjacency` per room, addressed by the room's 16-byte collection id
//! (`CsrAdjacency::for_collection`). Edges are recorded with the event and read
//! back through short ids:
//!
//! - backward (`prev_of`): the event's `prev_events`;
//! - forward (`children_of`): the events naming it in `prev_events`, a point
//!   read of the maintained reverse row.
//!
//! Reads find a room from an event id through the event locator
//! (`event_id -> room collection`), which `event_json_put` publishes and
//! `adjacency_put` re-publishes for every event a row touches, so an edge to a
//! parent whose body has not arrived still resolves.
//!
//! CSR rows are immutable once written. Retention deletes leave them; a room
//! purge drops the room's whole scope (`adjacency_purge_room`).

use std::collections::HashMap;

use mtxdb::csr_adjacency::{AppendDurability, CsrAdjacency, EventRecord};
use mtxdb::edge_log::EdgeLog;
use mtxdb::short_id::GraphReader;
use mtxdb::{NodeData, NodeId, StorageEngine};
use pyo3::prelude::*;

use super::mtxdb_syn::{
    csr_reader, csr_writer, event_dag_room_id, event_locator_collection_id, event_node_id,
    flush_adjacency_tails_io, is_retryable_contention, map_read_storage_error,
    mark_adjacency_tail_dirty, state_db,
};

/// One event's adjacency row as the caller supplies it.
type PutRow = (String, String, Vec<String>, Vec<String>); // room_id, event_id, prev, auth

fn adjacency_for(room_collection: &[u8; 16]) -> CsrAdjacency {
    CsrAdjacency::for_collection(room_collection)
}

/// Resolve each event id's room collection through its locator; `None` when no
/// locator exists.
fn resolve_rooms(event_ids: &[String]) -> PyResult<Vec<Option<[u8; 16]>>> {
    let engine = state_db()?;
    let node_ids: Vec<NodeId> = event_ids.iter().map(|id| event_node_id(id)).collect();
    let mut by_collection: HashMap<[u8; 16], Vec<(usize, NodeId)>> = HashMap::new();
    for (position, node_id) in node_ids.iter().enumerate() {
        by_collection
            .entry(event_locator_collection_id(node_id))
            .or_default()
            .push((position, *node_id));
    }
    let mut rooms: Vec<Option<[u8; 16]>> = vec![None; event_ids.len()];
    for (collection, ids) in by_collection {
        let only_ids: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
        let found = engine
            .get_read_committed(&collection, &only_ids)
            .map_err(map_read_storage_error)?;
        for ((position, _), value) in ids.into_iter().zip(found) {
            if let Some(data) = value {
                if let Ok(room) = <[u8; 16]>::try_from(data.bytes.as_ref()) {
                    rooms[position] = Some(room);
                }
            }
        }
    }
    Ok(rooms)
}

/// Record events' `prev_events` and `auth_events`.
///
/// Rows are `(room_id, event_id, prev_event_ids, auth_event_ids)`; a repeated
/// identical row is a no-op.
#[pyfunction]
#[pyo3(signature = (rows, durable = true))]
pub fn adjacency_put(py: Python<'_>, rows: Vec<PutRow>, durable: bool) -> PyResult<()> {
    let database = csr_writer()?;
    py.detach(|| {
        let mut by_room: HashMap<String, Vec<&PutRow>> = HashMap::new();
        for row in &rows {
            by_room.entry(row.0.clone()).or_default().push(row);
        }
        let mut locators: HashMap<[u8; 16], HashMap<NodeId, NodeData>> = HashMap::new();
        for (room_id, room_rows) in &by_room {
            let room_collection = event_dag_room_id(room_id);
            let prevs: Vec<Vec<&str>> = room_rows
                .iter()
                .map(|r| r.2.iter().map(String::as_str).collect())
                .collect();
            let auths: Vec<Vec<&str>> = room_rows
                .iter()
                .map(|r| r.3.iter().map(String::as_str).collect())
                .collect();
            let records: Vec<EventRecord<'_>> = room_rows
                .iter()
                .enumerate()
                .map(|(i, r)| EventRecord {
                    event_id: &r.1,
                    prev: &prevs[i],
                    auth: &auths[i],
                    relation: None,
                })
                .collect();
            // Every edge write goes through the edge log: the log entry is
            // journaled with the WAL, so a crash that loses the unsynced CSR
            // tail is repaired by `recover_edge_log` at the next open.
            adjacency_for(&room_collection)
                .record_events_logged(
                    &database,
                    &EdgeLog::new(),
                    &records,
                    if durable {
                        AppendDurability::Durable
                    } else {
                        AppendDurability::Deferred
                    },
                )
                .map_err(|e| {
                    let message = format!("mtxdb adjacency record error: {e}");
                    if is_retryable_contention(&e) {
                        pyo3::exceptions::PyBlockingIOError::new_err(message)
                    } else {
                        pyo3::exceptions::PyRuntimeError::new_err(message)
                    }
                })?;
            // Even a durable append leaves its log entries to prune; the next
            // flush checkpoints the log. Deferred appends also leave the tail
            // for that flush to sync.
            mark_adjacency_tail_dirty();
            // Locators for the event and every event it references, so reads
            // can find the room even where a parent's body is still missing.
            for row in room_rows {
                for id in std::iter::once(&row.1).chain(&row.2).chain(&row.3) {
                    let identity = event_node_id(id);
                    locators
                        .entry(event_locator_collection_id(&identity))
                        .or_default()
                        .insert(
                            identity,
                            NodeData::new(bytes::Bytes::copy_from_slice(&room_collection)),
                        );
                }
            }
        }
        let engine = state_db()?;
        for (collection, pairs) in locators {
            let pairs: Vec<(NodeId, NodeData)> = pairs.into_iter().collect();
            engine.put_many(&collection, &pairs).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb locator put error: {e}"))
            })?;
        }
        Ok(())
    })
}

/// `prev_events` of each event: `(event_id, Some(prev_ids))`, or `None` when the
/// event has no recorded adjacency.
#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn adjacency_get_prev(
    py: Python<'_>,
    event_ids: Vec<String>,
) -> PyResult<Vec<(String, Option<Vec<String>>)>> {
    py.detach(|| {
        let rooms = resolve_rooms(&event_ids)?;
        let graph = csr_reader()?;
        {
            let graph = &graph;
            let mut out = Vec::with_capacity(event_ids.len());
            for (event_id, room) in event_ids.iter().zip(&rooms) {
                let prev = match room {
                    Some(room) => adjacency_for(room)
                        .prev_of(graph, event_id)
                        .map_err(map_read_storage_error)?,
                    None => None,
                };
                out.push((event_id.clone(), prev));
            }
            Ok(out)
        }
    })
}

/// Events naming each id in their `prev_events`: `(event_id, Some(child_ids))`,
/// or `None` when nothing is recorded for the id.
#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn adjacency_get_children(
    py: Python<'_>,
    event_ids: Vec<String>,
) -> PyResult<Vec<(String, Option<Vec<String>>)>> {
    py.detach(|| {
        let rooms = resolve_rooms(&event_ids)?;
        let graph = csr_reader()?;
        {
            let graph = &graph;
            let mut out = Vec::with_capacity(event_ids.len());
            for (event_id, room) in event_ids.iter().zip(&rooms) {
                let children = match room {
                    Some(room) => children_of(graph, &adjacency_for(room), event_id)
                        .map_err(map_read_storage_error)?,
                    None => None,
                };
                out.push((event_id.clone(), children));
            }
            Ok(out)
        }
    })
}

fn children_of<R: GraphReader + ?Sized>(
    graph: &R,
    adjacency: &CsrAdjacency,
    event_id: &str,
) -> Result<Option<Vec<String>>, mtxdb::storage::StorageError> {
    let Some(short) = adjacency.short_id(graph, event_id)? else {
        return Ok(None);
    };
    let Some(children) = adjacency.children_of(graph, short)? else {
        return Ok(None);
    };
    adjacency
        .resolve_event_ids(graph, &children)?
        .into_iter()
        .map(|id| {
            id.ok_or_else(|| {
                mtxdb::storage::StorageError::Corrupt("child does not resolve".to_owned())
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Drop a room's whole adjacency scope (room purge).
#[pyfunction]
pub fn adjacency_purge_room(py: Python<'_>, room_id: String) -> PyResult<()> {
    let database = csr_writer()?;
    py.detach(|| {
        adjacency_for(&event_dag_room_id(&room_id))
            .purge(&database)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "mtxdb adjacency purge error: {e}"
                ))
            })?;
        // Make the tombstone durable and prune the edge log now. Entries still
        // in the log for this room would be replayed after the tombstone by a
        // later recovery and bring the purged room's edges back.
        mark_adjacency_tail_dirty();
        flush_adjacency_tails_io(&database).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "mtxdb adjacency purge flush error: {e}"
            ))
        })
    })
}

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(adjacency_put, m)?)?;
    m.add_function(wrap_pyfunction!(adjacency_get_prev, m)?)?;
    m.add_function(wrap_pyfunction!(adjacency_get_children, m)?)?;
    m.add_function(wrap_pyfunction!(adjacency_purge_room, m)?)?;
    Ok(())
}
