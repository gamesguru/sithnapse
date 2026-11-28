//! Room timelines in mtxdb's ordered event indexes.
//!
//! Event persistence stages one row per non-outlier event, and the indexes
//! answer the two room paginations Synapse runs in SQL:
//!
//! - topological: `ORDER BY topological_ordering, origin_server_ts,
//!   stream_ordering` (`_paginate_room_events_by_topological_ordering_txn`);
//! - stream: `ORDER BY stream_ordering` (`paginate_room_events_by_stream_ordering`).
//!
//! Each event also gets a small record mapping its body node id back to the
//! event id, in a per-room collection (`event_id_collection`), because index
//! entries carry only the 16-byte node id.
//!
//! Writes: [`PyMtxdbTransaction`](super::mtxdb_syn::PyMtxdbTransaction) buffers
//! rows and stages them at commit. Both orderings keep one head record shared
//! by every room, so two persists that each staged an append would conflict
//! with `StaleRead`. Staging under [`STAGE_LOCK`] and committing before
//! releasing it serializes only that window within this process. Several
//! event-persister processes writing timelines would still conflict and retry.
//!
//! Reads go through [`ReadCommittedEngine`] so a reader worker sees heads and
//! tree records the writer published after the worker opened.

use std::ops::Bound as RangeBound;
use std::sync::Mutex;

use mtxdb::ordered_timeline::{
    open_event_timeline_indexes, stage_event_timeline_batch, stage_purge_room_timeline,
    stage_remove_event_timeline_batch, EventStreamKey, EventTimelineEntry, EventTopoKey,
    ReadCommittedEngine,
};
use mtxdb::storage::StorageError;
use mtxdb::{
    Database, DatabaseTransaction, NodeData, NodeId, PackfileStorage, ShardType, StorageEngine,
};
use pyo3::prelude::*;

use super::mtxdb_syn::{
    assert_writable, event_dag_db, event_dag_room_id, event_node_id, group_full_logical_id,
    map_read_storage_error, member_collection_id, shared_database,
};

/// One event as persistence supplies it:
/// `(room_id, event_id, topological_ordering, origin_server_ts, stream_ordering)`.
pub(crate) type TimelineRow = (String, String, i64, i64, i64);

/// One returned page row: `(event_id, topological_ordering, origin_server_ts,
/// stream_ordering)`, the columns Synapse's pagination SQL selects.
type PageRow = (String, i64, i64, i64);

/// One event's resolved id and positions:
/// `(event_id, topological_ordering, origin_server_ts, stream_ordering)`.
///
/// Neither index key carries both positions: the topological key drops the
/// stream ordering and the stream key drops depth and timestamp. Synapse's
/// pagination needs all four, so the per-room id record keeps the full entry.
type ResolvedEntry = (String, i64, i64, i64);

const TIMELINE_ID_RECORD_VERSION: u8 = 1;
const TIMELINE_ID_RECORD_HEADER: usize = 25;

/// A Synapse pagination bound as `generate_pagination_bounds` returns it:
/// `(topological, origin_server_ts, stream)`, where the first two are `None`
/// for a live (stream-only) token.
type PyBound = (Option<i64>, Option<i64>, i64);

/// Serializes timeline staging and commit within this process; see the module
/// docs.
static STAGE_LOCK: Mutex<()> = Mutex::new(());

/// Index entries read per step while a stream-only bound filters a
/// topological scan.
const FILTER_STEP: usize = 256;

/// Per-room collection mapping an event body's node id to its event id.
pub(crate) fn event_id_collection(room_id: &str) -> [u8; 16] {
    let group = format!("sys:event-id:{room_id}");
    member_collection_id(*b"EVNT", &group_full_logical_id(group.as_bytes()))
}

/// Encode the per-event id record: version, all three positions, event id.
///
/// The positions are kept here rather than in the index keys because each key
/// is a projection that omits the other direction.
fn encode_timeline_id_record(
    event_id: &str,
    topological: i64,
    origin_server_ts: i64,
    stream: i64,
) -> Vec<u8> {
    let mut value = Vec::with_capacity(TIMELINE_ID_RECORD_HEADER + event_id.len());
    value.push(TIMELINE_ID_RECORD_VERSION);
    value.extend_from_slice(&topological.to_le_bytes());
    value.extend_from_slice(&origin_server_ts.to_le_bytes());
    value.extend_from_slice(&stream.to_le_bytes());
    value.extend_from_slice(event_id.as_bytes());
    value
}

fn decode_timeline_id_record(value: &[u8]) -> Option<ResolvedEntry> {
    if value.first() != Some(&TIMELINE_ID_RECORD_VERSION) || value.len() < TIMELINE_ID_RECORD_HEADER
    {
        return None;
    }
    let topological = i64::from_le_bytes(value[1..9].try_into().ok()?);
    let origin_server_ts = i64::from_le_bytes(value[9..17].try_into().ok()?);
    let stream = i64::from_le_bytes(value[17..25].try_into().ok()?);
    let event_id = std::str::from_utf8(&value[TIMELINE_ID_RECORD_HEADER..])
        .ok()?
        .to_owned();
    Some((event_id, topological, origin_server_ts, stream))
}

/// Reject rows that cannot be indexed.
pub(crate) fn validate_rows(rows: &[TimelineRow]) -> PyResult<()> {
    for (room_id, event_id, ..) in rows {
        if room_id.is_empty() || event_id.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "event_timeline_put needs a room id and an event id for every row",
            ));
        }
    }
    Ok(())
}

fn io_error(error: std::io::Error) -> StorageError {
    StorageError::Io(error)
}

/// Stage `rows` in `transaction` and run `commit` while holding
/// [`STAGE_LOCK`], so no other in-process persist can move the shared heads
/// between staging and publication. With no rows, just commits.
///
/// # Errors
/// Staging errors, or whatever `commit` returns.
pub(crate) fn commit_with_timeline<T>(
    transaction: &DatabaseTransaction<'_>,
    database: &Database,
    rows: Vec<TimelineRow>,
    commit: impl FnOnce() -> Result<T, StorageError>,
) -> Result<T, StorageError> {
    if rows.is_empty() {
        return commit();
    }
    let _guard = STAGE_LOCK
        .lock()
        .map_err(|_| StorageError::Internal("timeline stage lock poisoned".to_owned()))?;
    let mut entries = Vec::with_capacity(rows.len());
    for (room_id, event_id, topological, origin_server_ts, stream) in &rows {
        let node = event_node_id(event_id);
        transaction
            .put(
                ShardType::EventDag,
                event_id_collection(room_id),
                node,
                &NodeData::new(bytes::Bytes::from(encode_timeline_id_record(
                    event_id,
                    *topological,
                    *origin_server_ts,
                    *stream,
                ))),
            )
            .map_err(io_error)?;
        entries.push(EventTimelineEntry::new(
            event_dag_room_id(room_id),
            *topological,
            *origin_server_ts,
            *stream,
            node,
        ));
    }
    let pool = database.pool(ShardType::EventDag);
    let (topo, stream) = open_event_timeline_indexes(pool.as_ref());
    // No payloads are staged here, so the payload collection is unused.
    stage_event_timeline_batch(
        transaction,
        ShardType::EventDag,
        entries[0].room_scope,
        &[],
        &topo,
        &stream,
        &entries,
    )?;
    commit()
}

/// Index rows directly, for persistence without a transaction (no shared
/// WAL, or transactional persistence turned off).
///
/// Best effort, like the direct `event_json_put`: the event-id records are
/// written before either index, so an index entry never points at an unknown
/// event, but a crash between the two orderings can leave one without the
/// other. Readers treat an incomplete room as a reason to use SQL.
#[pyfunction]
pub fn event_timeline_put(py: Python<'_>, rows: Vec<TimelineRow>) -> PyResult<()> {
    assert_writable()?;
    validate_rows(&rows)?;
    if rows.is_empty() {
        return Ok(());
    }
    py.detach(|| {
        let engine = event_dag_db()?;
        let _guard = STAGE_LOCK.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err("timeline stage lock poisoned")
        })?;
        let mut by_room: std::collections::HashMap<[u8; 16], Vec<(NodeId, NodeData)>> =
            std::collections::HashMap::new();
        let mut entries = Vec::with_capacity(rows.len());
        for (room_id, event_id, topological, origin_server_ts, stream) in &rows {
            let node = event_node_id(event_id);
            by_room
                .entry(event_id_collection(room_id))
                .or_default()
                .push((
                    node,
                    NodeData::new(bytes::Bytes::from(encode_timeline_id_record(
                        event_id,
                        *topological,
                        *origin_server_ts,
                        *stream,
                    ))),
                ));
            entries.push(EventTimelineEntry::new(
                event_dag_room_id(room_id),
                *topological,
                *origin_server_ts,
                *stream,
                node,
            ));
        }
        let write_error = |e: StorageError| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("timeline write error: {e}"))
        };
        for (collection, records) in by_room {
            engine
                .put_many(&collection, &records)
                .map_err(write_error)?;
        }
        let (topo, stream) = open_event_timeline_indexes(engine.as_ref());
        let topo_keys: Vec<_> = entries.iter().map(|e| e.topo_key()).collect();
        let stream_keys: Vec<_> = entries.iter().map(|e| e.stream_key()).collect();
        topo.append_untransactional(&topo_keys)
            .map_err(write_error)?;
        stream
            .append_untransactional(&stream_keys)
            .map_err(write_error)?;
        Ok(())
    })
}

/// Remove event rows from both ordered indexes and their per-room ID maps.
///
/// With the shared WAL, all removals publish atomically. Without it, remove
/// the ID mappings first so any concurrent page that sees an index entry
/// falls back to SQL while the two index orders are being updated.
#[pyfunction]
pub fn timeline_remove_rows(py: Python<'_>, rows: Vec<TimelineRow>) -> PyResult<()> {
    assert_writable()?;
    validate_rows(&rows)?;
    if rows.is_empty() {
        return Ok(());
    }
    let database = shared_database()?;
    let engine = event_dag_db()?;
    py.detach(|| {
        let write_error = |error: StorageError| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("timeline removal error: {error}"))
        };
        let _guard = STAGE_LOCK.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err("timeline stage lock poisoned")
        })?;
        let mut entries = Vec::with_capacity(rows.len());
        let mut ids_by_collection: std::collections::HashMap<[u8; 16], Vec<NodeId>> =
            std::collections::HashMap::new();
        for (room_id, event_id, topological, origin_server_ts, stream) in &rows {
            let node = event_node_id(event_id);
            entries.push(EventTimelineEntry::new(
                event_dag_room_id(room_id),
                *topological,
                *origin_server_ts,
                *stream,
                node,
            ));
            ids_by_collection
                .entry(event_id_collection(room_id))
                .or_default()
                .push(node);
        }
        let topo_keys: Vec<_> = entries.iter().map(|entry| entry.topo_key()).collect();
        let stream_keys: Vec<_> = entries.iter().map(|entry| entry.stream_key()).collect();
        match &database {
            Some(database) => {
                let pool = database.pool(ShardType::EventDag);
                let (topo, stream) = open_event_timeline_indexes(pool.as_ref());
                let transaction = database.begin_transaction();
                stage_remove_event_timeline_batch(
                    &transaction,
                    ShardType::EventDag,
                    &topo,
                    &stream,
                    &entries,
                )
                .map_err(write_error)?;
                for (collection, node_ids) in ids_by_collection {
                    transaction
                        .remove_many(ShardType::EventDag, collection, &node_ids)
                        .map_err(io_error)
                        .map_err(write_error)?;
                }
                transaction.commit().map_err(write_error)
            }
            None => {
                let (topo, stream) = open_event_timeline_indexes(engine.as_ref());
                for (collection, node_ids) in ids_by_collection {
                    for node_id in node_ids {
                        let _ = engine.remove(&collection, &node_id).map_err(write_error)?;
                    }
                }
                topo.remove_untransactional(&topo_keys)
                    .map_err(write_error)?;
                stream
                    .remove_untransactional(&stream_keys)
                    .map_err(write_error)?;
                Ok(())
            }
        }
    })
}

/// Resolve node ids to their full event entries through the room's id
/// collection. `None` when any id is missing or was written in an older
/// id-only format: the caller must then use SQL.
fn resolve_entries(
    engine: &PackfileStorage,
    room_id: &str,
    nodes: &[NodeId],
) -> PyResult<Option<Vec<ResolvedEntry>>> {
    if nodes.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let records = engine
        .get_read_committed(&event_id_collection(room_id), nodes)
        .map_err(map_read_storage_error)?;
    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        let Some(data) = record else {
            return Ok(None);
        };
        let Some(entry) = decode_timeline_id_record(&data.bytes) else {
            return Ok(None);
        };
        entries.push(entry);
    }
    Ok(Some(entries))
}

fn read_error(error: StorageError) -> PyErr {
    map_read_storage_error(error)
}

/// Whether `stream` passes a stream-only bound, using
/// `generate_pagination_where_clause`'s operators. `is_from` selects the
/// from-bound operator; `backwards` the direction.
fn passes_stream_bound(stream: i64, bound: i64, is_from: bool, backwards: bool) -> bool {
    match (is_from, backwards) {
        // from: `bound >= stream` backwards, `bound < stream` forwards.
        (true, true) => stream <= bound,
        (true, false) => stream > bound,
        // to: `bound < stream` backwards, `bound >= stream` forwards.
        (false, true) => stream > bound,
        (false, false) => stream <= bound,
    }
}

/// Whether a resolved row passes one Synapse pagination bound. A full
/// `(depth, ts, stream)` bound is a lexicographic comparison; a stream-only
/// (live) bound compares the stream position alone.
fn passes_topological_bound(
    bound: PyBound,
    topological: i64,
    origin_server_ts: i64,
    stream: i64,
    is_from: bool,
    backwards: bool,
) -> bool {
    match bound {
        (Some(bound_topo), Some(bound_ts), bound_stream) => {
            let row = (topological, origin_server_ts, stream);
            let limit = (bound_topo, bound_ts, bound_stream);
            match (is_from, backwards) {
                (true, true) => row <= limit,
                (true, false) => row > limit,
                (false, true) => row > limit,
                (false, false) => row <= limit,
            }
        }
        _ => passes_stream_bound(stream, bound.2, is_from, backwards),
    }
}

/// Room events in topological order between Synapse pagination bounds.
///
/// `from_bound` and `to_bound` are `generate_pagination_bounds`' tuples, and
/// the SQL order is `(depth, origin_server_ts, stream)`. The index orders by
/// `(depth, origin_server_ts, node id)`, so a full bound seeks the whole
/// boundary group and the exact stream comparison is applied after resolving
/// the rows. A stream-only bound (a live token) filters while scanning.
///
/// Returns up to `limit` rows in pagination order, or `None` when the scan
/// budget ran out or an event entry could not be resolved: the caller must
/// then use SQL.
#[pyfunction]
#[pyo3(signature = (room_id, from_bound, to_bound, backwards, limit, scan_budget=10_000))]
pub fn timeline_page_topological(
    py: Python<'_>,
    room_id: String,
    from_bound: Option<PyBound>,
    to_bound: Option<PyBound>,
    backwards: bool,
    limit: usize,
    scan_budget: usize,
) -> PyResult<Option<Vec<PageRow>>> {
    py.detach(|| {
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let engine = event_dag_db()?;
        let view = ReadCommittedEngine(&engine);
        let (topo, _) = open_event_timeline_indexes(&view);
        let room = event_dag_room_id(&room_id);
        let forward = !backwards;

        let Some((root, _)) = topo.head().map_err(read_error)? else {
            return Ok(Some(Vec::new()));
        };
        let mut cursor = match from_bound {
            Some((Some(t), Some(ts), _)) => {
                // Start before (or after, backwards) the whole boundary group
                // so no same-(depth, ts) sibling is skipped.
                let key = if forward {
                    EventTopoKey::position_min(room, t, ts)
                } else {
                    EventTopoKey::position_max(room, t, ts)
                };
                Some(topo.cursor(root, key).map_err(read_error)?)
            }
            _ => None,
        };
        // Include the whole boundary group; the predicate filters individual
        // rows. In reverse the end is the lower bound, so seek the group's
        // minimum rather than its maximum.
        let end = match to_bound {
            Some((Some(t), Some(ts), _)) => {
                let key = if forward {
                    EventTopoKey::position_max(room, t, ts)
                } else {
                    EventTopoKey::position_min(room, t, ts)
                };
                RangeBound::Included(key)
            }
            _ => RangeBound::Unbounded,
        };
        let filtered = !matches!(from_bound, Some((Some(_), Some(_), _)))
            || !matches!(to_bound, Some((Some(_), Some(_), _)));

        let mut kept: Vec<(EventTopoKey, ResolvedEntry)> = Vec::new();
        let mut scanned = 0usize;
        loop {
            let step = if filtered || kept.len() >= limit {
                FILTER_STEP
            } else {
                limit - kept.len()
            };
            let page = topo
                .page_range(&room, cursor.as_ref(), forward, step, end)
                .map_err(read_error)?;
            scanned = scanned.saturating_add(page.entries.len());
            let nodes: Vec<NodeId> = page.entries.iter().map(|key| key.event_node_id).collect();
            let Some(entries) = resolve_entries(&engine, &room_id, &nodes)? else {
                return Ok(None);
            };
            for (key, entry) in page.entries.iter().zip(entries) {
                let (_, topological, origin_server_ts, stream) = &entry;
                let in_bounds = from_bound.is_none_or(|bound| {
                    passes_topological_bound(
                        bound,
                        *topological,
                        *origin_server_ts,
                        *stream,
                        true,
                        backwards,
                    )
                }) && to_bound.is_none_or(|bound| {
                    passes_topological_bound(
                        bound,
                        *topological,
                        *origin_server_ts,
                        *stream,
                        false,
                        backwards,
                    )
                });
                if in_bounds {
                    kept.push((*key, entry));
                }
            }
            // Equal-`(depth, ts)` rows are ordered by node id in the index but
            // by stream in SQL. Keep scanning until the group holding the
            // limit-th row is complete, then sort that group by stream.
            let done = if kept.len() >= limit {
                let (_, group_topo, group_ts, _) = &kept[limit - 1].1;
                match page.entries.last() {
                    Some(last) => {
                        let last_group = (last.topological_ordering, last.origin_server_ts);
                        if forward {
                            last_group > (*group_topo, *group_ts)
                        } else {
                            last_group < (*group_topo, *group_ts)
                        }
                    }
                    None => true,
                }
            } else {
                false
            };
            if done {
                break;
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
            if scanned >= scan_budget {
                return Ok(None);
            }
        }
        kept.sort_by(|a, b| {
            let a_key = (a.1 .1, a.1 .2, a.1 .3);
            let b_key = (b.1 .1, b.1 .2, b.1 .3);
            if forward {
                a_key.cmp(&b_key)
            } else {
                b_key.cmp(&a_key)
            }
        });
        kept.truncate(limit);
        Ok(Some(
            kept.into_iter()
                .map(|(_, (id, topo, ts, stream))| (id, topo, ts, stream))
                .collect(),
        ))
    })
}

/// Room events in stream order between stream positions, with
/// `paginate_room_events_by_stream_ordering`'s bounds: forwards
/// `from < x <= to`, backwards `from >= x > to`.
///
/// Returns up to `limit` rows in pagination order, or `None` when an event id
/// could not be resolved.
#[pyfunction]
pub fn timeline_page_stream(
    py: Python<'_>,
    room_id: String,
    from_stream: Option<i64>,
    to_stream: Option<i64>,
    backwards: bool,
    limit: usize,
) -> PyResult<Option<Vec<PageRow>>> {
    py.detach(|| {
        let engine = event_dag_db()?;
        let view = ReadCommittedEngine(&engine);
        let (_, stream) = open_event_timeline_indexes(&view);
        let room = event_dag_room_id(&room_id);
        let forward = !backwards;
        let end = match (to_stream, forward) {
            (None, _) => RangeBound::Unbounded,
            (Some(to), true) => RangeBound::Included(EventStreamKey::position_max(room, to)),
            (Some(to), false) => RangeBound::Excluded(EventStreamKey::position_max(room, to)),
        };
        let Some((root, _)) = stream.head().map_err(read_error)? else {
            return Ok(Some(Vec::new()));
        };
        let cursor = from_stream
            .map(|from| stream.cursor(root, EventStreamKey::position_max(room, from)))
            .transpose()
            .map_err(read_error)?;
        let page = stream
            .page_range(&room, cursor.as_ref(), forward, limit, end)
            .map_err(read_error)?;
        let nodes: Vec<NodeId> = page.entries.iter().map(|key| key.event_node_id).collect();
        let Some(entries) = resolve_entries(&engine, &room_id, &nodes)? else {
            return Ok(None);
        };
        Ok(Some(entries))
    })
}

/// Remove every timeline entry of a room from both orderings, then its
/// event-id records.
///
/// With the shared WAL both orderings are purged as one committed group;
/// without it, directly, one ordering after the other. The id records go last
/// so no remaining entry points at a missing id.
#[pyfunction]
pub fn timeline_purge_room(py: Python<'_>, room_id: String) -> PyResult<()> {
    assert_writable()?;
    let database = shared_database()?;
    let engine = event_dag_db()?;
    py.detach(|| {
        let purge_error = |e: StorageError| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("timeline purge error: {e}"))
        };
        let _guard = STAGE_LOCK.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err("timeline stage lock poisoned")
        })?;
        let room = event_dag_room_id(&room_id);
        let (topo, stream) = open_event_timeline_indexes(engine.as_ref());
        // Nothing was ever indexed: there are no heads to purge from.
        if topo.head().map_err(purge_error)?.is_some()
            || stream.head().map_err(purge_error)?.is_some()
        {
            match &database {
                Some(database) => {
                    let pool = database.pool(ShardType::EventDag);
                    let (topo, stream) = open_event_timeline_indexes(pool.as_ref());
                    let transaction = database.begin_transaction();
                    stage_purge_room_timeline(
                        &transaction,
                        ShardType::EventDag,
                        &topo,
                        &stream,
                        &room,
                    )
                    .and_then(|_| transaction.commit())
                    .map_err(purge_error)?;
                }
                None => {
                    topo.purge_prefix_untransactional(room)
                        .map_err(purge_error)?;
                    stream
                        .purge_prefix_untransactional(room)
                        .map_err(purge_error)?;
                }
            }
        }
        engine
            .delete_collection(&event_id_collection(&room_id))
            .map_err(purge_error)
    })
}

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(event_timeline_put, m)?)?;
    m.add_function(wrap_pyfunction!(timeline_remove_rows, m)?)?;
    m.add_function(wrap_pyfunction!(timeline_page_topological, m)?)?;
    m.add_function(wrap_pyfunction!(timeline_page_stream, m)?)?;
    m.add_function(wrap_pyfunction!(timeline_purge_room, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;
    use crate::database::mtxdb_syn::auth_chain_closure_tests::ensure_open;

    fn row(room: &str, n: i64, topo: i64, ts: i64, stream: i64) -> TimelineRow {
        (
            room.to_owned(),
            format!("$event-{room}-{n}"),
            topo,
            ts,
            stream,
        )
    }

    fn ids(rows: &Option<Vec<PageRow>>) -> Vec<String> {
        rows.as_ref()
            .expect("index answered")
            .iter()
            .map(|r| r.0.clone())
            .collect()
    }

    /// Concurrent commits in different rooms share both heads; staging under
    /// the lock must keep them from failing with `StaleRead`.
    #[test]
    fn concurrent_transactions_in_different_rooms_both_commit() {
        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(dir.path().to_path_buf()).unwrap());
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|worker| {
                let database = Arc::clone(&database);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let room = format!("!room{worker}:example.org");
                    for n in 0..20_i64 {
                        let transaction = database.begin_transaction();
                        let rows = vec![row(&room, n, n, 0, n.saturating_add(1))];
                        if n == 0 {
                            barrier.wait();
                        }
                        commit_with_timeline(&transaction, &database, rows, || {
                            transaction.commit()
                        })
                        .unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        let pool = database.pool(ShardType::EventDag);
        let (topo, _) = open_event_timeline_indexes(pool.as_ref());
        for worker in 0..8 {
            let room = event_dag_room_id(&format!("!room{worker}:example.org"));
            assert_eq!(topo.page(&room, None, true, 100).unwrap().entries.len(), 20);
        }
    }

    #[test]
    fn pages_match_synapse_pagination_bounds() {
        ensure_open();
        let room = "!timeline-bounds:example.org";
        // Depth 2 holds three events whose timestamp order differs from
        // their stream order; stream -1 is backfilled.
        let rows = vec![
            row(room, 1, 1, 50, -1),
            row(room, 2, 2, 300, 2),
            row(room, 3, 2, 100, 5),
            row(room, 4, 2, 200, 3),
            row(room, 5, 3, 0, 4),
            row(room, 6, 4, 0, 6),
        ];
        Python::attach(|py| {
            event_timeline_put(py, rows.clone()).unwrap();
            let e = |n: i64| format!("$event-{room}-{n}");

            // Topological order: (depth, ts, stream).
            let all =
                timeline_page_topological(py, room.into(), None, None, false, 10, 1000).unwrap();
            assert_eq!(ids(&all), [e(1), e(3), e(4), e(2), e(5), e(6)]);

            // Forwards from < x <= to, both full tokens.
            let page = timeline_page_topological(
                py,
                room.into(),
                Some((Some(2), Some(100), 5)),
                Some((Some(3), Some(0), 4)),
                false,
                10,
                1000,
            )
            .unwrap();
            assert_eq!(ids(&page), [e(4), e(2), e(5)]);

            // Backwards from >= x > to.
            let page = timeline_page_topological(
                py,
                room.into(),
                Some((Some(3), Some(0), 4)),
                Some((Some(2), Some(100), 5)),
                true,
                10,
                1000,
            )
            .unwrap();
            assert_eq!(ids(&page), [e(5), e(2), e(4)]);

            // Backwards from a live (stream-only) token: stream <= 4.
            let page = timeline_page_topological(
                py,
                room.into(),
                Some((None, None, 4)),
                None,
                true,
                10,
                1000,
            )
            .unwrap();
            assert_eq!(ids(&page), [e(5), e(2), e(4), e(1)]);

            // A tiny scan budget makes a filtered read give up.
            let none = timeline_page_topological(
                py,
                room.into(),
                Some((None, None, -5)),
                None,
                true,
                10,
                1,
            )
            .unwrap();
            assert!(none.is_none() || ids(&none).is_empty());

            // Stream order, backwards from >= x > to.
            let page = timeline_page_stream(py, room.into(), Some(5), Some(2), true, 10).unwrap();
            assert_eq!(ids(&page), [e(3), e(5), e(4)]);
            // Forwards from < x <= to, with the backfilled event.
            let page = timeline_page_stream(py, room.into(), Some(-2), Some(3), false, 10).unwrap();
            assert_eq!(ids(&page), [e(1), e(2), e(4)]);
            // Limit.
            let page = timeline_page_stream(py, room.into(), None, None, true, 2).unwrap();
            assert_eq!(ids(&page), [e(6), e(3)]);

            let rows = all.unwrap();
            assert_eq!(rows[1], (e(3), 2, 100, 5));

            timeline_purge_room(py, room.into()).unwrap();
            let after = timeline_page_stream(py, room.into(), None, None, false, 10).unwrap();
            assert_eq!(ids(&after), Vec::<String>::new());
            // Purging a room with no entries is a no-op.
            timeline_purge_room(py, room.into()).unwrap();
        });
    }

    /// The index orders equal-`(depth, ts)` rows by node id; the page must
    /// still return them in stream order and not skip the boundary group.
    #[test]
    fn topological_page_orders_equal_depth_and_ts_by_stream() {
        ensure_open();
        let room = "!timeline-ties:example.org";
        let rows = vec![
            row(room, 1, 2, 100, 7),
            row(room, 2, 2, 100, 3),
            row(room, 3, 2, 100, 5),
            row(room, 4, 3, 0, 8),
        ];
        Python::attach(|py| {
            event_timeline_put(py, rows).unwrap();
            let e = |n: i64| format!("$event-{room}-{n}");

            // Tied depth/timestamp rows come back in stream order.
            let all =
                timeline_page_topological(py, room.into(), None, None, false, 10, 1000).unwrap();
            assert_eq!(ids(&all), [e(2), e(3), e(1), e(4)]);

            // A cursor inside the tied group must not skip the remaining rows.
            let page = timeline_page_topological(
                py,
                room.into(),
                Some((Some(2), Some(100), 3)),
                None,
                false,
                10,
                1000,
            )
            .unwrap();
            assert_eq!(ids(&page), [e(3), e(1), e(4)]);

            // Backwards keeps the from row and excludes the to row.
            let page = timeline_page_topological(
                py,
                room.into(),
                Some((Some(2), Some(100), 5)),
                Some((Some(2), Some(100), 3)),
                true,
                10,
                1000,
            )
            .unwrap();
            assert_eq!(ids(&page), [e(3)]);

            timeline_purge_room(py, room.into()).unwrap();
        });
    }
}
