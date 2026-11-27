//! Per-event verdicts: whether an event is an outlier, rejected or soft-failed.
//!
//! These use mtxdb's interop [`Verdict`] record, keyed and bucketed exactly as
//! mtxdb defines it, in the EventDag pool. Persistence writes one record for
//! every event, so a missing record means the event is not stored.
//!
//! A verdict keeps a numeric rejection code, not Synapse's reason string.

use std::collections::HashMap;

use mtxdb::export_schema::{RejectionCode, SoftFailCode};
use mtxdb::interop::{
    decode_verdict, encode_verdict, event_record_id, event_verdict_collection_id, Verdict,
};
use mtxdb::{NodeData, NodeId, ShardType, StorageEngine};
use pyo3::prelude::*;

use super::mtxdb_syn::{
    assert_writable, event_dag_db, map_read_storage_error, map_transaction_io_error,
    opened_read_only,
};

/// `(event_id, outlier, rejection code, soft_failed)`.
pub(crate) type VerdictRow = (String, bool, Option<u8>, bool);

/// `(outlier, rejection code, soft_failed)`.
type VerdictValue = (bool, Option<u8>, bool);

/// Verdict records grouped by the bucket collection that holds them.
type VerdictPuts = HashMap<[u8; 16], Vec<(NodeId, NodeData)>>;

fn verdict_puts(rows: Vec<VerdictRow>) -> PyResult<VerdictPuts> {
    let mut puts = VerdictPuts::new();
    for (event_id, outlier, rejection, soft_failed) in rows {
        let node = event_record_id(event_id.as_bytes());
        let verdict = Verdict {
            outlier,
            rejection,
            soft_fail: soft_failed.then_some(SoftFailCode::AuthCheckFailed as u8),
            ..Verdict::default()
        };
        let record = encode_verdict(event_id.as_bytes(), &verdict).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("cannot encode verdict: {e}"))
        })?;
        puts.entry(event_verdict_collection_id(&node))
            .or_default()
            .push((node, NodeData::new(bytes::Bytes::from(record))));
    }
    Ok(puts)
}

fn write_puts(puts: VerdictPuts) -> PyResult<()> {
    let engine = event_dag_db()?;
    for (collection, pairs) in puts {
        engine.put_many(&collection, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
        })?;
    }
    Ok(())
}

/// Stage `rows` in a persistence transaction.
pub(crate) fn stage_verdicts(
    transaction: &mtxdb::DatabaseTransaction<'_>,
    rows: Vec<VerdictRow>,
) -> PyResult<()> {
    for (collection, pairs) in verdict_puts(rows)? {
        for (node, data) in pairs {
            transaction
                .put(ShardType::EventDag, collection, node, &data)
                .map_err(|e| map_transaction_io_error("stage verdict", e))?;
        }
    }
    Ok(())
}

/// The numeric code stored for a Synapse rejection reason. A reason mtxdb does
/// not know maps to its `Unknown` code, so the event still reads as rejected.
#[pyfunction]
pub fn event_rejection_code(reason: &str) -> u8 {
    RejectionCode::parse(reason).unwrap_or(RejectionCode::Unknown) as u8
}

/// Write (or replace) verdicts directly.
#[pyfunction]
pub fn event_verdict_put(py: Python<'_>, rows: Vec<VerdictRow>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| write_puts(verdict_puts(rows)?))
}

/// Tombstone the verdicts of deleted events.
#[pyfunction]
pub fn event_verdict_delete(py: Python<'_>, event_ids: Vec<String>) -> PyResult<()> {
    assert_writable()?;
    py.detach(|| {
        let mut tombstones = VerdictPuts::new();
        for event_id in event_ids {
            let node = event_record_id(event_id.as_bytes());
            tombstones
                .entry(event_verdict_collection_id(&node))
                .or_default()
                .push((node, NodeData::new(bytes::Bytes::new())));
        }
        write_puts(tombstones)
    })
}

/// Verdicts for `event_ids`, in input order; `None` for an event with no record.
#[pyfunction]
pub fn event_verdict_get(
    py: Python<'_>,
    event_ids: Vec<String>,
) -> PyResult<Vec<(String, Option<VerdictValue>)>> {
    py.detach(|| {
        let engine = event_dag_db()?;
        let nodes: Vec<NodeId> = event_ids
            .iter()
            .map(|id| event_record_id(id.as_bytes()))
            .collect();
        let mut by_collection: HashMap<[u8; 16], Vec<usize>> = HashMap::new();
        for (position, node) in nodes.iter().enumerate() {
            by_collection
                .entry(event_verdict_collection_id(node))
                .or_default()
                .push(position);
        }
        let mut values: Vec<Option<VerdictValue>> = vec![None; event_ids.len()];
        for (collection, positions) in by_collection {
            let wanted: Vec<NodeId> = positions.iter().map(|&p| nodes[p]).collect();
            let mut found = engine
                .get_read_committed(&collection, &wanted)
                .map_err(map_read_storage_error)?;
            if opened_read_only() && found.iter().any(|value| value.is_none()) {
                engine.refresh_collection(&collection).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "mtxdb refresh_collection error: {e}"
                    ))
                })?;
                found = engine
                    .get_read_committed(&collection, &wanted)
                    .map_err(map_read_storage_error)?;
            }
            for (position, value) in positions.into_iter().zip(found) {
                let Some(data) = value.filter(|data| !data.bytes.is_empty()) else {
                    continue;
                };
                let (stored_id, verdict) = decode_verdict(&data.bytes).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb verdict: {e}"))
                })?;
                // The record id is only 128 bits of the event id.
                if stored_id != event_ids[position].as_bytes() {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "mtxdb verdict id collision for {}",
                        event_ids[position]
                    )));
                }
                values[position] = Some((
                    verdict.outlier,
                    verdict.rejection,
                    verdict.soft_fail.is_some(),
                ));
            }
        }
        Ok(event_ids.into_iter().zip(values).collect())
    })
}
