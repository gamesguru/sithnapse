use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use mtxdb::{DatabaseLayout, NodeData, NodeId, PackfileStorage, ShardType, StorageEngine};
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use sha2::{Digest, Sha256};

use crate::database::core::{NodeStore, ROOM_PREFIX_LEN};

struct MtxdbPools {
    state: Arc<dyn StorageEngine>,
    event_dag: Arc<dyn StorageEngine>,
    auth_chain: Arc<dyn StorageEngine>,
}

static DBS: OnceCell<MtxdbPools> = OnceCell::new();
/// Serialize all read-modify-write cycles through the embedded engine.
/// The mtxdb `StorageEngine` trait has no atomic increment or transaction API,
/// so we hold this across get→put_many for counters and auth-chain manifests.
/// Only one SQL transaction runs at a time via the DB pool, so contention is
/// negligible.
static RMW_LOCK: Mutex<()> = Mutex::new(());

fn pools() -> PyResult<&'static MtxdbPools> {
    DBS.get()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mtxdb not opened"))
}

fn state_db() -> PyResult<&'static Arc<dyn StorageEngine>> {
    Ok(&pools()?.state)
}

fn event_dag_db() -> PyResult<&'static Arc<dyn StorageEngine>> {
    Ok(&pools()?.event_dag)
}

fn auth_chain_db() -> PyResult<&'static Arc<dyn StorageEngine>> {
    Ok(&pools()?.auth_chain)
}

fn shard_type_for_key(key: &[u8]) -> ShardType {
    if key.starts_with(b"event_json:") || key.starts_with(b"prev_event_edges:") {
        ShardType::EventDag
    } else {
        ShardType::State
    }
}

fn db_for_shard_type(shard_type: ShardType) -> PyResult<&'static Arc<dyn StorageEngine>> {
    match shard_type {
        ShardType::State => state_db(),
        ShardType::EventDag => event_dag_db(),
        ShardType::AuthChain => auth_chain_db(),
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

pub struct MtxdbStore {
    pub engine: Arc<dyn StorageEngine>,
}

impl NodeStore for MtxdbStore {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if let Some((room_prefix, structural_hash)) = parse_node_key(key) {
            let mut room_id = [0u8; 16];
            room_id[..ROOM_PREFIX_LEN].copy_from_slice(&room_prefix);

            let mut node_id = [0u8; 16];
            node_id.copy_from_slice(&structural_hash[..16]);

            let result = self
                .engine
                .get(&room_id, &node_id)
                .map_err(|e| e.to_string())?;
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
                .get(&room_id, &node_id)
                .map_err(|e| e.to_string())?;
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
// PyO3 Bindings
// -----------------------------------------------------------------------------

#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| {
        if DBS.get().is_some() {
            return Ok(());
        }
        let layout = DatabaseLayout::open(std::path::PathBuf::from(&path)).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to open mtxdb layout: {}", e))
        })?;
        let open_pool = |pool| {
            let path = layout.pool_dir(pool)?;
            PackfileStorage::open(path)
        };
        let state = Arc::new(open_pool(ShardType::State).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to open mtxdb state pool: {}",
                e
            ))
        })?);
        let event_dag = Arc::new(open_pool(ShardType::EventDag).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to open mtxdb event-dag pool: {}",
                e
            ))
        })?);
        let auth_chain = Arc::new(open_pool(ShardType::AuthChain).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to open mtxdb auth-chain pool: {}",
                e
            ))
        })?);
        let _ = DBS.set(MtxdbPools {
            state,
            event_dag,
            auth_chain,
        });
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
    let mut room_id = [0u8; 16];
    let prefix_len = std::cmp::min(room_prefix.len(), 16);
    room_id[..prefix_len].copy_from_slice(&room_prefix[..prefix_len]);

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

    py.detach(|| {
        let engine = state_db()?;
        engine.put_many(&room_id, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })
    })
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

        let results = engine.get_many(&room_id, &node_ids).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {}", e))
        })?;

        let mut out = Vec::with_capacity(chain_ids.len());
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
    let _guard = RMW_LOCK
        .lock()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e)))?;
    // No `py.detach` here — see increment_counters_batch's comment for why.
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
}

#[pyfunction]
pub fn delete_auth_chain_links_batch(namespace: String, pairs: Vec<(i64, i64)>) -> PyResult<()> {
    let _guard = RMW_LOCK
        .lock()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e)))?;
    // No `py.detach` here — see increment_counters_batch's comment for why.
    let engine = auth_chain_db()?;
    let room_id = namespace_room_id(&namespace);

    let mut grouped: HashMap<i64, HashSet<i64>> = HashMap::new();
    for (o_chain, o_seq) in pairs {
        grouped.entry(o_chain).or_default().insert(o_seq);
    }

    let mut pairs_to_put = Vec::with_capacity(grouped.len());

    for (chain_id, seqs_to_delete) in grouped {
        let node_id = chain_node_id(chain_id);
        if let Ok(Some(data)) = engine.get(&room_id, &node_id) {
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
}

// -----------------------------------------------------------------------------
// Generic KV (Event JSON / Event-to-State-Group)
// -----------------------------------------------------------------------------

fn kv_room_id() -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = 1; // Dedicated room_id for global flat KV data
    id
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
                ShardType::AuthChain => unreachable!("flat KV never routes to auth-chain"),
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
            let found = db_for_shard_type(shard_type)?
                .get_many(&kv_room_id(), &node_ids)
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {e}"))
                })?;
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

/// Store flat-KV records, routing each key to its shard type internally.
#[pyfunction]
pub fn batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    py.detach(|| {
        let mut state_puts = Vec::new();
        let mut event_puts = Vec::new();
        for (key, value) in pairs {
            let entry = (kv_node_id(&key), NodeData::new(bytes::Bytes::from(value)));
            match shard_type_for_key(&key) {
                ShardType::State => state_puts.push(entry),
                ShardType::EventDag => event_puts.push(entry),
                ShardType::AuthChain => unreachable!("flat KV never routes to auth-chain"),
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
    })
}

/// Tombstone flat-KV records in the shard type selected from each key.
#[pyfunction]
pub fn batch_delete(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<()> {
    let pairs = keys.into_iter().map(|key| (key, Vec::new())).collect();
    batch_put(py, pairs)
}

// -----------------------------------------------------------------------------
// HAMT Materialize / Lookup Wrappers
// -----------------------------------------------------------------------------

use rezzy::hamt::StructuralHash;

use crate::database::core::{self, NodeCache, StateEntries};
use crate::state_hamt::room_structural_key_raw;

static NODE_CACHE: OnceCell<NodeCache> = OnceCell::new();

fn node_cache() -> &'static NodeCache {
    NODE_CACHE.get_or_init(core::new_node_cache)
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

        core::materialize_state_hamt(
            &store,
            node_cache(),
            &namespace,
            &room_prefix,
            root_structural_hash,
            &structural_key,
        )
        .map(Some)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
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

        core::materialize_state_hamts(&store, node_cache(), &namespace, roots)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
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

        core::lookup_state_hamts(&store, node_cache(), &namespace, parsed_queries)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    })
}

pub type PyRootRecord = (i64, Vec<u8>, Vec<u8>, String, Vec<u8>);

#[pyfunction]
pub fn batch_get_state_hamt_roots(
    py: Python<'_>,
    namespace: String,
    groups: Vec<i64>,
) -> PyResult<Vec<Option<PyRootRecord>>> {
    py.detach(|| {
        let engine = state_db()?;
        let store = MtxdbStore {
            engine: Arc::clone(engine),
        };
        let records = core::batch_get_state_hamt_roots(&store, &namespace, &groups)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(groups
            .into_iter()
            .zip(records)
            .map(|(group, rec)| {
                rec.map(|r| {
                    let structural_hash_vec = r.root_hash.as_slice().to_vec();
                    let room_prefix_vec = r.room_prefix.to_vec();
                    (
                        group,
                        room_prefix_vec,
                        structural_hash_vec,
                        r.room_id,
                        r.lattice,
                    )
                })
            })
            .collect())
    })
}

#[pyfunction]
pub fn increment_counters_batch(pairs: Vec<(Vec<u8>, i64)>) -> PyResult<Vec<i64>> {
    let _guard = RMW_LOCK
        .lock()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e)))?;
    // No `py.detach` here: the RMW lock must be held across get→put_many,
    // and MutexGuard is !Send so it can't cross the Ungil boundary. The GIL
    // stays held for the duration (#[pyfunction] holds it by default), which
    // also serializes concurrent Python callers — so this is safe and fast
    // for local mmap I/O.
    let engine = state_db()?;
    let room_id = kv_room_id();
    let mut results = Vec::with_capacity(pairs.len());
    let mut puts = Vec::with_capacity(pairs.len());

    for (key, delta) in pairs {
        let node_id = kv_node_id(&key);
        let current = match engine.get(&room_id, &node_id) {
            Ok(Some(data)) => {
                if data.bytes.len() == 8 {
                    i64::from_be_bytes(data.bytes.as_ref().try_into().unwrap())
                } else {
                    0
                }
            }
            Ok(None) => 0,
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
    let mut room_id = [0u8; 16];
    let prefix_len = std::cmp::min(room_prefix.len(), 16);
    room_id[..prefix_len].copy_from_slice(&room_prefix[..prefix_len]);

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
        let results = engine.get_many(&room_id, &node_ids).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get error: {}", e))
        })?;
        Ok(results
            .into_iter()
            .map(|opt| opt.map(|d| d.bytes.to_vec()))
            .collect())
    })
}

#[pyfunction]
pub fn sync(py: Python<'_>) -> PyResult<()> {
    py.detach(|| {
        for (name, engine) in [
            ("state", state_db()?),
            ("event-dag", event_dag_db()?),
            ("auth-chain", auth_chain_db()?),
        ] {
            engine.sync().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "mtxdb sync error for {name} pool: {e}"
                ))
            })?;
        }
        Ok(())
    })
}

#[pyfunction]
pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open_client, m)?)?;
    m.add_function(wrap_pyfunction!(put_state_hamt_nodes, m)?)?;
    m.add_function(wrap_pyfunction!(get_state_hamt_nodes_batch, m)?)?;
    m.add_function(wrap_pyfunction!(get_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(put_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(delete_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(batch_get, m)?)?;
    m.add_function(wrap_pyfunction!(batch_put, m)?)?;
    m.add_function(wrap_pyfunction!(batch_delete, m)?)?;
    m.add_function(wrap_pyfunction!(materialize_state_hamt, m)?)?;
    m.add_function(wrap_pyfunction!(materialize_state_hamts, m)?)?;
    m.add_function(wrap_pyfunction!(lookup_state_hamts, m)?)?;
    m.add_function(wrap_pyfunction!(batch_get_state_hamt_roots, m)?)?;
    m.add_function(wrap_pyfunction!(increment_counters_batch, m)?)?;
    m.add_function(wrap_pyfunction!(sync, m)?)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.mtxdb_engine", m)?;
    Ok(())
}
