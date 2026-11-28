//! Generic HAMT node-store logic backing the embedded single-process KV
//! engine ([`crate::database::mtxdb_syn`]). The backend implements only
//! [`NodeStore`] (a thin point-lookup/write surface over its own storage
//! primitive) and owns its own process-global handle + node cache; the BFS
//! materialize/selective-lookup walk, the node-cache verify-on-hit logic,
//! and the key-encoding scheme live here. Kept separate from `mtxdb_syn.rs`
//! (rather than folded together) to keep the BFS logic engine-agnostic.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use lru::LruCache;
use rezzy::hamt::{HamtNode, StructuralHash};

use crate::state_hamt::{
    decode_persisted_node_verified, lookup_from_node_map_preencoded, materialize_from_node_map,
};

/// Minimal point-lookup/write surface every embedded HAMT KV backend must
/// provide. Prefix scan / open / register-module stay backend-specific
/// (each engine module exposes its own `scan_prefix`, `open_client`, etc.)
/// since those aren't used by the generic BFS walk below.
pub trait NodeStore {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String>;
}

/// Fixed width of the room-scoped key prefix -- a locality hint, not an
/// identity: the full key is always `prefix || structural_hash`, which
/// stays unique regardless of prefix collisions.
pub const ROOM_PREFIX_LEN: usize = 8;

/// One materialized state group: `(event_type, state_key, event_id)` triples.
pub type StateEntries = Vec<(String, String, String)>;

pub type NodeLocation = ([u8; ROOM_PREFIX_LEN], Vec<u8>, StructuralHash);
pub type SelectiveQuery = (
    [u8; ROOM_PREFIX_LEN],
    StructuralHash,
    Vec<u8>,
    Vec<(String, String)>,
);

/// Process-wide in-memory cache of decoded HAMT nodes, keyed by their full
/// room-prefixed storage key. HAMT nodes are immutable and
/// content-addressed, so a cache hit is always correct modulo the
/// structural-hash check performed on every hit.
const NODE_CACHE_CAPACITY: usize = 100_000;
pub type NodeCache = Mutex<LruCache<Vec<u8>, Arc<HamtNode<String, String>>>>;

pub fn new_node_cache() -> NodeCache {
    Mutex::new(LruCache::new(
        NonZeroUsize::new(NODE_CACHE_CAPACITY).expect("cache capacity is nonzero"),
    ))
}

/// Room-prefixed HAMT node key:
/// `hamt:node:<room_prefix_hex>:<structural_hash_hex>`.
///
/// Unsalted: the room prefix plus the content-addressed structural hash is
/// already globally unique, and the physical record id is derived from the
/// same two components.
pub fn node_key(room_prefix: &[u8; ROOM_PREFIX_LEN], structural_hash: &StructuralHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(10 + ROOM_PREFIX_LEN * 2 + 1 + 32);
    key.extend_from_slice(b"hamt:node:");
    key.extend_from_slice(hex::encode(room_prefix).as_bytes());
    key.push(b':');
    key.extend_from_slice(hex::encode(structural_hash).as_bytes());
    key
}

/// HAMT root key: `hamt:root:<state_group>`.
///
/// Unsalted: a state-group id is unique inside the database the store
/// belongs to, which is the only scope a root key is ever used in.
pub fn root_key(state_group: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(10 + 20);
    key.extend_from_slice(b"hamt:root:");
    key.extend_from_slice(state_group.to_string().as_bytes());
    key
}

/// One decoded HAMT root record: `(room_prefix, root_structural_hash,
/// lattice, room_id)`. Mirrors `_decode_state_hamt_root`
/// (bg_updates.py) byte-for-byte -- the same v1 root record format
/// `_encode_state_hamt_root` writes.
pub struct RootRecord {
    pub room_prefix: Vec<u8>,
    pub root_hash: StructuralHash,
    pub lattice: Vec<u8>,
    pub room_id: String,
}

pub fn decode_root_value(value: &[u8]) -> Result<RootRecord, String> {
    if value.len() < 9 || value.get(..4) != Some(b"MTHR") || value[4] != 1 {
        return Err("invalid or unsupported HAMT root record version".to_owned());
    }
    let prefix_len = u16::from_be_bytes([value[5], value[6]]) as usize;
    let room_id_len_offset = 7 + prefix_len;
    if value.len() < room_id_len_offset + 2 {
        return Err("truncated HAMT root record".to_owned());
    }
    let room_id_len =
        u16::from_be_bytes([value[room_id_len_offset], value[room_id_len_offset + 1]]) as usize;
    let room_id_start = room_id_len_offset + 2;
    let root_start = room_id_start + room_id_len;
    if value.len() < root_start + 32 {
        return Err("truncated HAMT root record".to_owned());
    }
    let room_prefix = value[7..room_id_len_offset].to_vec();
    let room_id =
        String::from_utf8(value[room_id_start..root_start].to_vec()).map_err(|e| e.to_string())?;
    let root_hash: StructuralHash = value[root_start..root_start + 32]
        .try_into()
        .expect("slice is exactly 32 bytes");
    let lattice = value[root_start + 32..].to_vec();
    Ok(RootRecord {
        room_prefix,
        root_hash,
        lattice,
        room_id,
    })
}

/// Encodes a batch of `(structural_hash, node_bytes)` pairs (the shape
/// `state_hamt.build_root_handle_with_lattice`/`apply_flat_state_updates`
/// return) into `(node_key, node_bytes)` pairs ready for `batch_put` --
/// shared by both engines' `put_state_hamt_nodes` so callers never write
/// under a raw structural_hash key (which the BFS walk above can't find,
/// since it always looks up the room-prefixed key).
pub fn encode_node_writes(
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    nodes: Vec<(StructuralHash, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    nodes
        .into_iter()
        .map(|(hash, bytes)| (node_key(room_prefix, &hash), bytes))
        .collect()
}

/// Batch size while walking the HAMT -- bounds how much of the node cache's
/// lock is held at once per round.
const NODE_FETCH_BATCH_SIZE: usize = 100;

/// Fetch a state group's HAMT: BFS-fetch the reachable nodes (batched),
/// decode each one, and materialize `(event_type, state_key, event_id)`
/// triples -- without crossing back into Python per node.
pub fn materialize_state_hamt(
    store: &dyn NodeStore,
    cache: &NodeCache,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    root_structural_hash: StructuralHash,
    structural_key: &[u8],
) -> Result<StateEntries, String> {
    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);
    let mut to_fetch: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);

    while !to_fetch.is_empty() {
        let current_batch: Vec<StructuralHash> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = cache.lock().unwrap();
                for hash in chunk {
                    let key = node_key(room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *hash));
                            } else {
                                let node = node.clone();
                                for child in &node.children {
                                    let child_hash = child.structural_hash();
                                    if seen.insert(child_hash) {
                                        to_fetch.insert(child_hash);
                                    }
                                }
                                node_map.insert(*hash, node);
                            }
                        }
                        None => still_missing.push((key, *hash)),
                    }
                }
            }

            for (key, expected_hash) in still_missing {
                let node_bytes = store
                    .get_raw(&key)?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, structural_key, expected_hash)?;

                for child in &node.children {
                    let child_hash = child.structural_hash();
                    if seen.insert(child_hash) {
                        to_fetch.insert(child_hash);
                    }
                }

                cache.lock().unwrap().put(key, node.clone());
                node_map.insert(expected_hash, node);
            }
        }
    }

    materialize_from_node_map(&root_structural_hash, &node_map)
}

pub fn materialize_state_hamts(
    store: &dyn NodeStore,
    cache: &NodeCache,
    roots: Vec<NodeLocation>,
) -> Result<Vec<StateEntries>, String> {
    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = roots.iter().cloned().collect();
    let mut to_fetch = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = cache.lock().unwrap();
                for (room_prefix, structural_key, hash) in chunk {
                    let key = node_key(room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((
                                    key,
                                    *room_prefix,
                                    structural_key.clone(),
                                    *hash,
                                ));
                            } else {
                                let node = node.clone();
                                for child in &node.children {
                                    let child_location = (
                                        *room_prefix,
                                        structural_key.clone(),
                                        child.structural_hash(),
                                    );
                                    if seen.insert(child_location.clone()) {
                                        to_fetch.insert(child_location);
                                    }
                                }
                                node_map
                                    .insert((*room_prefix, structural_key.clone(), *hash), node);
                            }
                        }
                        None => {
                            still_missing.push((key, *room_prefix, structural_key.clone(), *hash));
                        }
                    }
                }
            }

            for (key, room_prefix, structural_key, expected_hash) in still_missing {
                let node_bytes = store
                    .get_raw(&key)?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, &structural_key, expected_hash)?;

                for child in &node.children {
                    let child_location =
                        (room_prefix, structural_key.clone(), child.structural_hash());
                    if seen.insert(child_location.clone()) {
                        to_fetch.insert(child_location);
                    }
                }

                cache.lock().unwrap().put(key, node.clone());
                node_map.insert((room_prefix, structural_key, expected_hash), node);
            }
        }
    }

    type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
    let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
    for ((room_prefix, _, hash), node) in node_map {
        nodes_by_prefix
            .entry(room_prefix)
            .or_default()
            .insert(hash, node);
    }

    roots
        .into_iter()
        .map(|(room_prefix, _, root_hash)| {
            let nodes = nodes_by_prefix.get(&room_prefix).ok_or_else(|| {
                format!(
                    "Missing nodes for room prefix: {}",
                    hex::encode(room_prefix)
                )
            })?;
            materialize_from_node_map(&root_hash, nodes)
        })
        .collect()
}

pub fn lookup_state_hamts(
    store: &dyn NodeStore,
    cache: &NodeCache,
    queries: Vec<SelectiveQuery>,
) -> Result<Vec<StateEntries>, String> {
    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = queries
        .iter()
        .map(|(p, h, k, _)| (*p, k.clone(), *h))
        .collect();
    let mut to_fetch: HashSet<NodeLocation> = seen.clone();
    let encoded_query_keys = queries
        .iter()
        .map(|(_, _, _, keys)| {
            keys.iter()
                .map(|(event_type, state_key)| {
                    serde_json::to_string(&(event_type, state_key))
                        .map_err(|e| format!("Failed to encode HAMT state key: {e}"))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Each loop's lookup results are valid iff it enqueued no further nodes.
    // Keep the latest result so that successful final round is returned
    // directly instead of traversing every query a second time below.
    let mut latest_entries = vec![None; queries.len()];

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = cache.lock().unwrap();
                for (room_prefix, structural_key, hash) in chunk {
                    let key = node_key(room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((
                                    key,
                                    *room_prefix,
                                    structural_key.clone(),
                                    *hash,
                                ));
                            } else {
                                node_map.insert(
                                    (*room_prefix, structural_key.clone(), *hash),
                                    node.clone(),
                                );
                            }
                        }
                        None => {
                            still_missing.push((key, *room_prefix, structural_key.clone(), *hash));
                        }
                    }
                }
            }

            for (key, room_prefix, structural_key, expected_hash) in still_missing {
                let node_bytes = store
                    .get_raw(&key)?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, &structural_key, expected_hash)?;

                cache.lock().unwrap().put(key, node.clone());
                node_map.insert((room_prefix, structural_key, expected_hash), node);
            }
        }

        type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
        let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
        for ((room_prefix, _, hash), node) in &node_map {
            nodes_by_prefix
                .entry(*room_prefix)
                .or_default()
                .insert(*hash, Arc::clone(node));
        }

        for (index, (room_prefix, root_hash, structural_key, keys)) in queries.iter().enumerate() {
            if let Some(prefix_nodes) = nodes_by_prefix.get(room_prefix) {
                if prefix_nodes.contains_key(root_hash) {
                    let (entries, missing) = lookup_from_node_map_preencoded(
                        root_hash,
                        structural_key,
                        keys,
                        &encoded_query_keys[index],
                        prefix_nodes,
                    )?;
                    latest_entries[index] = Some(entries);
                    for missing_hash in missing {
                        let child_loc = (*room_prefix, structural_key.clone(), missing_hash);
                        if seen.insert(child_loc.clone()) {
                            to_fetch.insert(child_loc);
                        }
                    }
                }
            }
        }
    }

    latest_entries
        .into_iter()
        .enumerate()
        .map(|(index, entries)| {
            entries.ok_or_else(|| {
                let (room_prefix, _, _, _) = &queries[index];
                format!(
                    "Missing nodes for room prefix: {}",
                    hex::encode(room_prefix)
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{lookup_state_hamts, new_node_cache, node_key, NodeStore};
    use crate::state_hamt::{
        build_root_handle_and_nodes, room_hamt_prefix_raw, room_structural_key_raw,
    };

    struct MemoryStore(HashMap<Vec<u8>, Vec<u8>>);

    impl NodeStore for MemoryStore {
        fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
            Ok(self.0.get(key).cloned())
        }
    }

    #[test]
    fn lookup_returns_entry_resolved_by_the_last_fetch_round() {
        let room_id = "!lookup:test.example";
        let entries = (0..1_000)
            .map(|index| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{index}:test.example"),
                    format!("${index}"),
                )
            })
            .collect();
        let ((root_hash, _), nodes) =
            build_root_handle_and_nodes(room_id, entries).expect("HAMT root should build");
        let room_prefix = room_hamt_prefix_raw(room_id, false).expect("valid room prefix");
        let structural_key = room_structural_key_raw(room_id);
        let store = MemoryStore(
            nodes
                .into_iter()
                .map(|(hash, bytes)| (node_key(&room_prefix, &hash), bytes))
                .collect(),
        );

        let result = lookup_state_hamts(
            &store,
            &new_node_cache(),
            vec![(
                room_prefix,
                root_hash,
                structural_key,
                vec![(
                    "m.room.member".to_owned(),
                    "@user-42:test.example".to_owned(),
                )],
            )],
        )
        .expect("lookup should resolve nodes fetched by the final round");

        assert_eq!(
            result,
            vec![vec![(
                "m.room.member".to_owned(),
                "@user-42:test.example".to_owned(),
                "$42".to_owned(),
            )]]
        );
    }
}
