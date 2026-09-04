//! Generic HAMT node-store logic backing the embedded single-process KV
//! engine ([`crate::database::mdbx`]). The backend implements only
//! [`NodeStore`] (a thin point-lookup/write surface over its own storage
//! primitive) and owns its own process-global handle + node cache; the BFS
//! materialize/selective-lookup walk, the node-cache verify-on-hit logic,
//! and the key-encoding scheme live here. Kept separate from `mdbx.rs`
//! (rather than folded together) mainly because it was shared with a
//! second backend (fjall) that was benchmarked and dropped -- see
//! `database/mod.rs`'s doc comment.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use lru::LruCache;
use rezzy::hamt::{HamtNode, StructuralHash};
use sha2::{Digest, Sha256};

use crate::state_hamt::{
    decode_persisted_node_verified, lookup_from_node_map, materialize_from_node_map,
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

pub type NodeLocation = ([u8; ROOM_PREFIX_LEN], [u8; 32], StructuralHash);
pub type SelectiveQuery = (
    [u8; ROOM_PREFIX_LEN],
    StructuralHash,
    [u8; 32],
    Vec<(String, String)>,
);

/// Process-wide in-memory cache of decoded HAMT nodes, keyed by their full
/// (namespaced, room-prefixed) storage key. HAMT nodes are immutable and
/// content-addressed, so a cache hit is always correct modulo the
/// structural-hash check performed on every hit.
const NODE_CACHE_CAPACITY: usize = 100_000;
pub type NodeCache = Mutex<LruCache<Vec<u8>, Arc<HamtNode<String, String>>>>;

pub fn new_node_cache() -> NodeCache {
    Mutex::new(LruCache::new(
        NonZeroUsize::new(NODE_CACHE_CAPACITY).expect("cache capacity is nonzero"),
    ))
}

/// Namespaced, room-prefixed HAMT node key:
/// `hamt:node:<namespace_hash>:<room_prefix_hex>:<structural_hash_hex>`.
pub fn node_key(
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    structural_hash: &StructuralHash,
) -> Vec<u8> {
    let namespace_hash = Sha256::digest(namespace.as_bytes());
    let mut key = Vec::with_capacity(10 + 32 + 1 + ROOM_PREFIX_LEN * 2 + 1 + 32);
    key.extend_from_slice(b"hamt:node:");
    key.extend_from_slice(hex::encode(&namespace_hash[..16]).as_bytes());
    key.push(b':');
    key.extend_from_slice(hex::encode(room_prefix).as_bytes());
    key.push(b':');
    key.extend_from_slice(hex::encode(structural_hash).as_bytes());
    key
}

/// Per-namespace HAMT root key: `hamt:root:<namespace_hash_hex[..16]><state_group>`.
/// Must match `_state_hamt_root_key` in `synapse/storage/databases/
/// state/bg_updates.py` byte-for-byte.
pub fn root_key(namespace: &str, state_group: i64) -> Vec<u8> {
    let namespace_hash = Sha256::digest(namespace.as_bytes());
    let mut key = Vec::with_capacity(10 + 32 + 20);
    key.extend_from_slice(b"hamt:root:");
    key.extend_from_slice(hex::encode(&namespace_hash[..16]).as_bytes());
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
    if value.len() < 5 || value[0] != 1 {
        return Err("invalid or unsupported HAMT root record version".to_owned());
    }
    let prefix_len = u16::from_be_bytes([value[1], value[2]]) as usize;
    let room_id_len_offset = 3 + prefix_len;
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
    let room_prefix = value[3..room_id_len_offset].to_vec();
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

/// Namespaced key prefix covering every outgoing edge of one auth chain:
/// `auth_chain_link:<namespace_hash_hex[..16]>:<origin_chain_id_be>`. The
/// only definition of this layout -- unlike the HAMT node/root keys,
/// there's no separate Python-side encoder to keep in sync with, since
/// `embedded_event_auth_chain_links.py` is a thin pass-through to the
/// `mdbx_engine` functions built from these.
pub fn auth_chain_prefix(namespace: &str, origin_chain_id: i64) -> Vec<u8> {
    let namespace_hash = Sha256::digest(namespace.as_bytes());
    let mut key = Vec::with_capacity(17 + 32 + 1 + 8);
    key.extend_from_slice(b"auth_chain_link:");
    key.extend_from_slice(hex::encode(&namespace_hash[..16]).as_bytes());
    key.push(b':');
    key.extend_from_slice(&origin_chain_id.to_be_bytes());
    key
}

/// Narrower prefix covering only edges from one `(origin_chain_id,
/// origin_sequence_number)` pair -- what purge deletes by.
pub fn auth_chain_origin_seq_prefix(
    namespace: &str,
    origin_chain_id: i64,
    origin_sequence_number: i64,
) -> Vec<u8> {
    let mut key = auth_chain_prefix(namespace, origin_chain_id);
    key.push(b':');
    key.extend_from_slice(&origin_sequence_number.to_be_bytes());
    key
}

/// Full edge key: `<origin_seq_prefix>:<target_chain_id_be><target_seq_be>`.
pub fn auth_chain_link_key(
    namespace: &str,
    origin_chain_id: i64,
    origin_sequence_number: i64,
    target_chain_id: i64,
    target_sequence_number: i64,
) -> Vec<u8> {
    let mut key = auth_chain_origin_seq_prefix(namespace, origin_chain_id, origin_sequence_number);
    key.push(b':');
    key.extend_from_slice(&target_chain_id.to_be_bytes());
    key.extend_from_slice(&target_sequence_number.to_be_bytes());
    key
}

/// Decodes the `(origin_sequence_number, target_chain_id,
/// target_sequence_number)` suffix of an edge key, given the chain-level
/// prefix (`auth_chain_prefix`'s output) it was scanned under.
pub fn decode_auth_chain_link_suffix(key: &[u8], prefix: &[u8]) -> Result<(i64, i64, i64), String> {
    let suffix = key
        .get(prefix.len()..)
        .ok_or_else(|| "auth chain link key shorter than its own prefix".to_owned())?;
    // suffix is b":" + 8 bytes origin_seq + b":" + 8 bytes target_chain_id + 8 bytes target_seq
    if suffix.len() != 1 + 8 + 1 + 8 + 8 {
        return Err("malformed auth chain link key suffix".to_owned());
    }
    let origin_sequence_number = i64::from_be_bytes(suffix[1..9].try_into().unwrap());
    let target_chain_id = i64::from_be_bytes(suffix[10..18].try_into().unwrap());
    let target_sequence_number = i64::from_be_bytes(suffix[18..26].try_into().unwrap());
    Ok((
        origin_sequence_number,
        target_chain_id,
        target_sequence_number,
    ))
}

/// Batched root lookup: one call in from Python instead of an N-iteration
/// `for` loop each doing its own FFI round trip. Returns `None` per group
/// that has no root record in this engine (the caller falls back to SQL
/// for those, same self-healing shape as node reads).
pub fn batch_get_state_hamt_roots(
    store: &dyn NodeStore,
    namespace: &str,
    groups: &[i64],
) -> Result<Vec<Option<RootRecord>>, String> {
    groups
        .iter()
        .map(|&group| match store.get_raw(&root_key(namespace, group))? {
            Some(value) => Ok(Some(decode_root_value(&value)?)),
            None => Ok(None),
        })
        .collect()
}

/// Encodes a batch of `(structural_hash, node_bytes)` pairs (the shape
/// `state_hamt.build_root_handle_with_lattice`/`apply_flat_state_updates`
/// return) into `(node_key, node_bytes)` pairs ready for `batch_put` --
/// shared by both engines' `put_state_hamt_nodes` so callers never write
/// under a raw structural_hash key (which the BFS walk above can't find,
/// since it always looks up the namespaced/room-prefixed key).
pub fn encode_node_writes(
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    nodes: Vec<(StructuralHash, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    nodes
        .into_iter()
        .map(|(hash, bytes)| (node_key(namespace, room_prefix, &hash), bytes))
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
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    root_structural_hash: StructuralHash,
    structural_key: &[u8; 32],
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
                    let key = node_key(namespace, room_prefix, hash);
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
    namespace: &str,
    roots: Vec<NodeLocation>,
) -> Result<Vec<StateEntries>, String> {
    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = roots.iter().copied().collect();
    let mut to_fetch = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = cache.lock().unwrap();
                for (room_prefix, structural_key, hash) in chunk {
                    let key = node_key(namespace, room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *room_prefix, *structural_key, *hash));
                            } else {
                                let node = node.clone();
                                for child in &node.children {
                                    let child_location =
                                        (*room_prefix, *structural_key, child.structural_hash());
                                    if seen.insert(child_location) {
                                        to_fetch.insert(child_location);
                                    }
                                }
                                node_map.insert((*room_prefix, *structural_key, *hash), node);
                            }
                        }
                        None => still_missing.push((key, *room_prefix, *structural_key, *hash)),
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
                    let child_location = (room_prefix, structural_key, child.structural_hash());
                    if seen.insert(child_location) {
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
    namespace: &str,
    queries: Vec<SelectiveQuery>,
) -> Result<Vec<StateEntries>, String> {
    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = queries.iter().map(|(p, h, k, _)| (*p, *k, *h)).collect();
    let mut to_fetch: HashSet<NodeLocation> = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = cache.lock().unwrap();
                for (room_prefix, structural_key, hash) in chunk {
                    let key = node_key(namespace, room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *room_prefix, *structural_key, *hash));
                            } else {
                                node_map
                                    .insert((*room_prefix, *structural_key, *hash), node.clone());
                            }
                        }
                        None => still_missing.push((key, *room_prefix, *structural_key, *hash)),
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

        for (room_prefix, root_hash, structural_key, keys) in &queries {
            if let Some(prefix_nodes) = nodes_by_prefix.get(room_prefix) {
                if prefix_nodes.contains_key(root_hash) {
                    let (_entries, missing) =
                        lookup_from_node_map(root_hash, structural_key, keys, prefix_nodes)?;
                    for missing_hash in missing {
                        let child_loc = (*room_prefix, *structural_key, missing_hash);
                        if seen.insert(child_loc) {
                            to_fetch.insert(child_loc);
                        }
                    }
                }
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

    queries
        .into_iter()
        .map(|(room_prefix, root_hash, structural_key, keys)| {
            let prefix_nodes = nodes_by_prefix.get(&room_prefix).ok_or_else(|| {
                format!(
                    "Missing nodes for room prefix: {}",
                    hex::encode(room_prefix)
                )
            })?;
            let (entries, missing) =
                lookup_from_node_map(&root_hash, &structural_key, &keys, prefix_nodes)?;
            if !missing.is_empty() {
                return Err(format!(
                    "Unresolved missing nodes after fetch loop for root {:02x?}",
                    root_hash
                ));
            }
            Ok(entries)
        })
        .collect()
}
