/*
 * This file is licensed under the Affero General Public License (AGPL) version 3.
 *
 * Copyright (C) 2026 Element Creations Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * See the GNU Affero General Public License for more details:
 * <https://www.gnu.org/licenses/agpl-3.0.html>.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use hmac::{Hmac, Mac};
use pyo3::prelude::*;
use pyo3::types::{PyModule, PyModuleMethods};
use rezzy::{
    hamt::{HamtNode, NodeRef, PersistedInternalNode, RootHandle, StateGroupId, StructuralHash},
    LtHash,
};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;
type RootHandleParts = ([u8; 32], [u8; 32]);
type PersistedNodeBytes = (StructuralHash, Vec<u8>);
type BuiltRoot = (RootHandleParts, Vec<PersistedNodeBytes>);
type PyRootHandleParts = (Vec<u8>, Vec<u8>);
type PyPersistedNodeBytes = (Vec<u8>, Vec<u8>);
type PyBuiltRoot = (PyRootHandleParts, Vec<PyPersistedNodeBytes>);
type PyTypedRootDirEntry = (String, Vec<u8>);
type PyBuiltTypedRoot = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<PyPersistedNodeBytes>);
type PyDecodedTypedRoot = (Vec<u8>, Vec<u8>, Vec<PyTypedRootDirEntry>);
type PyBuiltRootWithLattice = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<PyPersistedNodeBytes>);
type PyStateUpdate = (String, String, Option<String>);
type PyAppliedStateUpdate = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<PyPersistedNodeBytes>);
type PyApplyOutcome = (Option<PyAppliedStateUpdate>, Vec<Vec<u8>>);
type PyBuiltTypedRootWithLattice = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<PyPersistedNodeBytes>,
);
type PyApplyTypedOutcome = (Option<PyAppliedStateUpdate>, Vec<Vec<u8>>);
type PyStateEntry = (String, String, String);
type PyReachabilityAudit = (Vec<Vec<u8>>, Vec<Vec<u8>>);
type PyStateLookup = (Vec<PyStateEntry>, Vec<Vec<u8>>);

const TYPED_ROOT_FORMAT: u8 = 0x02;

/// The compact directory at the root of a typed state HAMT. The directory is
/// sorted by event type and points at one state_key -> event_id HAMT per type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TypedRoot {
    /// Local structural identity of the typed directory itself (room-keyed,
    /// derived from the public room ID via HMAC-SHA256, used only to skip an
    /// unchanged directory across reloads — never compared across servers and
    /// never used as the state-group identity).
    pub structural_hash: StructuralHash,
    /// The cross-server, deduplicable state-group identifier: the unkeyed
    /// `LtHash` digest over the same logical `(event_type, state_key,
    /// event_id)` entries the flat HAMT root would hash, exactly as
    /// `RootHandle::state_group_id` does for the flat path. This MUST NOT be
    /// derived from the keyed `structural_hash` of subtrees.
    pub state_group_id: StateGroupId,
    pub directory: Vec<(String, StructuralHash)>,
}

impl TypedRoot {
    pub(crate) fn encode_v1(&self) -> Result<Vec<u8>, String> {
        let count = u16::try_from(self.directory.len())
            .map_err(|_| "typed root has too many event types".to_owned())?;
        let mut bytes = Vec::new();
        bytes.push(TYPED_ROOT_FORMAT);
        bytes.extend_from_slice(&self.structural_hash);
        bytes.extend_from_slice(&self.state_group_id);
        bytes.extend_from_slice(&count.to_le_bytes());
        for (event_type, hash) in &self.directory {
            let event_type_bytes = event_type.as_bytes();
            let len = u16::try_from(event_type_bytes.len())
                .map_err(|_| "event type is too long for typed root".to_owned())?;
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(event_type_bytes);
            bytes.extend_from_slice(hash);
        }
        Ok(bytes)
    }

    pub(crate) fn decode_v1(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = 0usize;
        let take = |cursor: &mut usize, count: usize| -> Result<&[u8], String> {
            let end = cursor
                .checked_add(count)
                .ok_or_else(|| "typed root length overflow".to_owned())?;
            let value = bytes
                .get(*cursor..end)
                .ok_or_else(|| "truncated typed root".to_owned())?;
            *cursor = end;
            Ok(value)
        };
        if take(&mut cursor, 1)?[0] != TYPED_ROOT_FORMAT {
            return Err("not a typed HAMT root".to_owned());
        }
        let structural_hash: StructuralHash = take(&mut cursor, 32)?.try_into().unwrap();
        let state_group_id: StateGroupId = take(&mut cursor, 32)?.try_into().unwrap();
        let count = u16::from_le_bytes(take(&mut cursor, 2)?.try_into().unwrap());
        let mut directory = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let len = u16::from_le_bytes(take(&mut cursor, 2)?.try_into().unwrap());
            let event_type = std::str::from_utf8(take(&mut cursor, len as usize)?)
                .map_err(|_| "typed root event type is not UTF-8".to_owned())?
                .to_owned();
            let hash: StructuralHash = take(&mut cursor, 32)?.try_into().unwrap();
            directory.push((event_type, hash));
        }
        if cursor != bytes.len() {
            return Err("trailing bytes in typed root".to_owned());
        }
        if directory.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err("typed root directory is not strictly sorted".to_owned());
        }
        Ok(Self {
            structural_hash,
            state_group_id,
            directory,
        })
    }
}

fn typed_subtree_key(room_key: &[u8; 32], event_type: &str) -> [u8; 32] {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(room_key).expect("HMAC key is valid");
    mac.update(b"typed-state-subtree:");
    mac.update(event_type.as_bytes());
    mac.finalize().into_bytes().into()
}

fn typed_root_hash(room_key: &[u8; 32], directory: &[(String, StructuralHash)]) -> StructuralHash {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(room_key).expect("HMAC key is valid");
    mac.update(b"typed-state-root:");
    for (event_type, hash) in directory {
        mac.update(&(event_type.len() as u32).to_le_bytes());
        mac.update(event_type.as_bytes());
        mac.update(hash);
    }
    let digest = mac.finalize().into_bytes();
    digest.into()
}

fn build_typed_root_and_nodes(
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> Result<(TypedRoot, Vec<PersistedNodeBytes>), String> {
    let (root, _lattice, nodes) = build_typed_root_nodes_and_lattice(room_id, entries)?;
    Ok((root, nodes))
}

/// Same as [`build_typed_root_and_nodes`], but also returns the full,
/// retained `LtHash` lattice — not just its collapsed `state_group_id`
/// digest — so a caller can later apply incremental updates against this
/// typed root via [`apply_typed_state_updates_impl`], mirroring
/// [`build_root_handle_nodes_and_lattice`] for the flat path.
fn build_typed_root_nodes_and_lattice(
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> Result<(TypedRoot, LtHash, Vec<PersistedNodeBytes>), String> {
    let room_key = room_structural_key_raw(room_id);
    let mut by_type: std::collections::BTreeMap<String, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    // The state-group identity is the unkeyed LtHash lattice over every
    // logical (event_type, state_key, event_id) entry — this must match the
    // flat path bit-for-bit (see build_root_handle_and_nodes), since LtHash's
    // addition is commutative/associative: summing it here, partitioned by
    // type, is provably identical to summing it there over the flat list, as
    // long as every entry contributes exactly once via the same `insert()`
    // encoding. It is NOT derived from the (keyed) subtree structural hashes.
    let mut lattice = LtHash::default();
    for (event_type, state_key, event_id) in &entries {
        lattice.insert(event_type, state_key, event_id);
    }
    for (event_type, state_key, event_id) in entries {
        by_type
            .entry(event_type)
            .or_default()
            .push((state_key, event_id));
    }

    let mut directory = Vec::with_capacity(by_type.len());
    let mut nodes = Vec::new();
    let mut seen = HashSet::new();
    for (event_type, type_entries) in by_type {
        let subtree_key = typed_subtree_key(&room_key, &event_type);
        let subtree = rezzy::hamt::build_hamt(&subtree_key, type_entries)
            .map_err(|e| format!("Failed to build typed HAMT subtree: {e:?}"))?;
        let subtree_hash = subtree.structural_hash;
        collect_persisted_nodes(subtree, &mut seen, &mut nodes);
        directory.push((event_type, subtree_hash));
    }
    let root = TypedRoot {
        structural_hash: typed_root_hash(&room_key, &directory),
        state_group_id: rezzy::hamt::state_group_id_from_lthash(&lattice),
        directory,
    };
    Ok((root, lattice, nodes))
}

#[must_use]
pub fn room_structural_key_raw(room_id: &str) -> [u8; 32] {
    let hash = Sha256::digest(room_id.as_bytes());
    hash.into()
}

/// Derive a fixed-width, room-scoped prefix used to lay out this room's HAMT
/// nodes contiguously in the embedded engine's flat sorted keyspace (see
/// `database/core.rs`'s `node_key`). Despite the historical name this isn't
/// TiKV-specific -- mdbx uses the exact same scheme (see `mdbx.rs`).
///
/// For MSC4291-style room versions the room ID *is* `!` + base64url(hash(create
/// event)) -- already a uniformly-distributed digest -- so we decode it directly
/// rather than hashing it again. For pre-MSC4291 versions the room ID is an
/// opaque, low-entropy string (`!<random localpart>:<server_name>`), so we
/// SHA-256 it and truncate the digest.
///
/// Callers must pass the real `room_version.msc4291_room_ids_as_hashes` flag
/// (the official per-room-version marker) rather than comparing version
/// numbers, since which versions get hash-based room IDs is not simply "v12
/// and above" (e.g. experimental/Hydra versions).
pub fn room_hamt_prefix_raw(
    room_id: &str,
    msc4291_room_ids_as_hashes: bool,
) -> Result<[u8; 8], String> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    const PREFIX_LEN: usize = 8;
    let mut prefix = [0u8; PREFIX_LEN];

    if msc4291_room_ids_as_hashes {
        // MSC4291 room IDs are `!` + base64url(hash) with no `:server_name`
        // suffix, but guard against one anyway (e.g. a future format change)
        // by decoding only the part before any colon.
        let body = room_id
            .strip_prefix('!')
            .unwrap_or(room_id)
            .split(':')
            .next()
            .unwrap_or("");
        let decoded = URL_SAFE_NO_PAD
            .decode(body)
            .map_err(|e| format!("Failed to decode MSC4291 room id as base64url: {e}"))?;
        if decoded.len() < PREFIX_LEN {
            return Err(format!(
                "Decoded MSC4291 room id hash is only {} bytes, expected at least {}",
                decoded.len(),
                PREFIX_LEN
            ));
        }
        prefix.copy_from_slice(&decoded[..PREFIX_LEN]);
    } else {
        let full_key = room_structural_key_raw(room_id);
        prefix.copy_from_slice(&full_key[..PREFIX_LEN]);
    }

    Ok(prefix)
}

fn root_handle_parts(root_handle: &RootHandle) -> RootHandleParts {
    (root_handle.structural_hash, root_handle.state_group_id)
}

fn collect_persisted_nodes(
    node: Arc<HamtNode<String, String>>,
    seen: &mut HashSet<StructuralHash>,
    nodes: &mut Vec<PersistedNodeBytes>,
) {
    if !seen.insert(node.structural_hash) {
        return;
    }

    for child in &node.children {
        if let NodeRef::Resolved(child_node) = child {
            collect_persisted_nodes(child_node.clone(), seen, nodes);
        }
    }

    let persisted: PersistedInternalNode<String, String> = node.as_ref().into();
    nodes.push((node.structural_hash, persisted.encode_v1()));
}

pub(crate) fn build_root_handle_and_nodes(
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> Result<BuiltRoot, String> {
    let (root_handle, lattice, nodes) = build_root_handle_nodes_and_lattice(room_id, entries)?;
    let _ = lattice;
    Ok((root_handle_parts(&root_handle), nodes))
}

/// Same as [`build_root_handle_and_nodes`], but also returns the full,
/// retained `LtHash` lattice — not just its collapsed `state_group_id`
/// digest — so a caller can later apply incremental updates against this
/// root via [`apply_flat_state_updates_impl`] without recomputing the
/// lattice from scratch. See docs/development-gg/persistent-typed-hamt-architecture.md.
fn build_root_handle_nodes_and_lattice(
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> Result<(RootHandle, LtHash, Vec<PersistedNodeBytes>), String> {
    let structural_key = room_structural_key_raw(room_id);
    let mut lattice = LtHash::default();
    for (event_type, state_key, event_id) in &entries {
        lattice.insert(event_type, state_key, event_id);
    }
    let entries = entries
        .into_iter()
        .map(|(event_type, state_key, event_id)| {
            (
                serde_json::to_string(&(event_type, state_key))
                    .expect("state key serialization should not fail"),
                event_id,
            )
        });
    let (root_handle, root_node) =
        rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
            .map_err(|e| format!("Failed to build HAMT root: {e:?}"))?;

    let mut seen = HashSet::new();
    let mut nodes = Vec::new();
    collect_persisted_nodes(root_node, &mut seen, &mut nodes);

    Ok((root_handle, lattice, nodes))
}

/// Encodes an `LtHash` lattice as 2048 little-endian bytes (1024 u16 lanes).
/// This is our own persistence format for the *retained* lattice — distinct
/// from `LtHash::digest()`, which collapses it to the 32-byte
/// `state_group_id`. The retained bytes are what a caller must pass back
/// into [`apply_flat_state_updates_impl`] to apply further incremental
/// updates; the digest alone cannot be "un-collapsed".
fn lattice_to_bytes(lattice: &LtHash) -> Vec<u8> {
    let mut out = Vec::with_capacity(2048);
    for lane in lattice.0.iter() {
        out.extend_from_slice(&lane.to_le_bytes());
    }
    out
}

fn lattice_from_bytes(bytes: &[u8]) -> Result<LtHash, String> {
    if bytes.len() != 2048 {
        return Err(format!(
            "LtHash lattice must be exactly 2048 bytes, got {}",
            bytes.len()
        ));
    }
    let mut lanes = [0u16; 1024];
    for (lane, chunk) in lanes.iter_mut().zip(bytes.chunks_exact(2)) {
        *lane = u16::from_le_bytes([chunk[0], chunk[1]]);
    }
    Ok(LtHash(lanes))
}

/// Like [`collect_persisted_nodes`], but skips any subtree whose structural
/// hash is already in `known` — i.e. already durable, from the caller's
/// perspective — without even recursing into its children. Because node
/// identity is content-addressed, an unchanged subtree keeps its old hash,
/// so this is exactly the set of nodes an incremental update actually needs
/// to write: the O(changed-path) node set, not the whole reachable tree.
fn collect_new_persisted_nodes(
    node: Arc<HamtNode<String, String>>,
    known: &HashSet<StructuralHash>,
    seen: &mut HashSet<StructuralHash>,
    nodes: &mut Vec<PersistedNodeBytes>,
) {
    if known.contains(&node.structural_hash) {
        return;
    }
    if !seen.insert(node.structural_hash) {
        return;
    }

    for child in &node.children {
        if let NodeRef::Resolved(child_node) = child {
            collect_new_persisted_nodes(child_node.clone(), known, seen, nodes);
        }
    }

    let persisted: PersistedInternalNode<String, String> = node.as_ref().into();
    nodes.push((node.structural_hash, persisted.encode_v1()));
}

fn structural_hash_from_slice(hash_bytes: &[u8]) -> Result<StructuralHash, String> {
    hash_bytes
        .try_into()
        .map_err(|_| "structural hash must be 32 bytes".to_owned())
}

/// The result of applying one or more single-key changes to an existing
/// flat HAMT root, without rebuilding or even fully materializing the tree.
///
/// This is the "load-bearing piece" from
/// docs/development-gg/persistent-typed-hamt-architecture.md: a normal state
/// PDU changes one `(event_type, state_key)` entry, and applying it here
/// costs O(log₃₂ S) path-copied nodes, not O(S) full-map reconstruction.
struct AppliedStateUpdate {
    structural_hash: StructuralHash,
    state_group_id: StateGroupId,
    lattice_bytes: Vec<u8>,
    /// Only the newly created nodes — i.e. excluding anything already
    /// present in the `nodes` the caller supplied. See
    /// [`collect_new_persisted_nodes`].
    new_nodes: Vec<PersistedNodeBytes>,
}

/// Outcome of [`apply_flat_state_updates_impl`]: either the update fully
/// applied, or it hit one or more resolver misses along the way — the
/// caller didn't supply enough already-fetched path nodes. A miss is not an
/// error: it mirrors the existing `lookup_state_entries` contract (see
/// `lookup_from_node_map`) so callers can reuse the same "fetch the missing
/// hashes, retry" loop already used for reads
/// (`_lookup_state_hamt_from_postgres_txn` et al.), discovering one more
/// tree level's worth of missing hashes per retry. Nothing is partially
/// applied either way, since nothing is written to `root` until every
/// update in the batch has resolved successfully.
enum ApplyOutcome {
    Applied(AppliedStateUpdate),
    Missing(HashSet<StructuralHash>),
}

/// Applies a batch of single-key changes to an existing flat HAMT root.
///
/// `root_node_bytes` is the current root node. `nodes` are any additional
/// already-persisted nodes the caller has fetched along the path(s) to the
/// keys being changed — callers are expected to fetch only what's needed for
/// the specific keys in `updates`, not the whole tree.
///
/// `lattice_bytes` is the *retained* 2048-byte `LtHash` for the current
/// root (see [`lattice_to_bytes`]) — not its collapsed `state_group_id` — so
/// the new lattice can be derived homomorphically (subtract the displaced
/// value, add the new one) rather than recomputed from a full entry scan.
///
/// `updates` is `(event_type, state_key, new_event_id)`, where
/// `new_event_id: None` means "remove this key".
fn apply_flat_state_updates_impl(
    room_id: &str,
    root_node_bytes: &[u8],
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    lattice_bytes: &[u8],
    updates: Vec<(String, String, Option<String>)>,
) -> Result<ApplyOutcome, String> {
    let structural_key = room_structural_key_raw(room_id);
    let mut lattice = lattice_from_bytes(lattice_bytes)?;

    let root_node = decode_persisted_node_with_key(root_node_bytes, &structural_key)?;
    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    node_map.insert(root_node.structural_hash, root_node.clone());
    for (hash_bytes, node_bytes) in nodes {
        let hash = structural_hash_from_slice(&hash_bytes)?;
        let node = decode_persisted_node_verified(&node_bytes, &structural_key, hash)?;
        node_map.insert(hash, node);
    }
    // Everything supplied by the caller is, by definition, already durable —
    // this is the boundary collect_new_persisted_nodes uses to know what NOT
    // to re-persist.
    let known: HashSet<StructuralHash> = node_map.keys().copied().collect();

    let mut root = root_node;
    for (event_type, state_key, new_event_id) in updates {
        let key = serde_json::to_string(&(&event_type, &state_key))
            .map_err(|e| format!("Failed to encode HAMT state key: {e}"))?;
        let mut resolver =
            |hash: &StructuralHash| -> Result<Arc<HamtNode<String, String>>, StructuralHash> {
                node_map.get(hash).cloned().ok_or(*hash)
            };

        macro_rules! handle_mutate_err {
            ($result:expr) => {
                match $result {
                    Ok(pair) => pair,
                    Err(rezzy::hamt::HamtMutateError::Resolve(missing_hash)) => {
                        return Ok(ApplyOutcome::Missing(HashSet::from([missing_hash])));
                    }
                    Err(rezzy::hamt::HamtMutateError::HashCollision { depth, bucket_size }) => {
                        return Err(format!(
                            "hash collision at depth {depth} with bucket size {bucket_size}"
                        ));
                    }
                    Err(rezzy::hamt::HamtMutateError::MaxDepthExceeded { depth }) => {
                        return Err(format!("HAMT maximum depth exceeded at depth {depth}"));
                    }
                }
            };
        }

        match new_event_id {
            Some(new_id) => {
                let (new_root, old_value) = handle_mutate_err!(rezzy::hamt::insert(
                    &root,
                    &structural_key,
                    key,
                    new_id.clone(),
                    &mut resolver
                ));
                match old_value {
                    Some(old) => lattice.replace(&event_type, &state_key, &old, &new_id),
                    None => lattice.insert(&event_type, &state_key, &new_id),
                }
                root = new_root;
            }
            None => {
                let (new_root, old_value) = handle_mutate_err!(rezzy::hamt::remove(
                    &root,
                    &structural_key,
                    key.as_str(),
                    &mut resolver
                ));
                if let Some(old) = old_value {
                    lattice.remove(&event_type, &state_key, &old);
                }
                root = new_root;
            }
        }
        // Register the freshly minted root so later updates in this same
        // batch can resolve through it without re-fetching anything.
        node_map.insert(root.structural_hash, root.clone());
    }

    let mut seen = HashSet::new();
    let mut new_nodes = Vec::new();
    collect_new_persisted_nodes(root.clone(), &known, &mut seen, &mut new_nodes);

    Ok(ApplyOutcome::Applied(AppliedStateUpdate {
        structural_hash: root.structural_hash,
        state_group_id: rezzy::hamt::state_group_id_from_lthash(&lattice),
        lattice_bytes: lattice_to_bytes(&lattice),
        new_nodes,
    }))
}

/// Result of applying updates to a typed root: the re-encoded `TypedRoot`
/// bytes, its (recomputed) `state_group_id`, the updated retained lattice,
/// and only the newly created subtree nodes.
struct AppliedTypedStateUpdate {
    typed_root_bytes: Vec<u8>,
    state_group_id: StateGroupId,
    lattice_bytes: Vec<u8>,
    new_nodes: Vec<PersistedNodeBytes>,
}

/// Same [`ApplyOutcome`] split as the flat path: a resolver miss is
/// retryable data, not an error.
enum ApplyTypedOutcome {
    Applied(AppliedTypedStateUpdate),
    Missing(HashSet<StructuralHash>),
}

/// Applies a batch of single-key changes to an existing typed root by
/// updating only the touched event types' subtrees via O(log₃₂ S_T)
/// path-copying, and touching only their directory entries — not by
/// rebuilding the whole typed structure from a full entry list. This is the
/// typed-root analogue of [`apply_flat_state_updates_impl`]; doing anything
/// less here (e.g. calling `build_typed_root` fresh from a materialized
/// entry list on every update) would reintroduce the same O(S)-per-PDU tax
/// this whole design exists to eliminate, just on the typed side instead of
/// the flat side.
///
/// `nodes` must include, for every event type touched by `updates`, the
/// nodes along the path(s) to the changed keys within that type's subtree
/// (the subtree root at minimum) — not the whole typed structure. A type
/// with no existing subtree (its first-ever entry) needs no prior nodes:
/// `rezzy::hamt::build_hamt` with an empty entry list produces a valid
/// empty root to insert into, so a brand-new event type is not a special
/// case requiring a different code path.
fn apply_typed_state_updates_impl(
    room_id: &str,
    typed_root_bytes: &[u8],
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    lattice_bytes: &[u8],
    updates: Vec<(String, String, Option<String>)>,
) -> Result<ApplyTypedOutcome, String> {
    let room_key = room_structural_key_raw(room_id);
    let mut lattice = lattice_from_bytes(lattice_bytes)?;

    let typed_root = TypedRoot::decode_v1(typed_root_bytes)?;

    let expected_hash = typed_root_hash(&room_key, &typed_root.directory);
    if typed_root.structural_hash != expected_hash {
        return Err(format!(
            "Typed root structural hash is corrupt: stored {:?}, \
             recomputed {expected_hash:?}",
            typed_root.structural_hash
        ));
    }
    let expected_sg_id = rezzy::hamt::state_group_id_from_lthash(&lattice);
    if typed_root.state_group_id != expected_sg_id {
        return Err(format!(
            "Typed root state_group_id is corrupt: stored {:?}, \
             recomputed from lattice {expected_sg_id:?}",
            typed_root.state_group_id
        ));
    }

    let mut directory: std::collections::BTreeMap<String, StructuralHash> =
        typed_root.directory.into_iter().collect();

    // NOTE: unlike `apply_flat_state_updates_impl`, these nodes cannot be
    // verified against a single key here: each belongs to a specific
    // event_type's subtree, hashed under that type's `typed_subtree_key`
    // (derived from `room_key` -- see below), not `room_key` itself, and
    // `nodes` carries no per-entry type tag to pick the right one. Doing this
    // properly would mean walking `directory` (type -> subtree root hash) and
    // assigning each node to a type via BFS from its subtree root before
    // decoding it, rather than decoding this flat list up front.
    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    for (hash_bytes, node_bytes) in nodes {
        let hash = structural_hash_from_slice(&hash_bytes)?;
        let node = decode_persisted_node_unverified(&node_bytes, hash)?;
        node_map.insert(hash, node);
    }
    let known: HashSet<StructuralHash> = node_map.keys().copied().collect();

    let mut by_type: std::collections::BTreeMap<String, Vec<(String, Option<String>)>> =
        std::collections::BTreeMap::new();
    for (event_type, state_key, new_event_id) in updates {
        by_type
            .entry(event_type)
            .or_default()
            .push((state_key, new_event_id));
    }

    let mut seen_new = HashSet::new();
    let mut new_nodes = Vec::new();

    for (event_type, type_updates) in by_type {
        let subtree_key = typed_subtree_key(&room_key, &event_type);

        let mut subtree_root = match directory.get(&event_type) {
            Some(hash) => match node_map.get(hash).cloned() {
                Some(node) => node,
                None => return Ok(ApplyTypedOutcome::Missing(HashSet::from([*hash]))),
            },
            None => rezzy::hamt::build_hamt(&subtree_key, Vec::<(String, String)>::new())
                .map_err(|e| format!("Failed to build empty typed subtree: {e:?}"))?,
        };

        for (state_key, new_event_id) in type_updates {
            let mut resolver =
                |hash: &StructuralHash| -> Result<Arc<HamtNode<String, String>>, StructuralHash> {
                    node_map.get(hash).cloned().ok_or(*hash)
                };

            macro_rules! handle_typed_mutate_err {
                ($result:expr) => {
                    match $result {
                        Ok(pair) => pair,
                        Err(rezzy::hamt::HamtMutateError::Resolve(missing_hash)) => {
                            return Ok(ApplyTypedOutcome::Missing(HashSet::from([missing_hash])));
                        }
                        Err(rezzy::hamt::HamtMutateError::HashCollision { depth, bucket_size }) => {
                            return Err(format!(
                                "hash collision at depth {depth} with bucket size {bucket_size}"
                            ));
                        }
                        Err(rezzy::hamt::HamtMutateError::MaxDepthExceeded { depth }) => {
                            return Err(format!("HAMT maximum depth exceeded at depth {depth}"));
                        }
                    }
                };
            }

            match new_event_id {
                Some(new_id) => {
                    let (new_subtree_root, old_value) =
                        handle_typed_mutate_err!(rezzy::hamt::insert(
                            &subtree_root,
                            &subtree_key,
                            state_key.clone(),
                            new_id.clone(),
                            &mut resolver
                        ));
                    match old_value {
                        Some(old) => lattice.replace(&event_type, &state_key, &old, &new_id),
                        None => lattice.insert(&event_type, &state_key, &new_id),
                    }
                    subtree_root = new_subtree_root;
                }
                None => {
                    let (new_subtree_root, old_value) =
                        handle_typed_mutate_err!(rezzy::hamt::remove(
                            &subtree_root,
                            &subtree_key,
                            state_key.as_str(),
                            &mut resolver
                        ));
                    if let Some(old) = old_value {
                        lattice.remove(&event_type, &state_key, &old);
                    }
                    subtree_root = new_subtree_root;
                }
            }
            node_map.insert(subtree_root.structural_hash, subtree_root.clone());
        }

        if subtree_root.datamap == 0 && subtree_root.nodemap == 0 {
            // The subtree lost its last entry -- drop the directory entry
            // entirely rather than keeping a pointer to an empty subtree.
            directory.remove(&event_type);
        } else {
            directory.insert(event_type, subtree_root.structural_hash);
        }
        collect_new_persisted_nodes(subtree_root, &known, &mut seen_new, &mut new_nodes);
    }

    let directory: Vec<(String, StructuralHash)> = directory.into_iter().collect();
    let new_root = TypedRoot {
        structural_hash: typed_root_hash(&room_key, &directory),
        state_group_id: rezzy::hamt::state_group_id_from_lthash(&lattice),
        directory,
    };
    let state_group_id = new_root.state_group_id;
    let typed_root_bytes = new_root.encode_v1()?;

    Ok(ApplyTypedOutcome::Applied(AppliedTypedStateUpdate {
        typed_root_bytes,
        state_group_id,
        lattice_bytes: lattice_to_bytes(&lattice),
        new_nodes,
    }))
}

/// Decode a persisted HAMT node **without verifying** its structural hash
/// against its content, trusting `structural_hash` as given by the caller.
///
/// Only safe to use where the caller cannot possibly supply a mismatched
/// hash/bytes pair from an untrusted source, or where the room's
/// `structural_key` genuinely isn't available (e.g. the embedded-engine
/// materialize/audit paths, which are deliberately keyless -- see their doc
/// comments). Wherever
/// `structural_key` is available, use [`decode_persisted_node_verified`]
/// instead, which actually recomputes the hash from the decoded content and
/// rejects a mismatch.
pub(crate) fn decode_persisted_node_unverified(
    node_bytes: &[u8],
    structural_hash: StructuralHash,
) -> Result<Arc<HamtNode<String, String>>, String> {
    let persisted = PersistedInternalNode::<String, String>::decode_v1_unverified(node_bytes)
        .map_err(|e| format!("Failed to decode persisted HAMT node: {e}"))?;
    let children: Vec<NodeRef<String, String>> = persisted
        .child_hashes
        .into_iter()
        .map(NodeRef::Lazy)
        .collect();
    Ok(Arc::new(HamtNode {
        datamap: persisted.datamap,
        nodemap: persisted.nodemap,
        leaves: persisted.leaves,
        children,
        structural_hash,
    }))
}

/// Decode a persisted HAMT node and verify that `expected_hash` is actually
/// the structural hash of its decoded content under `structural_key` --
/// unlike [`decode_persisted_node_unverified`], a corrupted or substituted
/// `node_bytes`/`expected_hash` pair is rejected rather than silently
/// trusted.
pub(crate) fn decode_persisted_node_verified(
    node_bytes: &[u8],
    structural_key: &[u8],
    expected_hash: StructuralHash,
) -> Result<Arc<HamtNode<String, String>>, String> {
    let node = PersistedInternalNode::<String, String>::decode_v1_verified(
        node_bytes,
        structural_key,
        expected_hash,
    )
    .map_err(|e| format!("Failed to decode persisted HAMT node: {e}"))?;
    Ok(Arc::new(node))
}

/// Decode just enough of a persisted node to read its children's hashes,
/// without reconstructing the full node. Used to drive BFS traversal.
pub(crate) fn node_child_hashes_raw(node_bytes: &[u8]) -> Result<Vec<StructuralHash>, String> {
    let node = PersistedInternalNode::<String, String>::decode_v1_unverified(node_bytes)
        .map_err(|e| format!("Failed to decode persisted HAMT node: {e}"))?;
    Ok(node.child_hashes)
}

/// Decode a persisted HAMT node, computing its structural hash from `structural_key`.
///
/// Use this when the node's hash is not known in advance (e.g. root nodes that
/// were stored only as bytes, without an accompanying hash). When the hash is
/// already known, prefer [`decode_persisted_node_verified`] which additionally
/// checks the result against it.
fn decode_persisted_node_with_key(
    node_bytes: &[u8],
    structural_key: &[u8],
) -> Result<Arc<HamtNode<String, String>>, String> {
    let persisted = PersistedInternalNode::<String, String>::decode_v1_unverified(node_bytes)
        .map_err(|e| format!("Failed to decode persisted HAMT node: {e}"))?;
    let children: Vec<NodeRef<String, String>> = persisted
        .child_hashes
        .into_iter()
        .map(NodeRef::Lazy)
        .collect();
    let structural_hash = HamtNode::<String, String>::compute_structural_hash(
        structural_key,
        persisted.datamap,
        persisted.nodemap,
        &persisted.leaves,
        &children,
    );
    Ok(Arc::new(HamtNode {
        datamap: persisted.datamap,
        nodemap: persisted.nodemap,
        leaves: persisted.leaves,
        children,
        structural_hash,
    }))
}

/// Walk a fully-resolved HAMT (every reachable node present in `node_map`)
/// starting at `root_hash`, emitting `(event_type, state_key, event_id)`
/// triples for every entry.
pub(crate) fn materialize_from_node_map(
    root_hash: &StructuralHash,
    node_map: &HashMap<StructuralHash, Arc<HamtNode<String, String>>>,
) -> Result<Vec<(String, String, String)>, String> {
    let root_node = node_map
        .get(root_hash)
        .cloned()
        .ok_or_else(|| format!("Missing persisted HAMT root node: {:02x?}", root_hash))?;

    let mut entries = Vec::new();
    let mut resolver = |hash: &StructuralHash| -> Result<Arc<HamtNode<String, String>>, String> {
        node_map
            .get(hash)
            .cloned()
            .ok_or_else(|| format!("Missing persisted HAMT node: {:02x?}", hash))
    };

    root_node.visit_entries(&mut resolver, &mut |key, value| {
        let (event_type, state_key): (String, String) = serde_json::from_str(key)
            .map_err(|e| format!("Failed to decode HAMT state key: {e}"))?;
        entries.push((event_type, state_key, value.clone()));
        Ok::<(), String>(())
    })?;

    Ok(entries)
}

pub(crate) fn lookup_from_node_map(
    root_hash: &StructuralHash,
    structural_key: &[u8],
    keys: &[(String, String)],
    node_map: &HashMap<StructuralHash, Arc<HamtNode<String, String>>>,
) -> Result<(Vec<PyStateEntry>, HashSet<StructuralHash>), String> {
    let root_node = node_map
        .get(root_hash)
        .cloned()
        .ok_or_else(|| format!("Missing persisted HAMT root node: {:02x?}", root_hash))?;
    let mut entries = Vec::new();
    let mut missing = HashSet::new();

    for (event_type, state_key) in keys {
        let key = serde_json::to_string(&(event_type, state_key))
            .map_err(|e| format!("Failed to encode HAMT state key: {e}"))?;
        let mut resolver = |hash: &StructuralHash| {
            node_map.get(hash).cloned().ok_or_else(|| {
                missing.insert(*hash);
            })
        };
        if let Ok(Some(event_id)) = root_node.search(structural_key, &key, &mut resolver) {
            entries.push((event_type.clone(), state_key.clone(), event_id));
        }
    }

    Ok((entries, missing))
}

fn structural_hash_from_bytes(hash_bytes: Vec<u8>) -> Result<StructuralHash, PyErr> {
    hash_bytes
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("structural hash must be 32 bytes"))
}

#[pyfunction]
#[pyo3(text_signature = "(room_id, /)")]
pub fn room_structural_key(room_id: &str) -> PyResult<Vec<u8>> {
    Ok(room_structural_key_raw(room_id).to_vec())
}

/// See `room_hamt_prefix_raw` for the derivation. `msc4291_room_ids_as_hashes`
/// must come from the caller's real `RoomVersion.msc4291_room_ids_as_hashes`.
#[pyfunction]
#[pyo3(text_signature = "(room_id, msc4291_room_ids_as_hashes, /)")]
pub fn room_hamt_prefix(room_id: &str, msc4291_room_ids_as_hashes: bool) -> PyResult<Vec<u8>> {
    room_hamt_prefix_raw(room_id, msc4291_room_ids_as_hashes)
        .map(|prefix| prefix.to_vec())
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
#[pyo3(text_signature = "(room_id, entries, /)")]
pub fn build_root_handle(
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> PyResult<PyBuiltRoot> {
    let (root_handle_parts, nodes) = build_root_handle_and_nodes(room_id, entries)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    Ok((
        (root_handle_parts.0.to_vec(), root_handle_parts.1.to_vec()),
        nodes
            .into_iter()
            .map(|(hash, bytes)| (hash.to_vec(), bytes))
            .collect(),
    ))
}

#[pyfunction]
#[pyo3(text_signature = "(room_id, entries, /)")]
pub fn build_root_handle_with_lattice(
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> PyResult<PyBuiltRootWithLattice> {
    let (root_handle, lattice, nodes) = build_root_handle_nodes_and_lattice(room_id, entries)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    Ok((
        root_handle.structural_hash.to_vec(),
        root_handle.state_group_id.to_vec(),
        lattice_to_bytes(&lattice),
        nodes
            .into_iter()
            .map(|(hash, bytes)| (hash.to_vec(), bytes))
            .collect(),
    ))
}

#[pyfunction]
#[pyo3(text_signature = "(room_id, root_node_bytes, nodes, lattice_bytes, updates, /)")]
pub fn apply_flat_state_updates(
    room_id: &str,
    root_node_bytes: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    lattice_bytes: Vec<u8>,
    updates: Vec<PyStateUpdate>,
) -> PyResult<PyApplyOutcome> {
    let outcome =
        apply_flat_state_updates_impl(room_id, &root_node_bytes, nodes, &lattice_bytes, updates)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    Ok(match outcome {
        ApplyOutcome::Applied(applied) => (
            Some((
                applied.structural_hash.to_vec(),
                applied.state_group_id.to_vec(),
                applied.lattice_bytes,
                applied
                    .new_nodes
                    .into_iter()
                    .map(|(hash, bytes)| (hash.to_vec(), bytes))
                    .collect(),
            )),
            Vec::new(),
        ),
        ApplyOutcome::Missing(missing) => (
            None,
            missing.into_iter().map(|hash| hash.to_vec()).collect(),
        ),
    })
}

#[pyfunction]
#[pyo3(text_signature = "(room_id, entries, /)")]
pub fn build_typed_root(
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> PyResult<PyBuiltTypedRoot> {
    let (root, nodes) = build_typed_root_and_nodes(room_id, entries)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    let root_bytes = root
        .encode_v1()
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok((
        root.structural_hash.to_vec(),
        root.state_group_id.to_vec(),
        root_bytes,
        nodes
            .into_iter()
            .map(|(hash, bytes)| (hash.to_vec(), bytes))
            .collect(),
    ))
}

#[pyfunction]
#[pyo3(text_signature = "(room_id, entries, /)")]
pub fn build_typed_root_with_lattice(
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> PyResult<PyBuiltTypedRootWithLattice> {
    let (root, lattice, nodes) = build_typed_root_nodes_and_lattice(room_id, entries)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    let root_bytes = root
        .encode_v1()
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok((
        root.structural_hash.to_vec(),
        root.state_group_id.to_vec(),
        lattice_to_bytes(&lattice),
        root_bytes,
        nodes
            .into_iter()
            .map(|(hash, bytes)| (hash.to_vec(), bytes))
            .collect(),
    ))
}

#[pyfunction]
#[pyo3(text_signature = "(room_id, typed_root_bytes, nodes, lattice_bytes, updates, /)")]
pub fn apply_typed_state_updates(
    room_id: &str,
    typed_root_bytes: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    lattice_bytes: Vec<u8>,
    updates: Vec<PyStateUpdate>,
) -> PyResult<PyApplyTypedOutcome> {
    let outcome =
        apply_typed_state_updates_impl(room_id, &typed_root_bytes, nodes, &lattice_bytes, updates)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    Ok(match outcome {
        ApplyTypedOutcome::Applied(applied) => (
            Some((
                applied.typed_root_bytes,
                applied.state_group_id.to_vec(),
                applied.lattice_bytes,
                applied
                    .new_nodes
                    .into_iter()
                    .map(|(hash, bytes)| (hash.to_vec(), bytes))
                    .collect(),
            )),
            Vec::new(),
        ),
        ApplyTypedOutcome::Missing(missing) => (
            None,
            missing.into_iter().map(|hash| hash.to_vec()).collect(),
        ),
    })
}

#[pyfunction]
#[pyo3(text_signature = "(root_bytes, /)")]
pub fn decode_typed_root(root_bytes: Vec<u8>) -> PyResult<PyDecodedTypedRoot> {
    let root =
        TypedRoot::decode_v1(&root_bytes).map_err(pyo3::exceptions::PyValueError::new_err)?;
    Ok((
        root.structural_hash.to_vec(),
        root.state_group_id.to_vec(),
        root.directory
            .into_iter()
            .map(|(event_type, hash)| (event_type, hash.to_vec()))
            .collect(),
    ))
}

/// Materialize a state group's entries from already-fetched node bytes.
///
/// Unlike [`lookup_state_entries`], this takes no `room_id`
/// and so cannot recompute (and therefore cannot verify) nodes' structural
/// hashes against their content -- it trusts that `nodes` genuinely came from
/// this room's Postgres `state_hamt_nodes` table, keyed by `structural_hash`.
#[pyfunction]
#[pyo3(text_signature = "(root_node_bytes, nodes, /)")]
pub fn materialize_state_entries(
    root_node_bytes: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<Vec<PyStateEntry>> {
    let mut root_hash = None;
    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();

    for (hash_bytes, node_bytes) in nodes {
        let hash = structural_hash_from_bytes(hash_bytes)?;
        if node_bytes == root_node_bytes {
            root_hash = Some(hash);
        }
        let node = decode_persisted_node_unverified(&node_bytes, hash)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        node_map.insert(hash, node);
    }

    let root_hash = root_hash.ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err("root_node_bytes not found in nodes list")
    })?;

    materialize_from_node_map(&root_hash, &node_map)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

/// Look up a set of `(event_type, state_key)` entries in a room's HAMT,
/// given a pool of already-fetched `(hash, node_bytes)` pairs.
///
/// `nodes` may contain entries irrelevant to this room -- callers resolving
/// several state groups in one batch (e.g.
/// `_lookup_state_hamt_from_postgres_many_txn`) share one fetched-node pool
/// across every group's call, including nodes belonging to entirely
/// different rooms that were never meant to be dereferenced by *this* room's
/// tree. So nodes are decoded unverified up front, and only verified after
/// the lookup, scoped to whatever actually turned out to be reachable from
/// `root_hash` -- which structurally can never include another room's nodes
/// (their hashes never appear as a child of this room's nodes). A
/// verification failure there is the real thing this function exists to
/// catch: corrupted/substituted bytes for a node this room's own tree
/// actually depends on.
fn lookup_state_entries_impl(
    structural_key: &[u8; 32],
    root_node_bytes: &[u8],
    nodes: Vec<(StructuralHash, Vec<u8>)>,
    keys: &[(String, String)],
) -> Result<(Vec<PyStateEntry>, Vec<StructuralHash>), String> {
    let root_node = decode_persisted_node_with_key(root_node_bytes, structural_key)?;
    let root_hash = root_node.structural_hash;
    let mut node_map = HashMap::from([(root_hash, root_node)]);
    let mut raw_bytes: HashMap<StructuralHash, Vec<u8>> = HashMap::new();
    for (hash, node_bytes) in nodes {
        let node = decode_persisted_node_unverified(&node_bytes, hash)?;
        raw_bytes.insert(hash, node_bytes);
        node_map.insert(hash, node);
    }

    let (entries, missing) = lookup_from_node_map(&root_hash, structural_key, keys, &node_map)?;

    if let Ok(typed_root) = TypedRoot::decode_v1(root_node_bytes) {
        for (event_type, subtree_root_hash) in typed_root.directory {
            let subtree_key = typed_subtree_key(structural_key, &event_type);
            let mut seen = HashSet::from([subtree_root_hash]);
            let mut stack = vec![subtree_root_hash];
            while let Some(hash) = stack.pop() {
                if let Some(bytes) = raw_bytes.get(&hash) {
                    decode_persisted_node_verified(bytes, &subtree_key, hash)?;
                }
                if let Some(node) = node_map.get(&hash) {
                    for child in &node.children {
                        let child_hash = child.structural_hash();
                        if seen.insert(child_hash) {
                            stack.push(child_hash);
                        }
                    }
                }
            }
        }
    } else {
        // A flat root uses the room structural key directly. As above, verify
        // only nodes actually reachable from this root: the fetched batch can
        // legitimately contain nodes for other rooms.
        let mut seen = HashSet::from([root_hash]);
        let mut stack = vec![root_hash];
        while let Some(hash) = stack.pop() {
            if let Some(bytes) = raw_bytes.get(&hash) {
                decode_persisted_node_verified(bytes, structural_key, hash)?;
            }
            if let Some(node) = node_map.get(&hash) {
                for child in &node.children {
                    let child_hash = child.structural_hash();
                    if seen.insert(child_hash) {
                        stack.push(child_hash);
                    }
                }
            }
        }
    }

    Ok((entries, missing.into_iter().collect()))
}

#[pyfunction]
#[pyo3(text_signature = "(room_id, root_node_bytes, nodes, keys, /)")]
pub fn lookup_state_entries(
    room_id: &str,
    root_node_bytes: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    keys: Vec<(String, String)>,
) -> PyResult<PyStateLookup> {
    let structural_key = room_structural_key_raw(room_id);
    let nodes = nodes
        .into_iter()
        .map(|(hash_bytes, node_bytes)| {
            structural_hash_from_bytes(hash_bytes).map(|hash| (hash, node_bytes))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let (entries, missing) =
        lookup_state_entries_impl(&structural_key, &root_node_bytes, nodes, &keys)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok((
        entries,
        missing.into_iter().map(|hash| hash.to_vec()).collect(),
    ))
}

#[pyfunction]
#[pyo3(text_signature = "(node_bytes, /)")]
pub fn node_child_hashes(node_bytes: Vec<u8>) -> PyResult<Vec<Vec<u8>>> {
    let hashes =
        node_child_hashes_raw(&node_bytes).map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(hashes.into_iter().map(|hash| hash.to_vec()).collect())
}

/// Audits reachability across a batch of nodes spanning potentially many
/// rooms at once, so (like [`materialize_state_entries`]) it has no
/// `room_id` and cannot verify nodes' structural hashes
/// against their content.
#[pyfunction]
#[pyo3(text_signature = "(roots, universe, nodes, /)")]
pub fn reachability_audit(
    roots: Vec<Vec<u8>>,
    universe: Vec<Vec<u8>>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<PyReachabilityAudit> {
    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();

    for (hash_bytes, node_bytes) in nodes {
        let hash = structural_hash_from_bytes(hash_bytes)?;
        let node = decode_persisted_node_unverified(&node_bytes, hash)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        node_map.insert(hash, node);
    }

    let roots = roots
        .into_iter()
        .map(structural_hash_from_bytes)
        .map(|hash| {
            hash.and_then(|hash| {
                node_map.get(&hash).cloned().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Missing HAMT root node: {:02x?}",
                        hash
                    ))
                })
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    let universe = universe
        .into_iter()
        .map(structural_hash_from_bytes)
        .collect::<PyResult<Vec<_>>>()?;

    let mut resolver = |hash: &StructuralHash| -> Result<Arc<HamtNode<String, String>>, String> {
        node_map
            .get(hash)
            .cloned()
            .ok_or_else(|| format!("Missing persisted HAMT node: {:02x?}", hash))
    };

    let audit = rezzy::hamt::bitmap_node_reachability_audit(roots, universe, &mut resolver)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e:?}")))?;
    let reachable = audit
        .reachable
        .iter()
        .map(|idx| {
            audit.universe.hash_at(idx).ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "HAMT bitmap audit returned out-of-range reachable index: {idx}"
                ))
            })
        })
        .map(|hash| hash.map(|hash| hash.to_vec()))
        .collect::<PyResult<Vec<_>>>()?;
    let unreachable = audit
        .unreachable
        .iter()
        .map(|idx| {
            audit.universe.hash_at(idx).ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "HAMT bitmap audit returned out-of-range unreachable index: {idx}"
                ))
            })
        })
        .map(|hash| hash.map(|hash| hash.to_vec()))
        .collect::<PyResult<Vec<_>>>()?;

    Ok((reachable, unreachable))
}

#[pyfunction]
#[pyo3(text_signature = "(roots, universe, nodes, /)")]
pub fn unreachable_node_hashes(
    roots: Vec<Vec<u8>>,
    universe: Vec<Vec<u8>>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<Vec<Vec<u8>>> {
    reachability_audit(roots, universe, nodes).map(|(_, unreachable)| unreachable)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "state_hamt")?;
    child_module.add_function(wrap_pyfunction!(room_structural_key, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(room_hamt_prefix, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(build_root_handle, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(
        build_root_handle_with_lattice,
        &child_module
    )?)?;
    child_module.add_function(wrap_pyfunction!(apply_flat_state_updates, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(build_typed_root, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(
        build_typed_root_with_lattice,
        &child_module
    )?)?;
    child_module.add_function(wrap_pyfunction!(apply_typed_state_updates, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(decode_typed_root, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_entries, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(lookup_state_entries, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(node_child_hashes, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(reachability_audit, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(unreachable_node_hashes, &child_module)?)?;
    m.add_submodule(&child_module)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.state_hamt", &child_module)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_applied(outcome: Result<ApplyOutcome, String>, msg: &str) -> AppliedStateUpdate {
        match outcome.expect(msg) {
            ApplyOutcome::Applied(applied) => applied,
            ApplyOutcome::Missing(missing) => panic!("unexpected missing nodes: {missing:02x?}"),
        }
    }

    fn expect_applied_typed(
        outcome: Result<ApplyTypedOutcome, String>,
        msg: &str,
    ) -> AppliedTypedStateUpdate {
        match outcome.expect(msg) {
            ApplyTypedOutcome::Applied(applied) => applied,
            ApplyTypedOutcome::Missing(missing) => {
                panic!("unexpected missing nodes: {missing:02x?}")
            }
        }
    }

    #[test]
    fn apply_typed_state_updates_matches_full_rebuild_with_few_new_nodes() {
        // The typed-root analogue of apply_flat_state_updates_matches_full_
        // rebuild_with_few_new_nodes: a one-key replace against an existing
        // typed root must converge on exactly what a full build_typed_root
        // rebuild of the resulting state produces, and write only a small,
        // depth-bounded number of new subtree nodes -- not O(S_T), and
        // definitely not O(S).
        let room_id = "!room:test.example";
        let mut entries: Vec<(String, String, String)> = (0..300)
            .map(|i| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{i}:test.example"),
                    format!("$original-{i}"),
                )
            })
            .collect();
        entries.push((
            "m.room.create".to_owned(),
            String::new(),
            "$create".to_owned(),
        ));
        entries.push((
            "m.room.power_levels".to_owned(),
            String::new(),
            "$pl".to_owned(),
        ));

        let (typed_root, lattice, nodes) =
            build_typed_root_nodes_and_lattice(room_id, entries.clone())
                .expect("initial typed root should build");
        let total_node_count = nodes.len();
        let typed_root_bytes = typed_root.encode_v1().expect("typed root should encode");

        // --- Replace one existing key within the m.room.member subtree ---
        let changed_key = "@user-150:test.example".to_owned();
        let applied = expect_applied_typed(
            apply_typed_state_updates_impl(
                room_id,
                &typed_root_bytes,
                nodes.iter().map(|(h, b)| (h.to_vec(), b.clone())).collect(),
                &lattice_to_bytes(&lattice),
                vec![(
                    "m.room.member".to_owned(),
                    changed_key.clone(),
                    Some("$replaced-150".to_owned()),
                )],
            ),
            "incremental typed replace should apply",
        );

        assert!(
            applied.new_nodes.len() < 10,
            "expected a small, depth-bounded number of new subtree nodes for one \
             changed key out of {} entries, got {}",
            entries.len(),
            applied.new_nodes.len()
        );
        assert!(
            applied.new_nodes.len() < total_node_count,
            "incremental typed update must not rewrite the whole structure"
        );

        let mut rebuilt_entries = entries.clone();
        let idx = rebuilt_entries
            .iter()
            .position(|(_, sk, _)| sk == &changed_key)
            .expect("changed key must exist in original entries");
        rebuilt_entries[idx].2 = "$replaced-150".to_owned();
        let (rebuilt_typed_root, _, _) =
            build_typed_root_nodes_and_lattice(room_id, rebuilt_entries.clone())
                .expect("full typed rebuild should build");

        let applied_typed_root =
            TypedRoot::decode_v1(&applied.typed_root_bytes).expect("applied root should decode");
        assert_eq!(
            applied_typed_root.structural_hash, rebuilt_typed_root.structural_hash,
            "incrementally updated typed root must have the same directory structural_hash as \
             a full rebuild of the resulting state"
        );
        assert_eq!(
            applied_typed_root.directory, rebuilt_typed_root.directory,
            "incrementally updated typed root must have the same per-type subtree hashes as a \
             full rebuild of the resulting state"
        );
        assert_eq!(
            applied.state_group_id, rebuilt_typed_root.state_group_id,
            "incrementally updated typed root must have the same cross-server state_group_id \
             as a full rebuild of the resulting state"
        );

        // --- Insert a state key for a brand-new event type (no prior subtree) ---
        let applied_typed_root_bytes = applied.typed_root_bytes.clone();
        let mut combined_nodes = nodes;
        combined_nodes.extend(applied.new_nodes.clone());

        let applied2 = expect_applied_typed(
            apply_typed_state_updates_impl(
                room_id,
                &applied_typed_root_bytes,
                combined_nodes
                    .iter()
                    .map(|(h, b)| (h.to_vec(), b.clone()))
                    .collect(),
                &applied.lattice_bytes,
                vec![(
                    "m.room.join_rules".to_owned(),
                    String::new(),
                    Some("$join_rules".to_owned()),
                )],
            ),
            "incremental insert of a brand-new event type should apply",
        );

        let mut rebuilt_entries2 = rebuilt_entries;
        rebuilt_entries2.push((
            "m.room.join_rules".to_owned(),
            String::new(),
            "$join_rules".to_owned(),
        ));
        let (rebuilt_typed_root2, _, _) =
            build_typed_root_nodes_and_lattice(room_id, rebuilt_entries2)
                .expect("second full typed rebuild should build");
        let applied_typed_root2 = TypedRoot::decode_v1(&applied2.typed_root_bytes)
            .expect("second applied root should decode");

        assert_eq!(
            applied_typed_root2.directory, rebuilt_typed_root2.directory,
            "adding a brand-new event type incrementally must match a full rebuild"
        );
        assert_eq!(applied2.state_group_id, rebuilt_typed_root2.state_group_id);
    }

    #[test]
    fn apply_typed_state_updates_removes_empty_type_directory_entry() {
        let room_id = "!room:test.example";
        let entries = vec![
            (
                "m.room.create".to_owned(),
                String::new(),
                "$create".to_owned(),
            ),
            (
                "m.room.join_rules".to_owned(),
                String::new(),
                "$join_rules".to_owned(),
            ),
        ];
        let (typed_root, lattice, nodes) = build_typed_root_nodes_and_lattice(room_id, entries)
            .expect("initial typed root should build");
        assert!(
            typed_root
                .directory
                .iter()
                .any(|(event_type, _)| event_type == "m.room.join_rules"),
            "initial directory should contain the removed event type"
        );
        let empty_subtree_hash = rezzy::hamt::build_hamt(
            &typed_subtree_key(&room_structural_key_raw(room_id), "m.room.join_rules"),
            Vec::<(String, String)>::new(),
        )
        .expect("empty typed subtree should build")
        .structural_hash;

        let applied = expect_applied_typed(
            apply_typed_state_updates_impl(
                room_id,
                &typed_root.encode_v1().expect("typed root should encode"),
                nodes.iter().map(|(h, b)| (h.to_vec(), b.clone())).collect(),
                &lattice_to_bytes(&lattice),
                vec![("m.room.join_rules".to_owned(), String::new(), None)],
            ),
            "removing a type's final state entry should apply",
        );
        let applied_root =
            TypedRoot::decode_v1(&applied.typed_root_bytes).expect("applied root should decode");

        let (rebuilt_root, _, _) = build_typed_root_nodes_and_lattice(
            room_id,
            vec![(
                "m.room.create".to_owned(),
                String::new(),
                "$create".to_owned(),
            )],
        )
        .expect("full rebuild should build");

        assert_eq!(applied_root.directory, rebuilt_root.directory);
        assert_eq!(applied_root.structural_hash, rebuilt_root.structural_hash);
        assert!(
            !applied_root
                .directory
                .iter()
                .any(|(event_type, _)| event_type == "m.room.join_rules"),
            "an emptied subtree must not remain in the typed root directory"
        );
        assert!(
            applied
                .new_nodes
                .iter()
                .any(|(hash, _)| *hash == empty_subtree_hash),
            "the emptied subtree remains an emitted immutable node"
        );
    }

    #[test]
    fn apply_typed_state_updates_reports_missing_node_for_retry() {
        let room_id = "!room:test.example";
        let entries: Vec<(String, String, String)> = (0..200)
            .map(|i| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{i}:test.example"),
                    format!("${i}"),
                )
            })
            .collect();
        let (typed_root, lattice, _nodes) = build_typed_root_nodes_and_lattice(room_id, entries)
            .expect("initial typed root should build");
        let typed_root_bytes = typed_root.encode_v1().expect("typed root should encode");

        let outcome = apply_typed_state_updates_impl(
            room_id,
            &typed_root_bytes,
            Vec::new(), // deliberately withhold the subtree root and every path node
            &lattice_to_bytes(&lattice),
            vec![(
                "m.room.member".to_owned(),
                "@user-150:test.example".to_owned(),
                Some("$replaced".to_owned()),
            )],
        )
        .expect("should not be a hard error");

        match outcome {
            ApplyTypedOutcome::Missing(missing) => assert!(
                !missing.is_empty(),
                "expected at least one missing hash to retry with"
            ),
            ApplyTypedOutcome::Applied(_) => {
                panic!("expected a Missing outcome when subtree nodes were withheld")
            }
        }
    }

    #[test]
    fn apply_flat_state_updates_reports_missing_node_for_retry() {
        // A caller that supplied only the root (no path nodes) must get back
        // a retryable Missing outcome, not a hard error -- this is what lets
        // Python reuse the existing lookup_state_entries-style retry loop
        // (see _lookup_state_hamt_from_postgres_txn) instead of a bespoke
        // error-handling path.
        let room_id = "!room:test.example";
        let entries: Vec<(String, String, String)> = (0..200)
            .map(|i| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{i}:test.example"),
                    format!("${i}"),
                )
            })
            .collect();
        let (root_handle, lattice, nodes) = build_root_handle_nodes_and_lattice(room_id, entries)
            .expect("initial root should build");
        let root_bytes = nodes
            .iter()
            .find(|(hash, _)| *hash == root_handle.structural_hash)
            .map(|(_, bytes)| bytes.clone())
            .expect("root node must be among the built nodes");

        let outcome = apply_flat_state_updates_impl(
            room_id,
            &root_bytes,
            Vec::new(), // deliberately withhold every non-root node
            &lattice_to_bytes(&lattice),
            vec![(
                "m.room.member".to_owned(),
                "@user-150:test.example".to_owned(),
                Some("$replaced".to_owned()),
            )],
        )
        .expect("should not be a hard error");

        match outcome {
            ApplyOutcome::Missing(missing) => assert!(
                !missing.is_empty(),
                "expected at least one missing hash to retry with"
            ),
            ApplyOutcome::Applied(_) => {
                panic!("expected a Missing outcome when path nodes were withheld")
            }
        }
    }

    #[test]
    fn apply_flat_state_updates_matches_full_rebuild_with_few_new_nodes() {
        // The load-bearing property: a one-key change against an existing
        // root must (a) converge on exactly what a full rebuild of the
        // resulting state would produce, and (b) write only a small,
        // depth-bounded number of new nodes — not O(S). This is what
        // separates the incremental update path from the "materialize full
        // state, rebuild everything" cost this whole design exists to avoid.
        let room_id = "!room:test.example";
        let mut entries: Vec<(String, String, String)> = (0..300)
            .map(|i| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{i}:test.example"),
                    format!("$original-{i}"),
                )
            })
            .collect();
        entries.push((
            "m.room.create".to_owned(),
            String::new(),
            "$create".to_owned(),
        ));

        let (root_handle, lattice, nodes) =
            build_root_handle_nodes_and_lattice(room_id, entries.clone())
                .expect("initial root should build");
        let total_node_count = nodes.len();
        let root_bytes = nodes
            .iter()
            .find(|(hash, _)| *hash == root_handle.structural_hash)
            .map(|(_, bytes)| bytes.clone())
            .expect("root node must be among the built nodes");

        // --- Replace one existing key ---
        let changed_key = "@user-150:test.example".to_owned();
        let applied = expect_applied(
            apply_flat_state_updates_impl(
                room_id,
                &root_bytes,
                nodes.iter().map(|(h, b)| (h.to_vec(), b.clone())).collect(),
                &lattice_to_bytes(&lattice),
                vec![(
                    "m.room.member".to_owned(),
                    changed_key.clone(),
                    Some("$replaced-150".to_owned()),
                )],
            ),
            "incremental replace should apply",
        );

        assert!(
            applied.new_nodes.len() < 10,
            "expected a small, depth-bounded number of new nodes for one \
             changed key out of {}, got {}",
            entries.len(),
            applied.new_nodes.len()
        );
        assert!(
            applied.new_nodes.len() < total_node_count,
            "incremental update must not rewrite the whole tree"
        );

        let mut rebuilt_entries = entries.clone();
        let idx = rebuilt_entries
            .iter()
            .position(|(_, sk, _)| sk == &changed_key)
            .expect("changed key must exist in original entries");
        rebuilt_entries[idx].2 = "$replaced-150".to_owned();
        let (rebuilt_parts, _) = build_root_handle_and_nodes(room_id, rebuilt_entries.clone())
            .expect("full rebuild should build");

        assert_eq!(
            applied.structural_hash, rebuilt_parts.0,
            "incrementally updated root must have the same structural_hash as a full rebuild \
             of the resulting state (canonical CHAMP shape)"
        );
        assert_eq!(
            applied.state_group_id, rebuilt_parts.1,
            "incrementally updated root must have the same cross-server state_group_id as a \
             full rebuild of the resulting state"
        );

        // --- Insert a brand-new key on top of the already-updated root ---
        let applied_root_bytes = applied
            .new_nodes
            .iter()
            .find(|(hash, _)| *hash == applied.structural_hash)
            .map(|(_, bytes)| bytes.clone())
            .expect("updated root must be among the newly persisted nodes");
        let mut combined_nodes = nodes;
        combined_nodes.extend(applied.new_nodes.clone());

        let applied2 = expect_applied(
            apply_flat_state_updates_impl(
                room_id,
                &applied_root_bytes,
                combined_nodes
                    .iter()
                    .map(|(h, b)| (h.to_vec(), b.clone()))
                    .collect(),
                &applied.lattice_bytes,
                vec![(
                    "m.room.member".to_owned(),
                    "@user-999:test.example".to_owned(),
                    Some("$new-999".to_owned()),
                )],
            ),
            "incremental insert should apply",
        );

        assert!(
            applied2.new_nodes.len() < 10,
            "expected a small, depth-bounded number of new nodes for one new key, got {}",
            applied2.new_nodes.len()
        );

        let mut rebuilt_entries2 = rebuilt_entries;
        rebuilt_entries2.push((
            "m.room.member".to_owned(),
            "@user-999:test.example".to_owned(),
            "$new-999".to_owned(),
        ));
        let (rebuilt_parts2, _) = build_root_handle_and_nodes(room_id, rebuilt_entries2)
            .expect("second full rebuild should build");

        assert_eq!(applied2.structural_hash, rebuilt_parts2.0);
        assert_eq!(applied2.state_group_id, rebuilt_parts2.1);
    }

    #[test]
    fn apply_flat_state_updates_remove_matches_full_rebuild() {
        let room_id = "!room:test.example";
        let entries: Vec<(String, String, String)> = (0..50)
            .map(|i| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{i}:test.example"),
                    format!("${i}"),
                )
            })
            .collect();

        let (root_handle, lattice, nodes) =
            build_root_handle_nodes_and_lattice(room_id, entries.clone())
                .expect("initial root should build");
        let root_bytes = nodes
            .iter()
            .find(|(hash, _)| *hash == root_handle.structural_hash)
            .map(|(_, bytes)| bytes.clone())
            .expect("root node must be among the built nodes");

        let removed_key = "@user-7:test.example".to_owned();
        let applied = expect_applied(
            apply_flat_state_updates_impl(
                room_id,
                &root_bytes,
                nodes.iter().map(|(h, b)| (h.to_vec(), b.clone())).collect(),
                &lattice_to_bytes(&lattice),
                vec![("m.room.member".to_owned(), removed_key.clone(), None)],
            ),
            "incremental remove should apply",
        );

        let rebuilt_entries: Vec<_> = entries
            .into_iter()
            .filter(|(_, sk, _)| sk != &removed_key)
            .collect();
        let (rebuilt_parts, _) = build_root_handle_and_nodes(room_id, rebuilt_entries)
            .expect("full rebuild after removal should build");

        assert_eq!(applied.structural_hash, rebuilt_parts.0);
        assert_eq!(applied.state_group_id, rebuilt_parts.1);
    }

    #[test]
    fn lattice_bytes_roundtrip() {
        let mut lattice = LtHash::default();
        lattice.insert("m.room.create", "", "$abc");
        lattice.insert("m.room.member", "@a:test", "$def");

        let bytes = lattice_to_bytes(&lattice);
        assert_eq!(bytes.len(), 2048);
        let decoded = lattice_from_bytes(&bytes).expect("lattice should decode");
        assert_eq!(lattice.digest(), decoded.digest());
    }

    #[test]
    fn structural_key_is_deterministic() {
        let room_id = "!room:test.example";

        let key1 = room_structural_key_raw(room_id);
        let key2 = room_structural_key_raw(room_id);
        let other_key = room_structural_key_raw("!other:test.example");

        assert_eq!(key1, key2);
        assert_ne!(key1, other_key);
        assert_eq!(key1.len(), 32);
    }

    #[test]
    fn room_hamt_prefix_is_deterministic_and_room_scoped() {
        let room_id = "!AbCdEfGhIjKlMnOpQr:test.example";
        let other_room_id = "!ZyXwVuTsRqPoNmLkJi:test.example";

        let prefix1 = room_hamt_prefix_raw(room_id, false).unwrap();
        let prefix2 = room_hamt_prefix_raw(room_id, false).unwrap();
        let other_prefix = room_hamt_prefix_raw(other_room_id, false).unwrap();

        assert_eq!(prefix1, prefix2);
        assert_ne!(prefix1, other_prefix);
        assert_eq!(prefix1.len(), 8);
    }

    #[test]
    fn room_hamt_prefix_decodes_msc4291_room_ids_without_rehashing() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        // A 32-byte "create event reference hash", as msc4291 room IDs encode.
        let create_event_hash = [42u8; 32];
        let room_id = format!("!{}", URL_SAFE_NO_PAD.encode(create_event_hash));

        let prefix = room_hamt_prefix_raw(&room_id, true).unwrap();

        // The prefix should be exactly the leading 8 bytes of the decoded
        // hash -- i.e. derived directly from the room ID, not re-hashed
        // through the (server-secret-salted) v1-v11 path.
        assert_eq!(prefix, create_event_hash[..8]);
    }

    #[test]
    fn room_hamt_prefix_rejects_invalid_msc4291_room_id() {
        assert!(room_hamt_prefix_raw("!not-valid-base64!!!", true).is_err());
    }

    #[test]
    fn build_root_handle_returns_root_and_persisted_nodes() {
        let room_id = "!room:test.example";
        let entries = vec![
            (
                "m.room.member".to_owned(),
                "@alice:test.example".to_owned(),
                "$1".to_owned(),
            ),
            ("m.room.name".to_owned(), "".to_owned(), "$2".to_owned()),
            ("m.room.topic".to_owned(), "".to_owned(), "$3".to_owned()),
        ];

        let ((structural_hash, state_group_id), nodes) =
            build_root_handle_and_nodes(room_id, entries).expect("HAMT root should build");

        assert_eq!(structural_hash.len(), 32);
        assert_eq!(state_group_id.len(), 32);
        assert!(!nodes.is_empty());
        assert!(nodes
            .iter()
            .all(|(hash, bytes)| hash.len() == 32 && !bytes.is_empty()));
    }

    #[test]
    fn typed_root_roundtrips_sorted_directory() {
        let entries = vec![
            (
                "m.room.member".to_owned(),
                "@alice:test".to_owned(),
                "$1".to_owned(),
            ),
            ("m.room.create".to_owned(), "".to_owned(), "$2".to_owned()),
        ];
        let (root, nodes) =
            build_typed_root_and_nodes("!room:test", entries).expect("typed root should build");
        assert_eq!(root.directory[0].0, "m.room.create");
        assert_eq!(root.directory[1].0, "m.room.member");
        assert!(!nodes.is_empty());
        let encoded = root.encode_v1().expect("typed root should encode");
        assert_eq!(
            TypedRoot::decode_v1(&encoded).expect("typed root should decode"),
            root
        );
        assert_eq!(encoded[0], TYPED_ROOT_FORMAT);
    }

    #[test]
    fn typed_root_state_group_id_matches_flat_root() {
        // Same logical state, built two ways, must converge on the same
        // cross-server state-group identity. The keyed structural hashes are
        // expected to differ (flat vs. typed layouts are different local
        // structures); state_group_id must not.
        let room_id = "!room:test.example";
        let entries = vec![
            (
                "m.room.member".to_owned(),
                "@alice:test".to_owned(),
                "$1".to_owned(),
            ),
            ("m.room.create".to_owned(), "".to_owned(), "$2".to_owned()),
            (
                "m.room.power_levels".to_owned(),
                "".to_owned(),
                "$3".to_owned(),
            ),
        ];

        let (flat_parts, _) =
            build_root_handle_and_nodes(room_id, entries.clone()).expect("flat root should build");
        let (typed_root, _) =
            build_typed_root_and_nodes(room_id, entries).expect("typed root should build");

        let (_, flat_state_group_id) = flat_parts;
        assert_eq!(
            typed_root.state_group_id, flat_state_group_id,
            "typed root's state_group_id must equal the flat root's for identical state"
        );
        assert_ne!(
            flat_state_group_id, [0u8; 32],
            "state_group_id must not be the digest of the zero lattice"
        );
    }

    #[test]
    fn typed_root_rejects_unsorted_directory() {
        let root = TypedRoot {
            structural_hash: [0u8; 32],
            state_group_id: [0u8; 32],
            directory: vec![("z".to_owned(), [1u8; 32]), ("a".to_owned(), [2u8; 32])],
        };
        let mut encoded = vec![TYPED_ROOT_FORMAT];
        encoded.extend_from_slice(&root.structural_hash);
        encoded.extend_from_slice(&root.state_group_id);
        encoded.extend_from_slice(&2u16.to_le_bytes());
        for (event_type, hash) in root.directory {
            encoded.extend_from_slice(&(event_type.len() as u16).to_le_bytes());
            encoded.extend_from_slice(event_type.as_bytes());
            encoded.extend_from_slice(&hash);
        }
        assert!(TypedRoot::decode_v1(&encoded).is_err());
    }

    #[test]
    fn materialize_state_entries_roundtrips_root() {
        let room_id = "!room:test.example";
        let entries = vec![
            (
                "m.room.member".to_owned(),
                "@alice:test.example".to_owned(),
                "$1".to_owned(),
            ),
            ("m.room.name".to_owned(), "".to_owned(), "$2".to_owned()),
        ];

        let ((root_hash, _), nodes) =
            build_root_handle_and_nodes(room_id, entries).expect("HAMT root should build");

        let root_bytes = nodes
            .iter()
            .find(|(hash, _)| hash == &root_hash)
            .map(|(_, bytes)| bytes.clone())
            .expect("root node should exist");

        let recovered = materialize_state_entries(
            root_bytes,
            nodes
                .into_iter()
                .map(|(hash, bytes)| (hash.to_vec(), bytes))
                .collect(),
        )
        .expect("HAMT materialization should work");

        assert!(!recovered.is_empty());
        assert!(recovered
            .iter()
            .any(|(etype, state_key, event_id)| etype == "m.room.member"
                && state_key == "@alice:test.example"
                && event_id == "$1"));
        assert!(recovered.iter().any(|(_, _, event_id)| event_id == "$2"));
        assert_eq!(root_hash.len(), 32);
    }

    #[test]
    fn lookup_state_entries_fetches_only_requested_paths() {
        let room_id = "!room:test.example";
        let entries = (0..1_000)
            .map(|i| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{i}:test.example"),
                    format!("${i}"),
                )
            })
            .collect::<Vec<_>>();
        let ((root_hash, _), nodes) =
            build_root_handle_and_nodes(room_id, entries).expect("HAMT root should build");
        let all_nodes = nodes
            .into_iter()
            .map(|(hash, bytes)| {
                decode_persisted_node_unverified(&bytes, hash)
                    .map(|node| (hash, node))
                    .expect("node should decode")
            })
            .collect::<HashMap<_, _>>();
        let root_only = HashMap::from([(
            root_hash,
            all_nodes
                .get(&root_hash)
                .cloned()
                .expect("root should exist"),
        )]);
        let keys = vec![
            (
                "m.room.member".to_owned(),
                "@user-42:test.example".to_owned(),
            ),
            (
                "m.room.member".to_owned(),
                "@missing:test.example".to_owned(),
            ),
        ];
        let structural_key = room_structural_key_raw(room_id);

        let (partial, missing) =
            lookup_from_node_map(&root_hash, &structural_key, &keys, &root_only)
                .expect("partial lookup should identify missing nodes");
        assert!(partial.is_empty());
        assert!(!missing.is_empty());
        assert!(missing.len() < all_nodes.len());

        let (found, missing) = lookup_from_node_map(&root_hash, &structural_key, &keys, &all_nodes)
            .expect("complete lookup should work");
        assert!(missing.is_empty());
        assert_eq!(
            found,
            vec![(
                "m.room.member".to_owned(),
                "@user-42:test.example".to_owned(),
                "$42".to_owned()
            )]
        );
    }

    #[test]
    fn lookup_state_entries_rejects_substituted_reachable_flat_node() {
        let room_id = "!room:test.example";
        let entries = (0..1_000)
            .map(|i| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{i}:test.example"),
                    format!("${i}"),
                )
            })
            .collect::<Vec<_>>();
        let ((root_hash, _), mut nodes) = build_root_handle_and_nodes(room_id, entries).unwrap();
        let root_bytes = nodes
            .iter()
            .find(|(hash, _)| *hash == root_hash)
            .map(|(_, bytes)| bytes.clone())
            .unwrap();
        let child_hash = node_child_hashes_raw(&root_bytes)
            .unwrap()
            .into_iter()
            .next()
            .expect("large flat root should have a child");

        // The substituted bytes are valid node bytes, but do not match the
        // structural hash under which they were supplied.
        nodes
            .iter_mut()
            .find(|(hash, _)| *hash == child_hash)
            .expect("child should be persisted")
            .1 = root_bytes.clone();

        let structural_key = room_structural_key_raw(room_id);
        assert!(lookup_state_entries_impl(&structural_key, &root_bytes, nodes, &[]).is_err());
    }

    #[test]
    fn lookup_state_entries_impl_tolerates_another_rooms_nodes_in_the_batch() {
        // Regression test for a real production bug: callers resolving
        // several state groups in one round (e.g.
        // `_lookup_state_hamt_from_postgres_many_txn`) share one fetched-node
        // pool across every group's `lookup_state_entries` call, so it can
        // legitimately contain nodes belonging to a different room. Eagerly
        // verifying every supplied node against *this* room's key used to
        // reject the lookup outright over a hash mismatch that was never a
        // real problem -- reproduced against real sytest/Complement failures
        // (cross-room backfill pagination, room-upgrade search) once
        // `lookup_state_entries` started verifying node hashes for real.
        let room_id = "!room:test.example";
        let other_room_id = "!other:test.example";

        let entries = vec![("m.room.name".to_owned(), "".to_owned(), "$name".to_owned())];
        let ((root_hash, _), nodes) =
            build_root_handle_and_nodes(room_id, entries).expect("HAMT root should build");
        let root_node_bytes = nodes
            .iter()
            .find(|(hash, _)| *hash == root_hash)
            .map(|(_, bytes)| bytes.clone())
            .expect("root node bytes should be present");

        let other_entries = vec![(
            "m.room.name".to_owned(),
            "".to_owned(),
            "$other-name".to_owned(),
        )];
        let ((_other_root_hash, _), other_nodes) =
            build_root_handle_and_nodes(other_room_id, other_entries)
                .expect("HAMT root should build");

        // Simulate the shared multi-room fetch pool: this room's own nodes,
        // plus another room's, exactly as `_lookup_state_hamt_from_postgres_many_txn`
        // would hand them to `lookup_state_entries`.
        let combined_nodes: Vec<(StructuralHash, Vec<u8>)> =
            nodes.into_iter().chain(other_nodes).collect();

        let structural_key = room_structural_key_raw(room_id);
        let keys = vec![("m.room.name".to_owned(), "".to_owned())];

        let (entries, missing) =
            lookup_state_entries_impl(&structural_key, &root_node_bytes, combined_nodes, &keys)
                .expect(
                    "lookup must not fail just because the batch also carries another room's nodes",
                );
        assert!(missing.is_empty());
        assert_eq!(
            entries,
            vec![("m.room.name".to_owned(), "".to_owned(), "$name".to_owned())]
        );
    }

    #[test]
    fn unreachable_node_hashes_reports_orphan_root() {
        let room_id = "!room:test.example";
        let live_entries = vec![("m.room.name".to_owned(), "".to_owned(), "$live".to_owned())];
        let orphan_entries = vec![(
            "m.room.topic".to_owned(),
            "".to_owned(),
            "$orphan".to_owned(),
        )];

        let ((_, _), live_nodes) = build_root_handle_and_nodes(room_id, live_entries)
            .expect("live HAMT root should build");
        let ((_, _), orphan_nodes) = build_root_handle_and_nodes(room_id, orphan_entries)
            .expect("orphan HAMT root should build");

        let (live_root_hash, _live_root_bytes) = live_nodes
            .last()
            .cloned()
            .expect("live root node should exist");
        let (orphan_root_hash, _) = orphan_nodes
            .last()
            .cloned()
            .expect("orphan root node should exist");
        let all_nodes: Vec<_> = live_nodes
            .into_iter()
            .chain(orphan_nodes)
            .map(|(hash, bytes)| (hash.to_vec(), bytes))
            .collect();
        let universe: Vec<_> = all_nodes.iter().map(|(hash, _)| hash.clone()).collect();

        let (reachable, unreachable) =
            reachability_audit(vec![live_root_hash.to_vec()], universe, all_nodes)
                .expect("reachability audit should succeed");

        assert!(reachable.contains(&live_root_hash.to_vec()));
        assert!(!reachable.contains(&orphan_root_hash.to_vec()));
        assert!(unreachable.contains(&orphan_root_hash.to_vec()));
        assert!(!unreachable.contains(&live_root_hash.to_vec()));
    }

    #[test]
    fn multi_room_selective_lookup_divergent_shapes() {
        let room_id_1 = "!room1:test.example";
        let room_id_2 = "!room2:test.example";
        let key_1 = room_structural_key_raw(room_id_1);
        let key_2 = room_structural_key_raw(room_id_2);

        // Room 1: 50 member events (deep tree) + room name
        let mut entries_1 = vec![("m.room.name".to_owned(), "".to_owned(), "$name1".to_owned())];
        for i in 0..50 {
            entries_1.push((
                "m.room.member".to_owned(),
                format!("@user{i}:test.example"),
                format!("$event_1_{i}"),
            ));
        }

        // Room 2: different types (topic, join_rules, power_levels)
        let entries_2 = vec![
            (
                "m.room.topic".to_owned(),
                "".to_owned(),
                "$topic2".to_owned(),
            ),
            (
                "m.room.join_rules".to_owned(),
                "".to_owned(),
                "$rules2".to_owned(),
            ),
            (
                "m.room.power_levels".to_owned(),
                "".to_owned(),
                "$power2".to_owned(),
            ),
        ];

        let ((root_hash_1, _), nodes_1) =
            build_root_handle_and_nodes(room_id_1, entries_1).unwrap();
        let ((root_hash_2, _), nodes_2) =
            build_root_handle_and_nodes(room_id_2, entries_2).unwrap();

        // Shared multi-room node map (simulating the shared node cache/fetcher)
        let mut combined_nodes: HashMap<StructuralHash, Arc<HamtNode<String, String>>> =
            HashMap::new();
        for (h, node_bytes) in nodes_1.into_iter().chain(nodes_2) {
            let node = decode_persisted_node_unverified(&node_bytes, h).unwrap();
            combined_nodes.insert(h, node);
        }

        // 1. Look up m.room.member @user25:test.example in Room 1
        let query_1 = vec![(
            "m.room.member".to_owned(),
            "@user25:test.example".to_owned(),
        )];
        let (found_1, missing_1) =
            lookup_from_node_map(&root_hash_1, &key_1, &query_1, &combined_nodes).unwrap();
        assert!(missing_1.is_empty());
        assert_eq!(
            found_1,
            vec![(
                "m.room.member".to_owned(),
                "@user25:test.example".to_owned(),
                "$event_1_25".to_owned()
            )]
        );

        // 2. Look up m.room.topic and m.room.power_levels in Room 2
        let query_2 = vec![
            ("m.room.topic".to_owned(), "".to_owned()),
            ("m.room.power_levels".to_owned(), "".to_owned()),
        ];
        let (found_2, missing_2) =
            lookup_from_node_map(&root_hash_2, &key_2, &query_2, &combined_nodes).unwrap();
        assert!(missing_2.is_empty());
        assert_eq!(found_2.len(), 2);
        assert!(found_2.contains(&(
            "m.room.topic".to_owned(),
            "".to_owned(),
            "$topic2".to_owned()
        )));
        assert!(found_2.contains(&(
            "m.room.power_levels".to_owned(),
            "".to_owned(),
            "$power2".to_owned()
        )));

        // 3. Verify cross-room isolation: Querying room 2 for room 1's key returns empty
        let (found_isolated, missing_isolated) =
            lookup_from_node_map(&root_hash_2, &key_2, &query_1, &combined_nodes).unwrap();
        assert!(missing_isolated.is_empty());
        assert!(found_isolated.is_empty());
    }
}
