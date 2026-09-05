//! Cumulative incremental-vs-full-rebuild cost for the flat HAMT state store.
//!
//! Modeled directly on rezzy's `benches/db/cumulative_rebuild.rs`: build a
//! room's state from empty up to `S` entries, one mutation at a time, and at
//! *every* checkpoint pay whatever that step's real cost is at the state's
//! *current* size -- then sum it. This is what
//! `docs/development-gg/persistent-typed-hamt-architecture.md` calls the
//! "O(S)-per-update tax" this branch's incremental path-copying
//! (`apply_flat_state_updates`) exists to fix, made concrete and measured
//! rather than asserted:
//!
//! | strategy                          | per-op cost      |
//! |------------------------------------|------------------|
//! | full rebuild from `current_state_ids` (`build_root_handle_with_lattice`) | O(S)      |
//! | incremental path-copying (`apply_flat_state_updates`)                    | O(K log S) |
//!
//! `K` is the size of the delta (1 for a single state event). At S entries,
//! a full rebuild always re-hashes and re-persists every entry; incremental
//! apply only touches the ~log32(S) trie nodes on the path from the changed
//! leaf to the root.
//!
//! Run with: `cargo bench --bench state_hamt`
#![allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::items_after_statements,
    clippy::doc_markdown,
    clippy::print_stdout
)]

use std::time::Instant;

use synapse::state_hamt::{apply_flat_state_updates, build_root_handle_with_lattice};

const S_MAX: usize = 4096;
const CHECKPOINTS: &[usize] = &[16, 64, 256, 1024, 2048, 4096];
const ROOM_ID: &str = "!bench-cumulative-rebuild:example.com";

/// One entry per step: a distinct `(event_type, state_key)` so every
/// mutation actually inserts a new leaf rather than overwriting one already
/// present -- this is what makes the state grow to size `S` over `S` steps.
fn entry(i: usize) -> (String, String, String) {
    (
        "m.room.member".to_owned(),
        format!("@user{i}:example.com"),
        format!("$event{i}"),
    )
}

fn main() {
    println!("cumulative_rebuild: full-rebuild vs incremental path-copying");
    println!("(building a room's state from empty to {S_MAX} entries, one event at a time)");
    println!();
    println!(
        "{:>8}  {:>18}  {:>18}  {:>10}  {:>16}",
        "size", "full rebuild", "incremental apply", "speedup", "cumul. node reads"
    );

    // --- Full rebuild: at each checkpoint, rebuild from the complete
    // current_state_ids map, as `store_state_group` does when it has no
    // usable prev_group root (its documented worst case).
    let mut full_rebuild_totals: Vec<(usize, f64)> = Vec::new();
    {
        let mut state: Vec<(String, String, String)> = Vec::new();
        let mut checkpoint_idx = 0;
        let mut cumulative = 0.0f64;
        for i in 0..S_MAX {
            state.push(entry(i));
            let start = Instant::now();
            build_root_handle_with_lattice(ROOM_ID, state.clone())
                .expect("full rebuild should succeed");
            cumulative += start.elapsed().as_secs_f64();

            let size = i + 1;
            if checkpoint_idx < CHECKPOINTS.len() && size == CHECKPOINTS[checkpoint_idx] {
                full_rebuild_totals.push((size, cumulative));
                checkpoint_idx += 1;
            }
        }
    }

    // --- Incremental path-copying: build the root once, then apply each
    // subsequent event as a single-key delta via apply_flat_state_updates,
    // exactly as `store_state_group`/`_persist_state_hamt_incremental_txn`
    // do when a prev_group root is available.
    //
    // Critically, this must NOT hand the function the entire accumulated
    // node history on every call -- that would silently turn "incremental"
    // into O(total nodes ever created) and defeat the point of the
    // benchmark. Production only ever prefetches a *sparse* local cache
    // and lets the function report which specific node hashes it still
    // needs (`ApplyOutcome::Missing`), fetching just those from persistent
    // storage (`backing_store` here stands in for SQL/the embedded engine)
    // and retrying. That fetch-on-demand loop is reproduced
    // below, starting from nothing but the current root.
    let mut incremental_totals: Vec<(usize, f64)> = Vec::new();
    let mut incremental_node_reads: Vec<(usize, usize)> = Vec::new();
    {
        let mut backing_store: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        let start = Instant::now();
        let (root_hash, _sg, lattice_bytes, nodes) =
            build_root_handle_with_lattice(ROOM_ID, vec![entry(0)])
                .expect("initial build should succeed");
        let mut cumulative = start.elapsed().as_secs_f64();
        backing_store.extend(nodes);
        let mut root_hash = root_hash;
        let mut lattice_bytes = lattice_bytes;

        let mut checkpoint_idx = 0;
        let mut cumulative_node_reads = 0usize;
        for i in 1..S_MAX {
            let (event_type, state_key, event_id) = entry(i);
            let root_node_bytes = backing_store[&root_hash].clone();

            let start = Instant::now();
            let mut local_nodes: Vec<(Vec<u8>, Vec<u8>)> =
                vec![(root_hash.clone(), root_node_bytes.clone())];
            let (applied, node_reads) = loop {
                let (applied, missing) = apply_flat_state_updates(
                    ROOM_ID,
                    root_node_bytes.clone(),
                    local_nodes.clone(),
                    lattice_bytes.clone(),
                    vec![(
                        event_type.clone(),
                        state_key.clone(),
                        Some(event_id.clone()),
                    )],
                )
                .expect("incremental apply should succeed");
                if missing.is_empty() {
                    break (applied, local_nodes.len());
                }
                for hash in missing {
                    local_nodes.push((hash.clone(), backing_store[&hash].clone()));
                }
            };
            cumulative += start.elapsed().as_secs_f64();
            cumulative_node_reads += node_reads;

            let (new_root_hash, _sg, new_lattice_bytes, new_nodes) =
                applied.expect("a single-key update always applies");
            backing_store.extend(new_nodes);
            root_hash = new_root_hash;
            lattice_bytes = new_lattice_bytes;

            let size = i + 1;
            if checkpoint_idx < CHECKPOINTS.len() && size == CHECKPOINTS[checkpoint_idx] {
                incremental_totals.push((size, cumulative));
                incremental_node_reads.push((size, cumulative_node_reads));
                checkpoint_idx += 1;
            }
        }
    }

    for ((size, full), (_, incremental), (_, node_reads)) in itertools_zip3(
        &full_rebuild_totals,
        &incremental_totals,
        &incremental_node_reads,
    ) {
        let speedup = if *incremental > 0.0 {
            full / incremental
        } else {
            f64::INFINITY
        };
        println!(
            "{size:>8}  {:>15.3}ms  {:>15.3}ms  {speedup:>9.1}x  {node_reads:>16}",
            full * 1000.0,
            incremental * 1000.0,
        );
    }
}

type Zip3Item<'a, A, B, C> = (&'a (usize, A), &'a (usize, B), &'a (usize, C));

/// Zips three slices of `(usize, T)` checkpoint pairs together.  This is
/// just `Iterator::zip` twice, named for readability at the call site.
fn itertools_zip3<'a, A, B, C>(
    a: &'a [(usize, A)],
    b: &'a [(usize, B)],
    c: &'a [(usize, C)],
) -> impl Iterator<Item = Zip3Item<'a, A, B, C>> {
    a.iter()
        .zip(b.iter())
        .zip(c.iter())
        .map(|((x, y), z)| (x, y, z))
}
