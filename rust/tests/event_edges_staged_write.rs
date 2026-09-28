//! Coverage for `event_edges_put`'s staged-transaction path.
//!
//! `event_edges_put` now stages its backward records, forward-list rewrite,
//! and locators through one `DatabaseTransaction` (one journal group) when a
//! shared WAL is open, instead of three direct `put_many` calls. The unit
//! tests in `embedded_edges.rs` open the store without `SYNAPSE_MTXDB_WAL`
//! (see `auth_chain_closure_tests::ensure_open`), so they only exercise the
//! direct-engine fallback; this binary sets the WAL env var before its first
//! `open_client`, so it exercises the staged path instead.
//!
//! What needs proving here is that staging doesn't change behavior: the
//! forward-list read-modify-write still merges/dedupes correctly, and a
//! second `event_edges_put` call sees the first one's committed result
//! (proving the transaction actually published, not just staged and
//! discarded).

use pyo3::prelude::*;
use synapse::database::embedded_edges::{
    event_edges_get_backward, event_edges_get_forward, event_edges_put,
};
use synapse::database::mtxdb_syn::open_client;

const NAMESPACE: &str = "integration-edges-staged-write";
const ROOM: &str = "!integration-edges-staged-write:example.org";

#[test]
fn staged_put_merges_forward_lists_and_publishes_backward_edges() {
    // SAFETY: single-threaded test-binary startup, before `open_client`
    // reads it (see `mtxdb_transaction_get_edges.rs`'s `open_wal_store`).
    unsafe {
        std::env::set_var("SYNAPSE_MTXDB_WAL", "1");
    }
    let dir = tempfile::tempdir().expect("tempdir");
    Python::initialize();
    Python::attach(|py| {
        open_client(py, dir.path().to_string_lossy().into_owned())
            .expect("open_client with WAL enabled");

        let parent = "$staged-parent".to_string();

        // First call: two children on one parent.
        event_edges_put(
            py,
            NAMESPACE.to_string(),
            vec![
                (
                    ROOM.to_string(),
                    "$staged-child-1".to_string(),
                    parent.clone(),
                    false,
                ),
                (
                    ROOM.to_string(),
                    "$staged-child-2".to_string(),
                    parent.clone(),
                    false,
                ),
            ],
        )
        .expect("first staged put");

        // Second call: a third child on the same parent, plus a repeat of
        // child 1 (must dedupe, not double up). Reading the committed
        // result of the first call back and merging it correctly is exactly
        // the read-modify-write staging must get right.
        event_edges_put(
            py,
            NAMESPACE.to_string(),
            vec![
                (
                    ROOM.to_string(),
                    "$staged-child-1".to_string(),
                    parent.clone(),
                    false,
                ),
                (
                    ROOM.to_string(),
                    "$staged-child-3".to_string(),
                    parent.clone(),
                    false,
                ),
            ],
        )
        .expect("second staged put");

        let forward = event_edges_get_forward(py, NAMESPACE.to_string(), vec![parent.clone()])
            .expect("read forward after both staged puts");
        let mut children = forward[0].1.clone().expect("parent has children");
        children.sort();
        assert_eq!(
            children,
            vec![
                "$staged-child-1".to_string(),
                "$staged-child-2".to_string(),
                "$staged-child-3".to_string(),
            ],
            "both staged puts must have committed and merged, with no duplicate"
        );

        let backward = event_edges_get_backward(
            py,
            NAMESPACE.to_string(),
            vec!["$staged-child-3".to_string()],
        )
        .expect("read backward for a child written by the staged path");
        assert_eq!(
            backward[0].1,
            Some(vec![(parent.clone(), false)]),
            "the staged backward record must be committed and readable"
        );
    });
}
