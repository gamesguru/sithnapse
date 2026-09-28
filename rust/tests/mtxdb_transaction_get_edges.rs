//! Coverage for `MtxdbTransaction::get_edges` / `put_edges`, the staged
//! edges-pool read/write pair that `event_edges_put`'s read-modify-write will
//! use once it stages inside the persist transaction instead of writing the
//! edges pool directly.
//!
//! `DatabaseTransaction::get` already exists in mtxdb (reads the stage first,
//! then falls back to the live pool); what needs proving here is the
//! *visibility* contract of the binding: a staged put is visible through the
//! transaction that staged it before `commit()`, becomes visible to other
//! transactions only after `commit()`, and is discarded by `abort()`.
//!
//! This needs its own process (a separate integration-test binary) because
//! `begin_transaction` only exists with the shared WAL open, which this
//! crate's process-global `OnceCell` decides once, from `SYNAPSE_MTXDB_WAL`,
//! on the first `open_client` call in the binary. A single test keeps that
//! one-shot open deterministic.

use pyo3::prelude::*;
use synapse::database::mtxdb_syn::{begin_transaction, open_client};

fn open_wal_store() -> tempfile::TempDir {
    // SAFETY: this test binary's own single-threaded startup, before any
    // other thread reads the environment; `wal_enabled()` is read exactly
    // once, from inside `open_client` below.
    unsafe {
        std::env::set_var("SYNAPSE_MTXDB_WAL", "1");
    }
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn staged_edges_are_read_by_their_own_transaction_and_published_on_commit() {
    let dir = open_wal_store();
    Python::initialize();
    Python::attach(|py| {
        open_client(py, dir.path().to_string_lossy().into_owned())
            .expect("open_client with WAL enabled");

        let txn = begin_transaction()
            .expect("begin_transaction")
            .expect("shared WAL must yield a transaction");

        // A committed edge: the staged put is visible to its own transaction
        // immediately, and to the next transaction only once committed.
        let committed = [7u8; 16];
        let committed_node = [9u8; 16];
        let committed_data = b"committed-edge".to_vec();

        assert_eq!(
            txn.get_edges(py, committed, vec![committed_node])
                .expect("live-pool miss before staging"),
            vec![None],
        );
        txn.put_edges(py, committed, committed_node, committed_data.clone())
            .expect("stage committed edge");
        assert_eq!(
            txn.get_edges(py, committed, vec![committed_node])
                .expect("staged read through the staging transaction"),
            vec![Some(committed_data.clone())],
        );

        txn.commit(py).expect("commit");

        let after_commit = begin_transaction()
            .expect("begin_transaction")
            .expect("shared WAL must yield a transaction");
        assert_eq!(
            after_commit
                .get_edges(py, committed, vec![committed_node])
                .expect("read after commit"),
            vec![Some(committed_data)],
        );
        after_commit.abort().expect("abort");

        // An aborted edge: visible to its own transaction, gone afterwards.
        let aborted = [3u8; 16];
        let aborted_node = [4u8; 16];

        let txn = begin_transaction()
            .expect("begin_transaction")
            .expect("shared WAL must yield a transaction");
        txn.put_edges(py, aborted, aborted_node, b"aborted-edge".to_vec())
            .expect("stage aborted edge");
        assert_eq!(
            txn.get_edges(py, aborted, vec![aborted_node])
                .expect("staged read before abort"),
            vec![Some(b"aborted-edge".to_vec())],
        );
        txn.abort().expect("abort");

        let after_abort = begin_transaction()
            .expect("begin_transaction")
            .expect("shared WAL must yield a transaction");
        assert_eq!(
            after_abort
                .get_edges(py, aborted, vec![aborted_node])
                .expect("read after abort"),
            vec![None],
        );
        after_abort.abort().expect("abort");
    });
}
