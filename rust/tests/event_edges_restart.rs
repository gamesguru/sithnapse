//! Out-of-process restart/recovery coverage for embedded event-edge tombstones
//! and the lazy forward-edge filter.
//!
//! The Python edge tests cannot express a real restart: the mtxdb Python
//! binding is a process-global `OnceCell`, so a store opened once in a process
//! is never reopened. This test instead re-executes its own test binary twice:
//!
//! 1. a **writer** child opens a fresh store, writes two children on one
//!    parent, tombstones one -- deliberately leaving the parent's forward list
//!    stale, since `event_edges_delete` never rewrites forward lists -- and
//!    syncs the edges pool;
//! 2. a **reader** child, a separate process, opens the same store fresh and
//!    asserts the tombstoned child is still filtered out of the forward read,
//!    the live sibling survives, and the tombstone itself persisted.
//!
//! That is the `embedded-edge-tombstones.md` "restart and recovery" scenario:
//! the filter's decision must rest only on persisted state, never on anything
//! the deleting process held in memory.

use std::process::{Child, Command, ExitStatus};
use std::sync::Once;
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use synapse::database::embedded_edges::{
    event_edges_delete, event_edges_get_backward, event_edges_get_forward, event_edges_put,
};
use synapse::database::mtxdb_syn::{open_client, sync_auth_chain};

const DIR_ENV: &str = "SYNAPSE_TEST_EDGES_RESTART_DIR";
const PHASE_ENV: &str = "SYNAPSE_TEST_EDGES_RESTART_PHASE";
const TEST_NAME: &str = "event_edges_tombstones_survive_restart";
const NAMESPACE: &str = "integration-edges-restart";
const ROOM: &str = "!integration-edges-restart:example.org";
const PARENT: &str = "$restart-parent";
const LIVE: &str = "$restart-live";
const DEAD: &str = "$restart-dead";

static PYTHON: Once = Once::new();

fn initialize_python() {
    PYTHON.call_once(Python::initialize);
}

fn write_phase(py: Python<'_>, dir: &str) {
    open_client(py, dir.to_owned()).expect("open store for writing");
    event_edges_put(
        py,
        NAMESPACE.to_owned(),
        vec![
            (ROOM.to_owned(), LIVE.to_owned(), PARENT.to_owned(), false),
            (ROOM.to_owned(), DEAD.to_owned(), PARENT.to_owned(), false),
        ],
    )
    .expect("write edges");
    // Tombstone one child. The parent's forward list is deliberately left
    // holding both child ids; only the backward record marks DEAD deleted.
    event_edges_delete(py, NAMESPACE.to_owned(), vec![DEAD.to_owned()]).expect("tombstone child");
    sync_auth_chain(py).expect("sync edges pool");
}

fn read_phase(py: Python<'_>, dir: &str) {
    open_client(py, dir.to_owned()).expect("reopen store in a fresh process");

    let forward = event_edges_get_forward(py, NAMESPACE.to_owned(), vec![PARENT.to_owned()])
        .expect("read forward after restart");
    assert_eq!(forward.len(), 1);
    assert_eq!(
        forward[0].1,
        Some(vec![LIVE.to_owned()]),
        "the tombstoned child must still be filtered out after restart"
    );

    let backward_live = event_edges_get_backward(py, NAMESPACE.to_owned(), vec![LIVE.to_owned()])
        .expect("read live backward after restart");
    assert_eq!(
        backward_live[0].1,
        Some(vec![(PARENT.to_owned(), false)]),
        "the live child's backward edge must survive restart"
    );

    let backward_dead = event_edges_get_backward(py, NAMESPACE.to_owned(), vec![DEAD.to_owned()])
        .expect("read tombstoned backward after restart");
    assert!(
        backward_dead[0].1.is_none(),
        "the explicit tombstone must survive restart, not read as absent-legacy"
    );
}

#[test]
fn event_edges_tombstones_survive_restart() {
    if let Ok(dir) = std::env::var(DIR_ENV) {
        initialize_python();
        let phase = std::env::var(PHASE_ENV).expect("phase env set by the parent");
        Python::attach(|py| match phase.as_str() {
            "write" => write_phase(py, &dir),
            "read" => read_phase(py, &dir),
            other => panic!("unknown phase {other:?}"),
        });
        return;
    }

    let root = tempfile::tempdir().expect("temporary mtxdb directory");
    let dir = root.path().to_string_lossy().into_owned();

    // Two separate processes, so the reader cannot reuse the writer's handles
    // or any in-memory tombstone state.
    run_phase(&dir, "write");
    run_phase(&dir, "read");
}

/// Spawn this test binary to run `TEST_NAME` in `phase` against `dir`, and
/// require it to exit successfully within the deadline.
fn run_phase(dir: &str, phase: &str) {
    let mut child = spawn_phase(dir, phase).expect("spawn child process");
    let status = wait_bounded(&mut child, phase);
    assert!(
        status.success(),
        "the {phase} child process failed: {status:?}"
    );
}

fn spawn_phase(dir: &str, phase: &str) -> std::io::Result<Child> {
    Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(DIR_ENV, dir)
        .env(PHASE_ENV, phase)
        .spawn()
}

/// Wait with a deadline. A plain `wait()` can block forever, so poll and kill
/// on timeout before panicking.
fn wait_bounded(child: &mut Child, phase: &str) -> ExitStatus {
    let started = Instant::now();
    let timeout = Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().expect("poll child process") {
            return status;
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the {phase} child process did not exit within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
