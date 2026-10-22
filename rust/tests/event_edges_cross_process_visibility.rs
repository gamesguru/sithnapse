//! Regression test for `event_edges` forward-read cross-process visibility:
//! does a read-only worker process, already open *before* a writer commits,
//! observe the new edge on a read, without reopening?
//!
//! This is deliberately a harder scenario than `event_edges_restart.rs`
//! (which reopens fresh *after* the write, so it always sees the durable
//! on-disk state). Here the reader opens once, then the writer commits, then
//! the reader reads through its already-open handle -- the shape a real
//! worker process is in between its own commits/refreshes.
//!
//! Originally written as an open-ended probe: `event_edges_get_forward` used
//! plain `get_many`, which per mtxdb's `StorageEngine::get_many` only
//! redirects to the read-journal overlay while `transaction_overlay_users
//! != 0` (mid-publish), not unconditionally -- unlike `get_read_committed`,
//! which checks it unconditionally. That predicted (and this test then
//! confirmed, 5/5 runs) a miss here. `event_edges_get_forward` was switched
//! to `get_read_committed` afterward
//! (`res/docs/2026-09-28-event-edge-cross-process-read-visibility.md`), so
//! this is now a regression test asserting the fixed behavior (a hit),
//! not an open probe.
//!
//! Two processes, synchronized by small marker files (no shared clock
//! assumptions): reader opens and signals ready; writer waits for that
//! signal, writes+commits, signals done; reader waits for the done signal,
//! then reads immediately through its already-open handle.

use std::fs;
use std::path::Path;
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use synapse::database::embedded_edges::{event_edges_get_forward, event_edges_put};
use synapse::database::mtxdb_syn::{open_client, open_client_read_only, sync_auth_chain};

const DIR_ENV: &str = "SYNAPSE_TEST_EDGES_VISIBILITY_DIR";
const PHASE_ENV: &str = "SYNAPSE_TEST_EDGES_VISIBILITY_PHASE";
const TEST_NAME: &str = "reader_open_before_write_observes_it_via_read_committed";
const NAMESPACE: &str = "integration-edges-visibility";
const ROOM: &str = "!integration-edges-visibility:example.org";
const PARENT: &str = "$visibility-parent";
const CHILD: &str = "$visibility-child";

fn marker_path(dir: &str, name: &str) -> std::path::PathBuf {
    Path::new(dir).join(name)
}

fn wait_for_marker(path: &Path, timeout: Duration) {
    let started = Instant::now();
    while !path.is_file() {
        assert!(
            started.elapsed() < timeout,
            "timed out waiting for marker {path:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn reader_phase(py: Python<'_>, dir: &str) {
    // The on-disk layout (pool directories/shards) doesn't exist until the
    // writer has opened at least once, so wait for that -- this is before
    // the write itself, not before the writer process starts.
    wait_for_marker(&marker_path(dir, "store_ready"), Duration::from_secs(30));

    // Open BEFORE the writer's commit: this is the case a plain get_many
    // cannot see past without its own refresh -- the case `get_read_committed`
    // exists for.
    open_client_read_only(py, dir.to_owned()).expect("reader open (before writer's write)");
    fs::write(marker_path(dir, "reader_ready"), b"").expect("write reader_ready marker");

    wait_for_marker(&marker_path(dir, "writer_done"), Duration::from_secs(30));

    // Single read attempt, no retry/refresh loop: this is exactly what a
    // caller gets today (`get_read_committed`, which checks the read-journal
    // overlay unconditionally rather than only mid-publish).
    let forward = event_edges_get_forward(py, NAMESPACE.to_owned(), vec![PARENT.to_owned()])
        .expect("forward read after writer's commit");
    let observed_child = forward[0]
        .1
        .as_ref()
        .is_some_and(|c| c.contains(&CHILD.to_owned()));

    fs::write(
        marker_path(dir, "reader_result"),
        if observed_child {
            b"hit" as &[u8]
        } else {
            b"miss"
        },
    )
    .expect("write reader_result marker");
}

fn writer_phase(py: Python<'_>, dir: &str) {
    open_client(py, dir.to_owned()).expect("writer open");
    fs::write(marker_path(dir, "store_ready"), b"").expect("write store_ready marker");
    wait_for_marker(&marker_path(dir, "reader_ready"), Duration::from_secs(30));

    event_edges_put(
        py,
        NAMESPACE.to_owned(),
        vec![(ROOM.to_owned(), CHILD.to_owned(), PARENT.to_owned(), false)],
    )
    .expect("writer put");
    sync_auth_chain(py).expect("sync edges pool");

    fs::write(marker_path(dir, "writer_done"), b"").expect("write writer_done marker");
}

#[test]
fn reader_open_before_write_observes_it_via_read_committed() {
    if let Ok(dir) = std::env::var(DIR_ENV) {
        Python::initialize();
        let phase = std::env::var(PHASE_ENV).expect("phase env set by the parent");
        Python::attach(|py| match phase.as_str() {
            "reader" => reader_phase(py, &dir),
            "writer" => writer_phase(py, &dir),
            other => panic!("unknown phase {other:?}"),
        });
        return;
    }

    // SAFETY: single-threaded test-binary startup, before any child reads it.
    unsafe {
        std::env::set_var("SYNAPSE_MTXDB_WAL", "1");
    }
    let root = tempfile::tempdir().expect("temporary mtxdb directory");
    let dir = root.path().to_string_lossy().into_owned();

    // Reader spawned first so it can open before the writer commits; the
    // reader_ready/writer_done markers are the actual ordering guarantee,
    // not spawn order.
    let mut reader = spawn_phase(&dir, "reader");
    let mut writer = spawn_phase(&dir, "writer");

    let writer_status = wait_bounded(&mut writer, "writer");
    assert!(writer_status.success(), "writer failed: {writer_status:?}");
    let reader_status = wait_bounded(&mut reader, "reader");
    assert!(reader_status.success(), "reader failed: {reader_status:?}");

    let result = fs::read_to_string(marker_path(&dir, "reader_result"))
        .expect("reader_result marker written");
    println!(
        "cross-process visibility: reader opened before writer's commit, \
         single post-commit get_read_committed read: {result}"
    );
    assert_eq!(
        result, "hit",
        "a reader opened before the writer's commit must observe it on a \
         get_read_committed read -- if this is 'miss', event_edges_get_forward \
         has regressed off get_read_committed back onto a plain read"
    );
}

fn spawn_phase(dir: &str, phase: &str) -> Child {
    spawn_test_phase(TEST_NAME, dir, phase)
}

fn spawn_test_phase(test_name: &str, dir: &str, phase: &str) -> Child {
    Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(DIR_ENV, dir)
        .env(PHASE_ENV, phase)
        .spawn()
        .expect("spawn child process")
}

// --- Repeated-refresh regression: the same long-lived reader must keep
// picking up *every* subsequent published group, not just the first one
// after open. This is the generation-signal path specifically: mtxdb#af82b409
// ("perf(packfile): skip the read-committed stat via a cross-process publish
// signal", res/docs/2026-09-28-event-edge-cross-process-read-visibility.md)
// only skips the real refresh when its sampled generation is unchanged from
// the last one it actually refreshed through -- a reader that only ever
// checked once, or that got stuck on a stale cached generation, would still
// pass a single-write test but silently miss every write after the first.
// Two sequential writer commits against the same parent, with the same
// reader reading in between and after, rules that out.

const REPEATED_TEST_NAME: &str = "reader_observes_each_subsequent_published_group";
const REPEATED_PARENT: &str = "$repeated-refresh-parent";
const REPEATED_CHILD_1: &str = "$repeated-refresh-child-1";
const REPEATED_CHILD_2: &str = "$repeated-refresh-child-2";

fn repeated_writer_phase(py: Python<'_>, dir: &str) {
    open_client(py, dir.to_owned()).expect("writer open");
    fs::write(marker_path(dir, "store_ready"), b"").expect("write store_ready marker");
    wait_for_marker(&marker_path(dir, "reader_ready"), Duration::from_secs(30));

    event_edges_put(
        py,
        NAMESPACE.to_owned(),
        vec![(
            ROOM.to_owned(),
            REPEATED_CHILD_1.to_owned(),
            REPEATED_PARENT.to_owned(),
            false,
        )],
    )
    .expect("writer put 1");
    sync_auth_chain(py).expect("sync edges pool after put 1");
    fs::write(marker_path(dir, "writer_done_1"), b"").expect("write writer_done_1 marker");

    // Wait for the reader to actually observe the first write before doing
    // the second: otherwise a reader that happens to catch up to both writes
    // in one refresh (because it was slow to get to its first read) would
    // pass even if it could only ever refresh once.
    wait_for_marker(&marker_path(dir, "reader_ack_1"), Duration::from_secs(30));

    event_edges_put(
        py,
        NAMESPACE.to_owned(),
        vec![(
            ROOM.to_owned(),
            REPEATED_CHILD_2.to_owned(),
            REPEATED_PARENT.to_owned(),
            false,
        )],
    )
    .expect("writer put 2");
    sync_auth_chain(py).expect("sync edges pool after put 2");
    fs::write(marker_path(dir, "writer_done_2"), b"").expect("write writer_done_2 marker");
}

fn repeated_reader_phase(py: Python<'_>, dir: &str) {
    wait_for_marker(&marker_path(dir, "store_ready"), Duration::from_secs(30));
    open_client_read_only(py, dir.to_owned()).expect("reader open (before either write)");
    fs::write(marker_path(dir, "reader_ready"), b"").expect("write reader_ready marker");

    wait_for_marker(&marker_path(dir, "writer_done_1"), Duration::from_secs(30));
    let after_first =
        event_edges_get_forward(py, NAMESPACE.to_owned(), vec![REPEATED_PARENT.to_owned()])
            .expect("forward read after first commit");
    let children_after_first: Vec<String> = after_first[0].1.clone().unwrap_or_default();
    fs::write(
        marker_path(dir, "reader_result_1"),
        children_after_first.join(","),
    )
    .expect("write reader_result_1 marker");
    fs::write(marker_path(dir, "reader_ack_1"), b"").expect("write reader_ack_1 marker");

    wait_for_marker(&marker_path(dir, "writer_done_2"), Duration::from_secs(30));
    // Same already-open handle, no reopen: this is the case a reader stuck on
    // a stale cached generation would fail.
    let after_second =
        event_edges_get_forward(py, NAMESPACE.to_owned(), vec![REPEATED_PARENT.to_owned()])
            .expect("forward read after second commit");
    let mut children_after_second: Vec<String> = after_second[0].1.clone().unwrap_or_default();
    children_after_second.sort();
    fs::write(
        marker_path(dir, "reader_result_2"),
        children_after_second.join(","),
    )
    .expect("write reader_result_2 marker");
}

#[test]
fn reader_observes_each_subsequent_published_group() {
    if let Ok(dir) = std::env::var(DIR_ENV) {
        Python::initialize();
        let phase = std::env::var(PHASE_ENV).expect("phase env set by the parent");
        Python::attach(|py| match phase.as_str() {
            "reader" => repeated_reader_phase(py, &dir),
            "writer" => repeated_writer_phase(py, &dir),
            other => panic!("unknown phase {other:?}"),
        });
        return;
    }

    // SAFETY: single-threaded test-binary startup, before any child reads it.
    unsafe {
        std::env::set_var("SYNAPSE_MTXDB_WAL", "1");
    }
    let root = tempfile::tempdir().expect("temporary mtxdb directory");
    let dir = root.path().to_string_lossy().into_owned();

    let mut reader = spawn_test_phase(REPEATED_TEST_NAME, &dir, "reader");
    let mut writer = spawn_test_phase(REPEATED_TEST_NAME, &dir, "writer");

    let writer_status = wait_bounded(&mut writer, "writer");
    assert!(writer_status.success(), "writer failed: {writer_status:?}");
    let reader_status = wait_bounded(&mut reader, "reader");
    assert!(reader_status.success(), "reader failed: {reader_status:?}");

    let result_1 = fs::read_to_string(marker_path(&dir, "reader_result_1"))
        .expect("reader_result_1 marker written");
    let result_2 = fs::read_to_string(marker_path(&dir, "reader_result_2"))
        .expect("reader_result_2 marker written");

    assert_eq!(
        result_1, REPEATED_CHILD_1,
        "after the first commit, the reader must see exactly child 1 (not \
         zero children -- a stale generation -- and not child 2, which \
         hasn't been written yet)"
    );

    let mut expected_after_second = [REPEATED_CHILD_1.to_string(), REPEATED_CHILD_2.to_string()];
    expected_after_second.sort();
    assert_eq!(
        result_2,
        expected_after_second.join(","),
        "after the second commit, the SAME already-open reader must see both \
         children -- if this only shows child 1, the reader refreshed once \
         and then got stuck on a stale cached generation instead of \
         continuing to detect every subsequent published group"
    );
}

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
