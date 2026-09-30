//! Measures the actual cost of `event_edges_get_forward`'s switch from plain
//! `get_many` to `get_read_committed` (`da7027d53`), on a genuine miss and a
//! genuine hit -- flagged twice as unmeasured (review, and the corrected doc
//! comment in `embedded_edges.rs`) since the real mechanism is
//! `get_many_with_refresh`'s refresh-on-miss path (a lock + durable-
//! fingerprint check, possibly a rescan), not a free overlay check.
//!
//! Two scenarios, because the cost model differs by process role:
//!
//! - **Writer reads its own writes** (`open_client`): the writer never
//!   installs the read-journal overlay for itself and explicitly disables
//!   `refresh_on_miss` (`mtxdb_syn.rs`, its own in-memory index is already
//!   authoritative for everything it wrote). `get_read_committed` should
//!   degrade to a pure passthrough to plain `get_many` here -- this is the
//!   single-process deployment shape, the common case.
//! - **Read-only worker reads** (`open_client_read_only`, shared WAL): this
//!   is where `refresh_on_miss` stays enabled and the read-journal overlay is
//!   installed, so this is where the real cost (if any) lives. Needs a
//!   separate process (own binary), same pattern as
//!   `event_edges_cross_process_visibility.rs`.
//!
//! Run with: `cargo test -p synapse --test event_edges_read_committed_cost --
//! --nocapture --test-threads=1`

use std::fs;
use std::path::Path;
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use synapse::database::embedded_edges::{event_edges_get_forward, event_edges_put};
use synapse::database::mtxdb_syn::{open_client, open_client_read_only, stats, sync};

const NAMESPACE: &str = "bench-edges-read-committed-cost";
const ROOM: &str = "!bench-edges-read-committed-cost:example.org";
const N_PARENTS: usize = 5_000;
const ITERATIONS: usize = 2_000;

fn percentiles(mut samples: Vec<f64>) -> (f64, f64) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = samples[samples.len() / 2];
    let p99 = samples[(samples.len() * 99) / 100];
    (p50 * 1e6, p99 * 1e6)
}

/// Event-DAG pool `(read_refreshes, read_refresh_bytes)`.
///
/// These are the always-on counters `refresh_read_journal` bumps
/// (`read_journal.rs:807,703`), so a read run's rescan frequency and scanned
/// volume can be attributed without enabling the opt-in logical counters.
fn event_dag_refresh_counters(py: Python<'_>) -> (u64, u64) {
    let all = stats(py).expect("stats");
    let all = all.bind(py);
    let pool = all
        .get_item("event_dag")
        .expect("get event_dag")
        .expect("event_dag present")
        .cast_into::<PyDict>()
        .expect("event_dag is a dict");
    let refreshes = pool
        .get_item("read_refreshes")
        .expect("get read_refreshes")
        .expect("read_refreshes present")
        .extract::<u64>()
        .expect("read_refreshes is u64");
    let bytes = pool
        .get_item("read_refresh_bytes")
        .expect("get read_refresh_bytes")
        .expect("read_refresh_bytes present")
        .extract::<u64>()
        .expect("read_refresh_bytes is u64");
    (refreshes, bytes)
}

/// Returns `(parents, leaf_children)`. `parents` are real hits: each has a
/// forward list (one child). `leaf_children` are the realistic *miss* shape:
/// each is a real, mirrored event (it has a locator, published for every
/// event_id/prev_event_id `event_edges_put` touches) whose own forward node
/// was simply never written, because nothing has cited it as a `prev_event`
/// yet -- unlike an id that was never mentioned at all, which misses at the
/// locator-resolution phase before ever reaching the forward-node lookup
/// this benchmark means to measure.
fn populate(py: Python<'_>) -> (Vec<String>, Vec<String>) {
    let mut parents = Vec::with_capacity(N_PARENTS);
    let mut leaf_children = Vec::with_capacity(N_PARENTS);
    let rows: Vec<(String, String, String, bool)> = (0..N_PARENTS)
        .map(|i| {
            let parent = format!("$cost-parent-{i}");
            let child = format!("$cost-child-{i}");
            parents.push(parent.clone());
            leaf_children.push(child.clone());
            (ROOM.to_owned(), child, parent, false)
        })
        .collect();
    event_edges_put(py, NAMESPACE.to_owned(), rows).expect("populate");
    // All three pools, not just edges: open_client_read_only opens all
    // three, and the state pool's read-journal overlay failed to enable
    // ("read-committed reload failed or checkpoint coverage advanced")
    // when only the edges pool had ever been synced/checkpointed.
    sync(py).expect("sync");
    (parents, leaf_children)
}

fn time_reads(py: Python<'_>, keys: &[String]) -> (f64, f64) {
    let mut samples = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let key = &keys[i % keys.len()];
        let started = Instant::now();
        event_edges_get_forward(py, NAMESPACE.to_owned(), vec![key.clone()]).expect("read");
        samples.push(started.elapsed().as_secs_f64());
    }
    percentiles(samples)
}

#[test]
fn writer_reads_its_own_writes_no_regression() {
    Python::initialize();
    let dir = tempfile::tempdir().expect("tempdir");
    Python::attach(|py| {
        open_client(py, dir.path().to_string_lossy().into_owned()).expect("open writer");
        let (parents, leaf_children) = populate(py);

        let (refreshes_before, bytes_before) = event_dag_refresh_counters(py);
        let (hit_p50, hit_p99) = time_reads(py, &parents);
        let (miss_p50, miss_p99) = time_reads(py, &leaf_children);
        let (refreshes_after, bytes_after) = event_dag_refresh_counters(py);

        println!(
            "writer (own writes, no read-journal overlay): \
             hit  p50={hit_p50:8.1}us p99={hit_p99:8.1}us | \
             miss p50={miss_p50:8.1}us p99={miss_p99:8.1}us | \
             refresh_reads={} refresh_bytes={}",
            refreshes_after - refreshes_before,
            bytes_after - bytes_before,
        );
    });
}

// --- Cross-process: writer populates, a separate read-only worker times its
// own hit/miss reads through the shared WAL's overlay + refresh-on-miss path.

const DIR_ENV: &str = "SYNAPSE_TEST_EDGES_COST_DIR";
const PHASE_ENV: &str = "SYNAPSE_TEST_EDGES_COST_PHASE";
const TEST_NAME: &str = "worker_reads_via_read_journal_overlay";

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

fn writer_phase(py: Python<'_>, dir: &str) {
    open_client(py, dir.to_owned()).expect("writer open");
    populate(py);
    fs::write(marker_path(dir, "writer_done"), b"").expect("write writer_done marker");
}

fn worker_phase(py: Python<'_>, dir: &str) {
    wait_for_marker(&marker_path(dir, "writer_done"), Duration::from_secs(30));

    // Opening the read-journal overlay can transiently fail while the
    // writer's checkpoint coverage is mid-transition ("read-committed reload
    // failed or checkpoint coverage advanced; retry the read" -- the error
    // names its own remedy). Not seen in event_edges_restart.rs/
    // event_edges_cross_process_visibility.rs, likely because this test
    // writes far more rows (N_PARENTS) before the worker opens.
    let mut attempt = 0;
    loop {
        match open_client_read_only(py, dir.to_owned()) {
            Ok(()) => break,
            Err(e) if attempt < 20 => {
                attempt += 1;
                eprintln!("worker open attempt {attempt} failed, retrying: {e}");
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("worker open (read-only, shared WAL) failed: {e}"),
        }
    }

    let parents: Vec<String> = (0..N_PARENTS)
        .map(|i| format!("$cost-parent-{i}"))
        .collect();
    let leaf_children: Vec<String> = (0..N_PARENTS).map(|i| format!("$cost-child-{i}")).collect();

    // First pass: cold, this process has never read these keys before. The
    // first sample is already after `open_client_read_only` installed the
    // overlay, so it captures the refreshes that open itself performed; the
    // deltas below are the refreshes the timed reads add.
    let (refreshes_before, bytes_before) = event_dag_refresh_counters(py);
    let (hit_p50, hit_p99) = time_reads(py, &parents);
    let (miss_p50, miss_p99) = time_reads(py, &leaf_children);
    let (refreshes_mid, bytes_mid) = event_dag_refresh_counters(py);
    // Second pass over the exact same keys: isolates a one-time cold-cache/
    // index-load cost (would drop sharply here) from a genuine per-call cost
    // the overlay-refresh check pays every time (would stay flat).
    let (hit_p50_warm, hit_p99_warm) = time_reads(py, &parents);
    let (miss_p50_warm, miss_p99_warm) = time_reads(py, &leaf_children);
    let (refreshes_after, bytes_after) = event_dag_refresh_counters(py);

    fs::write(
        marker_path(dir, "worker_result"),
        format!(
            "hit_p50={hit_p50} hit_p99={hit_p99} miss_p50={miss_p50} miss_p99={miss_p99} \
             hit_p50_warm={hit_p50_warm} hit_p99_warm={hit_p99_warm} \
             miss_p50_warm={miss_p50_warm} miss_p99_warm={miss_p99_warm} \
             open_refreshes={refreshes_before} open_refresh_bytes={bytes_before} \
             cold_refreshes={} cold_refresh_bytes={} \
             warm_refreshes={} warm_refresh_bytes={}",
            refreshes_mid - refreshes_before,
            bytes_mid - bytes_before,
            refreshes_after - refreshes_mid,
            bytes_after - bytes_mid,
        ),
    )
    .expect("write worker_result marker");
}

#[test]
fn worker_reads_via_read_journal_overlay() {
    if let Ok(dir) = std::env::var(DIR_ENV) {
        Python::initialize();
        let phase = std::env::var(PHASE_ENV).expect("phase env set by the parent");
        Python::attach(|py| match phase.as_str() {
            "writer" => writer_phase(py, &dir),
            "worker" => worker_phase(py, &dir),
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

    let mut writer = spawn_phase(&dir, "writer");
    let writer_status = wait_bounded(&mut writer, "writer");
    assert!(writer_status.success(), "writer failed: {writer_status:?}");

    let mut worker = spawn_phase(&dir, "worker");
    let worker_status = wait_bounded(&mut worker, "worker");
    assert!(worker_status.success(), "worker failed: {worker_status:?}");

    let result =
        fs::read_to_string(marker_path(&dir, "worker_result")).expect("worker_result written");
    println!("read-only worker (shared WAL, read-journal overlay + refresh-on-miss): {result}");
}

fn spawn_phase(dir: &str, phase: &str) -> Child {
    Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(DIR_ENV, dir)
        .env(PHASE_ENV, phase)
        .spawn()
        .expect("spawn child process")
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
