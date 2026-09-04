//! Embedded single-process HAMT node/root storage. `core` holds the
//! generic BFS materialize/selective-lookup walk and key encoding; `mdbx`
//! is the (sole) driver implementing `core::NodeStore` over libmdbx.
//!
//! fjall was benchmarked alongside mdbx (see scripts-dev/benchmark_hamt_*
//! and benchmark_event_json_storage.py) and dropped: mdbx won every real
//! measurement (point reads, batch reads, and it needs no worker-process
//! bridge at all, since mdbx supports native multi-process mmap access
//! unlike fjall's single-writer-process LSM). `core`'s `NodeStore` trait
//! is kept generic rather than folded into `mdbx.rs` directly in case a
//! second backend is ever worth adding again, but there is currently only
//! one driver.

pub mod core;
pub mod mdbx;

use pyo3::prelude::*;

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    mdbx::register_module(py, m)?;
    Ok(())
}
