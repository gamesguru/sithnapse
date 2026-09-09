//! Embedded single-process HAMT node/root storage. `core` holds the
//! generic BFS materialize/selective-lookup walk and key encoding; `mtxdb`
//! is the (sole) driver implementing `core::NodeStore` over mtxdb's
//! append-only content-addressed packfiles.

pub mod core;
pub mod mtxdb;

use pyo3::prelude::*;

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    mtxdb::register_module(py, m)?;
    Ok(())
}
