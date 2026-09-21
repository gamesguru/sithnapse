//! Embedded single-process HAMT node/root storage. `core` holds the
//! generic BFS materialize/selective-lookup walk and key encoding; `mtxdb`
//! is the (sole) driver implementing `core::NodeStore` over mtxdb's
//! append-only content-addressed packfiles.

pub mod core;
pub mod embedded_edges;
pub mod mtxdb;

use pyo3::prelude::*;

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "mtxdb_engine")?;
    mtxdb::register_module(&child_module)?;
    embedded_edges::register_module(&child_module)?;

    m.add_submodule(&child_module)?;

    // We need to manually add the module to sys.modules to make `from
    // synapse.synapse_rust import mtxdb_engine` work.
    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.mtxdb_engine", child_module)?;

    Ok(())
}
