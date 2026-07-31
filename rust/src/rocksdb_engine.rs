use std::sync::Mutex;

use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use rocksdb::{Options, DB};

static DB_INSTANCE: OnceCell<Mutex<DB>> = OnceCell::new();

#[pyfunction]
pub fn open_db(path: &str) -> PyResult<()> {
    if DB_INSTANCE.get().is_some() {
        return Ok(());
    }
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = DB::open(&opts, path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    DB_INSTANCE
        .set(Mutex::new(db))
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to set DB instance"))?;
    Ok(())
}

#[pyfunction]
pub fn put(key: &str, value: &str) -> PyResult<()> {
    let db_mutex = DB_INSTANCE.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("Database is not open. Call open_db first.")
    })?;
    let db = db_mutex.lock().unwrap();
    db.put(key.as_bytes(), value.as_bytes())
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}

#[pyfunction]
pub fn get(key: &str) -> PyResult<Option<String>> {
    let db_mutex = DB_INSTANCE.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("Database is not open. Call open_db first.")
    })?;
    let db = db_mutex.lock().unwrap();
    let val_bytes = db
        .get(key.as_bytes())
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    match val_bytes {
        Some(bytes) => {
            let s = String::from_utf8(bytes)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(Some(s))
        }
        None => Ok(None),
    }
}

#[pyfunction]
pub fn delete(key: &str) -> PyResult<()> {
    let db_mutex = DB_INSTANCE.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("Database is not open. Call open_db first.")
    })?;
    let db = db_mutex.lock().unwrap();
    db.delete(key.as_bytes())
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}

#[pyfunction]
pub fn scan_prefix(prefix: &str) -> PyResult<Vec<(String, String)>> {
    let db_mutex = DB_INSTANCE.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("Database is not open. Call open_db first.")
    })?;
    let db = db_mutex.lock().unwrap();
    let mut results = Vec::new();
    let prefix_bytes = prefix.as_bytes();

    // Create an iterator at the prefix
    let mut iter = db.raw_iterator();
    iter.seek(prefix_bytes);

    while iter.valid() {
        let key = iter.key().unwrap();
        if !key.starts_with(prefix_bytes) {
            break;
        }
        let val = iter.value().unwrap();
        let key_str = String::from_utf8(key.to_vec())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let val_str = String::from_utf8(val.to_vec())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        results.push((key_str, val_str));
        iter.next();
    }

    Ok(results)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "rocksdb_engine")?;
    child_module.add_function(wrap_pyfunction!(open_db, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(delete, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(scan_prefix, &child_module)?)?;

    m.add_submodule(&child_module)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.rocksdb_engine", &child_module)?;

    Ok(())
}
