/*
 * This file is licensed under the Affero General Public License (AGPL) version 3.
 *
 * Copyright (C) 2026 Element Creations Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * See the GNU Affero General Public License for more details:
 * <https://www.gnu.org/licenses/agpl-3.0.html>.
 */

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PySet, PyTuple};
use pythonize::depythonize;
use rezzy::{
    auth::roaring::AuthGraph, basespec::event_types::EventType, resolve_lattice_fold, LeanEvent,
    RoomId, SharedState, StateResVersion,
};
use serde_json::Value;

use crate::events::{Event, EventResolverData};

#[pyfunction]
#[pyo3(text_signature = "(state_sets, event_map, /)")]
pub fn get_auth_chain_difference_from_event_graph<'py>(
    py: Python<'py>,
    state_sets: Bound<'py, PyAny>,
    event_map: Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PySet>> {
    let mut auth_graph_events: HashMap<String, LeanEvent<String, ()>> =
        HashMap::with_capacity(event_map.len());
    for (k, v) in event_map.iter() {
        let event_id: String = k.extract()?;
        let auth_ids: Vec<String> = if let Ok(event) = v.extract::<PyRef<Event>>() {
            event.auth_event_ids()?
        } else {
            v.call_method0("auth_event_ids")?.extract()?
        };
        auth_graph_events.insert(
            event_id.clone(),
            LeanEvent {
                event_id,
                event_type: String::new(),
                state_key: None,
                power_level: 0,
                origin_server_ts: 0,
                sender: String::new(),
                content: (),
                prev_events: Vec::new(),
                auth_events: auth_ids,
                depth: 0,
                rejected: false,
                soft_fail: false,
                room_id: None,
            },
        );
    }
    let auth_graph = AuthGraph::build(&auth_graph_events);

    let mut union: Option<HashSet<String>> = None;
    let mut intersection: HashSet<String> = HashSet::new();

    for state_set in state_sets.try_iter()? {
        let state_set = state_set?;
        let values = state_set.call_method0("values")?;
        let mut state_set_ids = Vec::with_capacity(values.len()?);
        for value in values.try_iter()? {
            state_set_ids.push(value?.extract()?);
        }
        let closure: HashSet<String> = auth_graph
            .auth_difference(&[], &state_set_ids)
            .into_iter()
            .collect();

        match &mut union {
            None => {
                intersection = closure.clone();
                union = Some(closure);
            }
            Some(union) => {
                union.extend(closure.iter().cloned());
                intersection = intersection.intersection(&closure).cloned().collect();
            }
        }
    }

    let Some(union) = union else {
        return PySet::empty(py);
    };

    let result: HashSet<String> = union.difference(&intersection).cloned().collect();
    PySet::new(py, result)
}

fn resolver_data_to_lean_event(data: EventResolverData) -> LeanEvent<String, Value> {
    // For MSC4242 (room version 2.2), events carry `prev_state_events` instead
    // of `auth_events`. rezzy's LeanEvent folds both into a single `auth_events`
    // field and exposes them via `prev_state_events()` returning `&self.auth_events`.
    // Gated explicitly on the event's actual room version rather than on
    // whether `prev_state_events` happens to be non-empty, since a v2.2 event
    // can legitimately have no prior state to point to (e.g. the create
    // event) and must still be treated as MSC4242, not silently fall back to
    // `auth_events`.
    let auth_events = if data.msc4242_state_dags {
        data.prev_state_events
    } else {
        data.auth_events
    };
    LeanEvent {
        event_id: data.event_id,
        event_type: data.event_type,
        state_key: data.state_key,
        power_level: 0,
        origin_server_ts: data.origin_server_ts,
        sender: data.sender,
        content: data.content,
        prev_events: data.prev_events,
        auth_events,
        depth: data.depth,
        rejected: data.rejected,
        soft_fail: data.soft_failed,
        room_id: Some(RoomId::new(data.room_id)),
    }
}

fn py_to_lean_event(py_ev: &Bound<'_, PyAny>) -> PyResult<LeanEvent<String, Value>> {
    let event_id: String = py_ev.getattr("event_id")?.extract()?;
    let room_id: String = py_ev.getattr("room_id")?.extract()?;
    let event_type: String = py_ev.getattr("type")?.extract()?;
    let state_key: Option<String> = py_ev.call_method0("get_state_key")?.extract()?;
    let sender: String = py_ev.getattr("sender")?.extract()?;
    let origin_server_ts: u64 = py_ev.getattr("origin_server_ts")?.extract()?;
    let depth: u64 = py_ev.getattr("depth")?.extract()?;

    let prev_events: Vec<String> = py_ev.call_method0("prev_event_ids")?.extract()?;
    let auth_events: Vec<String> = py_ev.call_method0("auth_event_ids")?.extract()?;
    let prev_state_events: Vec<String> = py_ev
        .getattr("prev_state_events")
        .and_then(|value| value.extract())
        .unwrap_or_default();
    // For MSC4242 (room version 2.2), events carry `prev_state_events` instead
    // of `auth_events`. rezzy's LeanEvent folds both into a single `auth_events`
    // field and exposes them via `prev_state_events()` returning `&self.auth_events`.
    // Gated explicitly on the event's actual room version (matching Python's
    // own `supports_msc4242_state_dag`), not on whether `prev_state_events`
    // happens to be non-empty -- a v2.2 event can legitimately have no prior
    // state to point to and must still be treated as MSC4242.
    let msc4242_state_dags: bool = py_ev
        .getattr("room_version")?
        .getattr("msc4242_state_dags")?
        .extract()?;
    let auth_events = if msc4242_state_dags {
        prev_state_events
    } else {
        auth_events
    };
    let rejected_reason: Option<String> = py_ev.getattr("rejected_reason")?.extract()?;
    let soft_failed: bool = py_ev
        .getattr("internal_metadata")?
        .call_method0("is_soft_failed")?
        .extract()?;

    let py_content = py_ev.getattr("content")?;
    let content: Value = depythonize(&py_content)?;

    let power_level: i64 = 0;

    Ok(LeanEvent {
        event_id,
        event_type,
        state_key,
        power_level,
        origin_server_ts,
        sender,
        content,
        prev_events,
        auth_events,
        depth,
        rejected: rejected_reason.is_some(),
        soft_fail: soft_failed,
        room_id: Some(RoomId::new(room_id)),
    })
}

#[pyfunction]
#[pyo3(text_signature = "(unconflicted_state, conflicted_event_ids, event_map, /)")]
pub fn resolve_v2_via_lattice_fold<'py>(
    py: Python<'py>,
    unconflicted_state: Bound<'py, PyDict>,
    conflicted_event_ids: Bound<'py, PyAny>,
    event_map: Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyDict>> {
    let parsed_events = parse_event_map(event_map)?;
    resolve_v2_from_parsed_events(py, unconflicted_state, conflicted_event_ids, &parsed_events)
}

fn parse_event_map(
    event_map: Bound<'_, PyDict>,
) -> PyResult<HashMap<String, LeanEvent<String, Value>>> {
    let mut parsed_events = HashMap::with_capacity(event_map.len());
    for (k, v) in event_map.iter() {
        let event_id: String = k.extract()?;
        let lean_ev = if let Ok(event) = v.extract::<PyRef<Event>>() {
            resolver_data_to_lean_event(event.resolver_data()?)
        } else {
            py_to_lean_event(&v)?
        };
        parsed_events.insert(event_id, lean_ev);
    }
    Ok(parsed_events)
}

fn resolve_v2_from_parsed_events<'py>(
    py: Python<'py>,
    unconflicted_state: Bound<'py, PyDict>,
    conflicted_event_ids: Bound<'py, PyAny>,
    parsed_events: &HashMap<String, LeanEvent<String, Value>>,
) -> PyResult<Bound<'py, PyDict>> {
    let mut unconf_state = SharedState::new();
    for (k, v) in unconflicted_state.iter() {
        let (type_str, state_key): (String, String) = k.extract()?;
        let val: String = v.extract()?;
        unconf_state.insert((EventType::from(type_str), state_key), val);
    }

    let conflicted_ids: Vec<String> = conflicted_event_ids.extract()?;
    let mut conflicted_events = HashMap::with_capacity(conflicted_ids.len());
    for id in conflicted_ids {
        if let Some(ev) = parsed_events.get(&id) {
            conflicted_events.insert(id.clone(), ev.clone());
        }
    }

    let resolved = resolve_lattice_fold(
        unconf_state,
        conflicted_events,
        parsed_events,
        StateResVersion::V2,
    );

    let py_resolved = PyDict::new(py);
    for ((type_, state_key), event_id) in resolved {
        let py_key = PyTuple::new(py, [type_.as_str(), &state_key])?;
        py_resolved.set_item(py_key, event_id)?;
    }

    Ok(py_resolved)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "state_res")?;
    child_module.add_function(wrap_pyfunction!(
        get_auth_chain_difference_from_event_graph,
        &child_module
    )?)?;
    child_module.add_function(wrap_pyfunction!(
        resolve_v2_via_lattice_fold,
        &child_module
    )?)?;
    m.add_submodule(&child_module)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.state_res", child_module)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver_data(
        msc4242_state_dags: bool,
        auth_events: Vec<String>,
        prev_state_events: Vec<String>,
    ) -> EventResolverData {
        EventResolverData {
            event_id: "$event".to_owned(),
            room_id: "!room:test".to_owned(),
            event_type: "m.room.message".to_owned(),
            state_key: None,
            sender: "@user:test".to_owned(),
            origin_server_ts: 0,
            depth: 0,
            prev_events: Vec::new(),
            auth_events,
            prev_state_events,
            msc4242_state_dags,
            content: Value::Null,
            rejected: false,
            soft_failed: false,
        }
    }

    #[test]
    fn non_msc4242_room_always_uses_auth_events() {
        let data = resolver_data(false, vec!["$auth1".to_owned()], Vec::new());
        let lean = resolver_data_to_lean_event(data);
        assert_eq!(lean.auth_events, vec!["$auth1".to_owned()]);
    }

    #[test]
    fn msc4242_room_uses_prev_state_events_even_when_empty() {
        // The regression this guards against: a v2.2 event that legitimately
        // has no prior state to point to (e.g. the create event) must NOT
        // silently fall back to `auth_events` just because
        // `prev_state_events` happens to be empty.
        let data = resolver_data(true, vec!["$auth1".to_owned()], Vec::new());
        let lean = resolver_data_to_lean_event(data);
        assert_eq!(lean.auth_events, Vec::<String>::new());
    }

    #[test]
    fn msc4242_room_uses_prev_state_events_when_populated() {
        let data = resolver_data(true, vec!["$auth1".to_owned()], vec!["$pstate1".to_owned()]);
        let lean = resolver_data_to_lean_event(data);
        assert_eq!(lean.auth_events, vec!["$pstate1".to_owned()]);
    }
}
