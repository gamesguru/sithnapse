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

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PySet, PyTuple};
use pythonize::depythonize;
use rezzy::basespec::event_types::MAX_POWER_LEVEL_JSON;
use rezzy::{
    auth::roaring::AuthGraph, basespec::event_types::EventType, is_valid_mxid,
    resolve_semilattice_fold, EventContent, LeanEvent, RoomId, SharedState, StateResVersion,
};
use serde_json::Value;

use crate::events::{json_object::JsonObject, Event, EventResolverData};

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

/// The resolver keeps the source JSON tree and reads the fields it needs in place.
/// The normal `state/v2.py` event map contains Rust `Event` objects, which flow
/// through `resolver_data()` as a shared `JsonObject`; cloning it only clones
/// its `Arc`, with no JSON serialization or second content tree. The Python
/// fallback shares an existing `JsonObject` too; other JSON-compatible values
/// are depythonized once and shared behind an `Arc`.
#[derive(Clone)]
enum ResolverContent {
    SharedObject(JsonObject),
    SharedValue(Arc<Value>),
}

impl Default for ResolverContent {
    fn default() -> Self {
        Self::SharedValue(Arc::new(Value::Null))
    }
}

impl fmt::Debug for ResolverContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SharedObject(value) => {
                f.debug_tuple("SharedObject").field(value.as_map()).finish()
            }
            Self::SharedValue(value) => f.debug_tuple("SharedValue").field(value).finish(),
        }
    }
}

impl ResolverContent {
    fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Self::SharedObject(object) => object.get_field(key),
            Self::SharedValue(value) => value.get(key),
        }
    }
}

// Delegate to rezzy's coercion so the adapter can never drift from the
// resolver's own numeric rules.
fn coerce_serde_json_to_i64(value: &Value) -> Option<i64> {
    rezzy::coerce_json_integer_parts(
        value.as_i64(),
        value.as_u64(),
        value.as_f64(),
        value.as_str(),
    )
}

impl EventContent for ResolverContent {
    fn get_membership(&self) -> Option<&str> {
        self.get(rezzy::basespec::event_types::FIELD_MEMBERSHIP)?
            .as_str()
    }

    fn get_cdo_active_member(&self) -> Option<&str> {
        self.get("tk.nutra.cdo")?.get("active_member")?.as_str()
    }

    fn get_join_rule(&self) -> Option<&str> {
        self.get(rezzy::basespec::event_types::FIELD_JOIN_RULE)?
            .as_str()
    }

    fn get_user_power_level(&self, user: &str) -> Option<i64> {
        let users = self
            .get(rezzy::basespec::event_types::FIELD_USERS)?
            .as_object()?;
        coerce_serde_json_to_i64(users.get(user)?).map(|i| i.min(MAX_POWER_LEVEL_JSON))
    }

    fn get_event_power_level(&self, event_type: &str) -> Option<i64> {
        let events = self
            .get(rezzy::basespec::event_types::FIELD_EVENTS)?
            .as_object()?;
        coerce_serde_json_to_i64(events.get(event_type)?).map(|i| i.min(MAX_POWER_LEVEL_JSON))
    }

    fn get_users_default(&self) -> Option<i64> {
        coerce_serde_json_to_i64(self.get(rezzy::basespec::event_types::FIELD_USERS_DEFAULT)?)
            .map(|i| i.min(MAX_POWER_LEVEL_JSON))
    }

    fn get_events_default(&self) -> Option<i64> {
        coerce_serde_json_to_i64(self.get(rezzy::basespec::event_types::FIELD_EVENTS_DEFAULT)?)
            .map(|i| i.min(MAX_POWER_LEVEL_JSON))
    }

    fn get_state_default(&self) -> Option<i64> {
        coerce_serde_json_to_i64(self.get(rezzy::basespec::event_types::FIELD_STATE_DEFAULT)?)
            .map(|i| i.min(MAX_POWER_LEVEL_JSON))
    }

    fn get_ban(&self) -> Option<i64> {
        coerce_serde_json_to_i64(self.get(rezzy::basespec::event_types::FIELD_BAN)?)
            .map(|i| i.min(MAX_POWER_LEVEL_JSON))
    }

    fn get_kick(&self) -> Option<i64> {
        coerce_serde_json_to_i64(self.get(rezzy::basespec::event_types::FIELD_KICK)?)
            .map(|i| i.min(MAX_POWER_LEVEL_JSON))
    }

    fn get_invite(&self) -> Option<i64> {
        coerce_serde_json_to_i64(self.get(rezzy::basespec::event_types::FIELD_INVITE)?)
            .map(|i| i.min(MAX_POWER_LEVEL_JSON))
    }

    fn get_redact(&self) -> Option<i64> {
        coerce_serde_json_to_i64(self.get(rezzy::basespec::event_types::FIELD_REDACT)?)
    }

    fn get_creator(&self) -> Option<&str> {
        self.get(rezzy::basespec::event_types::FIELD_CREATOR)?
            .as_str()
    }

    fn get_room_version(&self) -> Option<&str> {
        self.get(rezzy::basespec::event_types::FIELD_ROOM_VERSION)?
            .as_str()
    }

    fn has_malformed_room_version(&self) -> bool {
        self.get(rezzy::basespec::event_types::FIELD_ROOM_VERSION)
            .is_some_and(|v| v.as_str().is_none())
    }

    fn get_m_federate(&self) -> Option<bool> {
        self.get("m.federate")?.as_bool()
    }

    fn get_redacts(&self) -> Option<&str> {
        self.get(rezzy::basespec::event_types::FIELD_REDACTS)?
            .as_str()
    }

    fn has_additional_creator(&self, sender: &str) -> bool {
        self.get(rezzy::basespec::event_types::FIELD_ADDITIONAL_CREATORS)
            .and_then(|v| v.as_array())
            .is_some_and(|arr| arr.iter().any(|v| v.as_str() == Some(sender)))
    }

    fn additional_creators_are_valid(&self) -> bool {
        match self.get(rezzy::basespec::event_types::FIELD_ADDITIONAL_CREATORS) {
            None => true,
            Some(v) => v.as_array().is_some_and(|arr| {
                arr.iter()
                    .all(|entry| entry.as_str().is_some_and(is_valid_mxid))
            }),
        }
    }

    fn get_join_authorised_via_users_server(&self) -> Option<&str> {
        self.get(rezzy::basespec::event_types::FIELD_JOIN_AUTHORISED_VIA_USERS_SERVER)?
            .as_str()
    }

    fn has_third_party_invite(&self) -> bool {
        self.get(rezzy::basespec::event_types::FIELD_THIRD_PARTY_INVITE)
            .is_some()
    }

    fn get_third_party_invite_token(&self) -> Option<&str> {
        self.get(rezzy::basespec::event_types::FIELD_THIRD_PARTY_INVITE)?
            .get(rezzy::basespec::event_types::FIELD_SIGNED)?
            .get(rezzy::basespec::event_types::FIELD_TOKEN)?
            .as_str()
    }

    fn get_third_party_invite_mxid(&self) -> Option<&str> {
        self.get(rezzy::basespec::event_types::FIELD_THIRD_PARTY_INVITE)?
            .get(rezzy::basespec::event_types::FIELD_SIGNED)?
            .get(rezzy::basespec::event_types::FIELD_MXID)?
            .as_str()
    }

    fn has_third_party_invite_signatures(&self) -> bool {
        self.get(rezzy::basespec::event_types::FIELD_THIRD_PARTY_INVITE)
            .and_then(|tpi| tpi.get(rezzy::basespec::event_types::FIELD_SIGNED))
            .and_then(|signed| signed.get(rezzy::basespec::event_types::FIELD_SIGNATURES))
            .and_then(|s| s.as_object())
            .is_some_and(|m| !m.is_empty())
    }

    fn visit_event_power_levels<'a>(&'a self, visitor: &mut dyn FnMut(&'a str, i64)) {
        if let Some(obj) = self
            .get(rezzy::basespec::event_types::FIELD_EVENTS)
            .and_then(|v| v.as_object())
        {
            for (k, v) in obj {
                if let Some(pl) = coerce_serde_json_to_i64(v) {
                    visitor(k.as_str(), pl.min(MAX_POWER_LEVEL_JSON));
                }
            }
        }
    }

    fn visit_user_power_levels<'a>(&'a self, visitor: &mut dyn FnMut(&'a str, i64)) {
        if let Some(obj) = self
            .get(rezzy::basespec::event_types::FIELD_USERS)
            .and_then(|v| v.as_object())
        {
            for (k, v) in obj {
                if let Some(pl) = coerce_serde_json_to_i64(v) {
                    visitor(k.as_str(), pl.min(MAX_POWER_LEVEL_JSON));
                }
            }
        }
    }

    fn visit_notification_power_levels<'a>(&'a self, visitor: &mut dyn FnMut(&'a str, i64)) {
        if let Some(obj) = self
            .get(rezzy::basespec::event_types::FIELD_NOTIFICATIONS)
            .and_then(|v| v.as_object())
        {
            for (k, v) in obj {
                if let Some(pl) = coerce_serde_json_to_i64(v) {
                    visitor(k.as_str(), pl.min(MAX_POWER_LEVEL_JSON));
                }
            }
        }
    }

    fn find_non_integer_scalar_pl(&self) -> Option<&'static str> {
        use rezzy::basespec::event_types::{
            FIELD_BAN, FIELD_EVENTS_DEFAULT, FIELD_INVITE, FIELD_KICK, FIELD_REDACT,
            FIELD_STATE_DEFAULT, FIELD_USERS_DEFAULT,
        };
        let scalars: &[(&str, &'static str)] = &[
            (FIELD_USERS_DEFAULT, "users_default"),
            (FIELD_EVENTS_DEFAULT, "events_default"),
            (FIELD_STATE_DEFAULT, "state_default"),
            (FIELD_BAN, "ban"),
            (FIELD_REDACT, "redact"),
            (FIELD_KICK, "kick"),
            (FIELD_INVITE, "invite"),
        ];
        for &(field, label) in scalars {
            if let Some(val) = self.get(field) {
                // V10+ strict integer checking (forbids strings/floats)
                if !val.is_i64() && !val.is_u64() {
                    return Some(label);
                }
            }
        }
        None
    }

    fn find_non_integer_map_pl(&self) -> Option<&'static str> {
        use rezzy::basespec::event_types::{FIELD_EVENTS, FIELD_NOTIFICATIONS};
        let maps: &[(&str, &'static str)] = &[
            (FIELD_EVENTS, "events"),
            (FIELD_NOTIFICATIONS, "notifications"),
        ];
        for &(field, label) in maps {
            if let Some(val) = self.get(field) {
                let Some(obj) = val.as_object() else {
                    return Some(label);
                };

                for v in obj.values() {
                    // V10+ strict integer checking
                    if !v.is_i64() && !v.is_u64() {
                        return Some(label);
                    }
                }
            }
        }
        None
    }

    fn has_non_integer_users_pl(&self, strict: bool) -> bool {
        use rezzy::basespec::event_types::FIELD_USERS;
        if let Some(val) = self.get(FIELD_USERS) {
            if let Some(obj) = val.as_object() {
                for v in obj.values() {
                    if strict {
                        // V10+ strict integer checking
                        if !v.is_i64() && !v.is_u64() {
                            return true;
                        }
                    } else if coerce_serde_json_to_i64(v).is_none() {
                        // V1-V9 allows coercible strings
                        return true;
                    }
                }
            } else {
                // `users` present but not an object
                return true;
            }
        }
        false
    }

    fn visit_user_keys<'a>(&'a self, visitor: &mut dyn FnMut(&'a str)) {
        if let Some(obj) = self
            .get(rezzy::basespec::event_types::FIELD_USERS)
            .and_then(|v| v.as_object())
        {
            for k in obj.keys() {
                visitor(k.as_str());
            }
        }
    }

    fn has_user_in_users(&self, user_id: &str) -> bool {
        self.get(rezzy::basespec::event_types::FIELD_USERS)
            .and_then(|v| v.as_object())
            .is_some_and(|obj| obj.contains_key(user_id))
    }
}

fn resolver_data_to_lean_event(
    data: EventResolverData,
) -> PyResult<LeanEvent<String, ResolverContent>> {
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
    Ok(LeanEvent {
        event_id: data.event_id,
        event_type: data.event_type,
        state_key: data.state_key,
        power_level: 0,
        origin_server_ts: data.origin_server_ts,
        sender: data.sender,
        content: ResolverContent::SharedObject(data.content),
        prev_events: data.prev_events,
        auth_events,
        depth: data.depth,
        rejected: data.rejected,
        soft_fail: data.soft_failed,
        room_id: Some(RoomId::new(data.room_id)),
    })
}

fn py_to_lean_event(py_ev: &Bound<'_, PyAny>) -> PyResult<LeanEvent<String, ResolverContent>> {
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
    // `EventBase` is the Rust `Event` pyclass, so the primary call site takes the
    // shared-`JsonObject` branch above. When it does not, share the content
    // object if it already is a `JsonObject` and only depythonize a real mapping.
    let content = match py_content.extract::<JsonObject>() {
        Ok(object) => ResolverContent::SharedObject(object),
        Err(_) => ResolverContent::SharedValue(Arc::new(depythonize(&py_content)?)),
    };

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
) -> PyResult<HashMap<String, LeanEvent<String, ResolverContent>>> {
    let mut parsed_events = HashMap::with_capacity(event_map.len());
    for (k, v) in event_map.iter() {
        let event_id: String = k.extract()?;
        let lean_ev = if let Ok(event) = v.extract::<PyRef<Event>>() {
            resolver_data_to_lean_event(event.resolver_data()?)?
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
    parsed_events: &HashMap<String, LeanEvent<String, ResolverContent>>,
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

    let resolved = resolve_semilattice_fold(
        &unconf_state,
        &conflicted_events,
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
            content: JsonObject::default(),
            rejected: false,
            soft_failed: false,
        }
    }

    #[test]
    fn non_msc4242_room_always_uses_auth_events() {
        let data = resolver_data(false, vec!["$auth1".to_owned()], Vec::new());
        let lean = resolver_data_to_lean_event(data).expect("lean event conversion");
        assert_eq!(lean.auth_events, vec!["$auth1".to_owned()]);
    }

    #[test]
    fn msc4242_room_uses_prev_state_events_even_when_empty() {
        // The regression this guards against: a v2.2 event that legitimately
        // has no prior state to point to (e.g. the create event) must NOT
        // silently fall back to `auth_events` just because
        // `prev_state_events` happens to be empty.
        let data = resolver_data(true, vec!["$auth1".to_owned()], Vec::new());
        let lean = resolver_data_to_lean_event(data).expect("lean event conversion");
        assert_eq!(lean.auth_events, Vec::<String>::new());
    }

    #[test]
    fn msc4242_room_uses_prev_state_events_when_populated() {
        let data = resolver_data(true, vec!["$auth1".to_owned()], vec!["$pstate1".to_owned()]);
        let lean = resolver_data_to_lean_event(data).expect("lean event conversion");
        assert_eq!(lean.auth_events, vec!["$pstate1".to_owned()]);
    }

    #[test]
    fn resolver_content_reads_both_retained_json_sources() {
        let source = r#"{"membership":"join","users":{"@a:test":100},"events":{"m.room.name":50},"creator":"@a:test","additional_creators":["@b:test"]}"#;
        let shared: JsonObject = serde_json::from_str(source).expect("valid event content");
        let parsed: Value = serde_json::from_str(source).expect("valid event content");
        let contents = [
            ResolverContent::SharedObject(shared),
            ResolverContent::SharedValue(Arc::new(parsed)),
        ];
        for content in contents {
            assert_eq!(content.get_membership(), Some("join"));
            assert_eq!(content.get_user_power_level("@a:test"), Some(100));
            assert_eq!(content.get_event_power_level("m.room.name"), Some(50));
            assert_eq!(content.get_creator(), Some("@a:test"));
            assert!(content.has_additional_creator("@b:test"));
            assert!(content.additional_creators_are_valid());
        }
    }

    #[test]
    fn resolver_number_coercion_matches_rezzy_across_json_spellings() {
        // serde_json is built with `arbitrary_precision`, so pin the adapter's
        // coercion against rezzy's own across the spellings that actually occur
        // in the wild (legacy float/string power levels, 2^53 boundaries, u64
        // overflow).
        for source in [
            "0",
            "-0",
            "50",
            "50.0",
            "-50.5",
            "\"50\"",
            "\"not-a-number\"",
            // Both sides of the 2^53 +- 1 boundary, plus u64 overflow.
            "9007199254740991",
            "9007199254740992",
            "9007199254740993",
            "-9007199254740991",
            "-9007199254740992",
            "-9007199254740993",
            "18446744073709551616",
            "1.25",
        ] {
            let serde_value: Value = serde_json::from_str(source).expect("valid serde number");
            let rezzy_value = rezzy::JsonValue::parse(source).expect("valid rezzy number");
            assert_eq!(
                coerce_serde_json_to_i64(&serde_value),
                rezzy::coerce_json_to_i64(&rezzy_value),
                "coercion parity for {source}"
            );
        }
    }

    #[test]
    fn resolver_content_enforces_v12_mxid_grammar() {
        let valid: Value = serde_json::from_str(r#"{"additional_creators":["@a:test","@b:test"]}"#)
            .expect("valid event content");
        let invalid: Value = serde_json::from_str(r#"{"additional_creators":["@A:test"]}"#)
            .expect("valid event content");
        let valid = ResolverContent::SharedValue(Arc::new(valid));
        let invalid = ResolverContent::SharedValue(Arc::new(invalid));
        assert!(valid.additional_creators_are_valid());
        assert!(!invalid.additional_creators_are_valid());
    }
}
