//! Speculative Rust-only prototype for:
//!
//!     sith compare-state '!room:example.org' server-a.example server-b.example
//!
//! This file is intentionally isolated and is not wired into the Cargo
//! package, Synapse, mtxdb startup, or federation routing yet.  It describes
//! the first read-only operator command around the existing mtxdb event-DAG,
//! edge, event-to-state-group, and HAMT data.

use std::collections::{BTreeMap, BTreeSet};

/// The state identity that is meaningful across homeservers.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StateKey {
    pub event_type: String,
    pub state_key: String,
}

/// A state snapshot at one DAG extremity.
#[derive(Clone, Debug, Default)]
pub struct RoomState {
    pub room_id: String,
    pub at_event: Option<String>,
    pub event_ids: BTreeMap<StateKey, String>,
}

/// One difference between two state snapshots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Difference {
    MissingLeft { key: StateKey, right_event_id: String },
    MissingRight { key: StateKey, left_event_id: String },
    Different {
        key: StateKey,
        left_event_id: String,
        right_event_id: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct Comparison {
    pub same: usize,
    pub differences: Vec<Difference>,
}

impl Comparison {
    pub fn between(left: &RoomState, right: &RoomState) -> Self {
        let keys: BTreeSet<_> = left
            .event_ids
            .keys()
            .chain(right.event_ids.keys())
            .cloned()
            .collect();

        let mut result = Self::default();
        for key in keys {
            match (left.event_ids.get(&key), right.event_ids.get(&key)) {
                (Some(left_id), Some(right_id)) if left_id == right_id => {
                    result.same += 1;
                }
                (Some(left_id), Some(right_id)) => {
                    result.differences.push(Difference::Different {
                        key,
                        left_event_id: left_id.clone(),
                        right_event_id: right_id.clone(),
                    });
                }
                (Some(left_id), None) => {
                    result.differences.push(Difference::MissingRight {
                        key,
                        left_event_id: left_id.clone(),
                    });
                }
                (None, Some(right_id)) => {
                    result.differences.push(Difference::MissingLeft {
                        key,
                        right_event_id: right_id.clone(),
                    });
                }
                (None, None) => unreachable!("comparison key came from one of the maps"),
            }
        }
        result
    }
}

/// The local source is intended to be backed by:
///
/// 1. mtxdb's room event-DAG and edge collections;
/// 2. forward-extremity discovery from the room DAG;
/// 3. the event -> state-group mapping; and
/// 4. state-HAMT root/node materialization.
///
/// No SQL or Python dependency is assumed by this interface.
pub trait LocalStateSource {
    type Error;

    fn current_state(&self, room_id: &str) -> Result<RoomState, Self::Error>;
}

/// The remote source is intended to use signed federation requests to obtain
/// `/state_ids` (and optionally `/state`) at the selected event.
pub trait RemoteStateSource {
    type Error;

    fn state_from_server(
        &self,
        room_id: &str,
        server_name: &str,
        at_event: Option<&str>,
    ) -> Result<RoomState, Self::Error>;
}

/// Command-level shape for the future `sith compare-state` implementation.
///
/// The eventual binary should:
///
/// ```text
/// sith compare-state ROOM [SERVER...]
///     [--at-event EVENT_ID]
///     [--summary]
///     [--json]
/// ```
///
/// Local state is always the left-hand snapshot. With multiple servers, the
/// first server is compared with local state and each later server is also
/// compared with the first server.
pub fn compare_state<L, R>(
    local: &L,
    remote: &R,
    room_id: &str,
    servers: &[String],
    at_event: Option<&str>,
) -> Result<Vec<(String, Comparison)>, CompareStateError<L::Error, R::Error>>
where
    L: LocalStateSource,
    R: RemoteStateSource,
{
    if servers.is_empty() {
        return Err(CompareStateError::NoServers);
    }

    let local_state = local
        .current_state(room_id)
        .map_err(CompareStateError::Local)?;
    let mut remote_states = Vec::with_capacity(servers.len());
    for server in servers {
        remote_states.push((
            server.clone(),
            remote
                .state_from_server(room_id, server, at_event)
                .map_err(CompareStateError::Remote)?,
        ));
    }

    let first = &remote_states[0].1;
    let mut comparisons = Vec::with_capacity(remote_states.len());
    comparisons.push((
        format!("local vs {}", remote_states[0].0),
        Comparison::between(&local_state, first),
    ));
    for pair in remote_states.iter().skip(1) {
        comparisons.push((
            format!("{} vs {}", remote_states[0].0, pair.0),
            Comparison::between(first, &pair.1),
        ));
    }
    Ok(comparisons)
}

#[derive(Debug)]
pub enum CompareStateError<LocalError, RemoteError> {
    NoServers,
    Local(LocalError),
    Remote(RemoteError),
}

/// Intended human-readable summary format:
///
/// ```text
/// room: !room:example.org
/// local extremity: $local-tip
/// local vs server-a.example
///   same: 307
///   changed: 5
///   missing locally: 2
///   missing remotely: 3
/// ```
///
/// Full differences should be emitted in stable `(event_type, state_key)` order
/// and JSON output should preserve the same ordering for scripting.
