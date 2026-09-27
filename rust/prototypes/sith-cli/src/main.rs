//! Standalone prototype CLI for room state comparison.
//!
//! This crate is intentionally outside the Synapse Rust crate and is not a
//! workspace member. It can be run directly with:
//!
//!     cargo run --manifest-path rust/prototypes/sith-cli/Cargo.toml -- \
//!       compare-state ROOM SERVER --local FILE --remote SERVER=FILE
//!
//! Live mtxdb and federation adapters are deliberately left for a later
//! integration step; this binary currently compares deterministic fixtures.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::process::ExitCode;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StateKey {
    event_type: String,
    state_key: String,
}

type State = BTreeMap<StateKey, String>;

fn usage() {
    eprintln!(
        "Usage:\n  sith compare-state ROOM SERVER... --local FILE --remote SERVER=FILE...\n\n\
Fixture format: one tab-separated `event_type<TAB>state_key<TAB>event_id` per line.\n\
Blank lines and lines beginning with # are ignored."
    );
}

fn load_snapshot(path: &str) -> Result<State, String> {
    let contents = fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
    let mut state = State::new();

    for (line_number, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('\t');
        let event_type = fields.next();
        let state_key = fields.next();
        let event_id = fields.next();
        if event_type.is_none()
            || state_key.is_none()
            || event_id.is_none()
            || fields.next().is_some()
        {
            return Err(format!(
                "{path}:{}: expected event_type<TAB>state_key<TAB>event_id",
                line_number + 1
            ));
        }

        state.insert(
            StateKey {
                event_type: event_type.unwrap().to_owned(),
                state_key: state_key.unwrap().to_owned(),
            },
            event_id.unwrap().to_owned(),
        );
    }
    Ok(state)
}

fn compare(left: &State, right: &State) -> (usize, Vec<String>) {
    let mut keys = left.keys().cloned().collect::<Vec<_>>();
    keys.extend(right.keys().filter(|key| !left.contains_key(*key)).cloned());
    keys.sort();

    let mut same = 0;
    let mut differences = Vec::new();
    for key in keys {
        match (left.get(&key), right.get(&key)) {
            (Some(left_id), Some(right_id)) if left_id == right_id => same += 1,
            (Some(left_id), Some(right_id)) => differences.push(format!(
                "changed {}\t{}: {} != {}",
                key.event_type, key.state_key, left_id, right_id
            )),
            (Some(left_id), None) => differences.push(format!(
                "missing remotely {}\t{}: {}",
                key.event_type, key.state_key, left_id
            )),
            (None, Some(right_id)) => differences.push(format!(
                "missing locally {}\t{}: {}",
                key.event_type, key.state_key, right_id
            )),
            (None, None) => unreachable!(),
        }
    }
    (same, differences)
}

fn compare_state(args: &[String]) -> Result<(), String> {
    if args.len() < 4 {
        usage();
        return Err("compare-state requires a room, server, --local, and --remote".into());
    }

    let room = &args[0];
    let mut servers = Vec::new();
    let mut local_path = None;
    let mut remotes = BTreeMap::new();
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--local" => {
                index += 1;
                local_path = args.get(index).cloned();
            }
            "--remote" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "--remote requires SERVER=FILE".to_owned())?;
                let (server, path) = value
                    .split_once('=')
                    .ok_or_else(|| "--remote requires SERVER=FILE".to_owned())?;
                remotes.insert(server.to_owned(), path.to_owned());
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown option: {value}"));
            }
            server => servers.push(server.to_owned()),
        }
        index += 1;
    }

    let local_path = local_path.ok_or_else(|| "missing --local FILE".to_owned())?;
    let local = load_snapshot(&local_path)?;
    if servers.is_empty() {
        servers.extend(remotes.keys().cloned());
    }
    if servers.is_empty() {
        return Err("at least one server is required".into());
    }

    println!("room: {room}");
    println!("local state events: {}", local.len());
    for server in servers {
        let path = remotes
            .get(&server)
            .ok_or_else(|| format!("no --remote {server}=FILE supplied"))?;
        let remote = load_snapshot(path)?;
        let (same, differences) = compare(&local, &remote);
        println!("local vs {server}");
        println!("  same: {same}");
        println!("  differences: {}", differences.len());
        for difference in differences {
            println!("  {difference}");
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(String::as_str) != Some("compare-state") {
        usage();
        return ExitCode::from(2);
    }

    match compare_state(&args[1..]) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sith: {error}");
            ExitCode::from(2)
        }
    }
}
