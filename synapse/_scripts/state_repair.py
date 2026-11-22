#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

import argparse
import json
import logging
from collections.abc import Sequence
from typing import Any, Literal, cast

import yaml

from twisted.internet import defer, reactor as reactor_

from synapse.config.homeserver import HomeServerConfig
from synapse.events import EventBase
from synapse.server import HomeServer
from synapse.storage import DataStore
from synapse.storage.database import LoggingTransaction, make_in_list_sql_clause
from synapse.types import ISynapseReactor
from synapse.types.state import StateFilter

reactor = cast(ISynapseReactor, reactor_)
logger = logging.getLogger("synapse_state_repair")


class RepairHomeserver(HomeServer):
    DATASTORE_CLASS = DataStore
    # Never migrate the database from a repair tool: refuse on a pending delta.
    DATABASE_SCHEMA_READ_ONLY = True

    def __init__(self, config: HomeServerConfig):
        super().__init__(
            hostname=config.server.server_name,
            config=config,
            reactor=reactor,
        )


def _load_room_ids(room: list[str], room_file: str | None) -> list[str]:
    room_ids = list(room)
    if room_file:
        with open(room_file) as f:
            room_ids.extend(
                line.strip()
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            )

    return sorted(set(room_ids))


def _count_from_row(row: tuple[Any, ...] | None) -> int:
    if row is None:
        return 0

    return int(row[0])


Command = Literal[
    "check-room",
    "list-rejected",
    "list-outliers",
    "repair-member",
    "repair-state-import",
]


def _event_summary(event: EventBase | None) -> dict[str, Any] | None:
    if event is None:
        return None

    return {
        "event_id": event.event_id,
        "type": event.type,
        "state_key": event.state_key,
        "sender": event.sender,
        "origin_server_ts": event.origin_server_ts,
        "membership": (
            event.content.get("membership") if event.type == "m.room.member" else None
        ),
        "rejection_reason": event.rejected_reason,
    }


def _event_rows(rows: Sequence[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [
        {
            "event_id": row[0],
            "type": row[1],
            "state_key": row[2],
            "sender": row[3],
            "depth": row[4],
            "topological_ordering": row[5],
            "stream_ordering": row[6],
            "origin_server_ts": row[7],
            "received_ts": row[8],
            "rejection_reason": row[9],
        }
        for row in rows
    ]


def _discover_room_txn(
    txn: LoggingTransaction, room_id: str, store: Any
) -> dict[str, Any]:
    txn.execute("SELECT room_version FROM rooms WHERE room_id = ?", (room_id,))
    room_row = txn.fetchone()

    txn.execute("SELECT 1 FROM partial_state_rooms WHERE room_id = ?", (room_id,))
    partial_state = txn.fetchone() is not None

    txn.execute(
        "SELECT event_id FROM event_forward_extremities WHERE room_id = ?",
        (room_id,),
    )
    forward_extremities = sorted(row[0] for row in txn.fetchall())

    txn.execute(
        """
        SELECT COUNT(*) FROM msc4242_state_dag_edges
        WHERE room_id = ? AND prev_state_event_id IS NOT NULL
        """,
        (room_id,),
    )
    state_edge_count = _count_from_row(txn.fetchone())

    txn.execute(
        "SELECT COUNT(*) FROM state_events WHERE room_id = ?",
        (room_id,),
    )
    state_event_count = _count_from_row(txn.fetchone())

    txn.execute(
        "SELECT COUNT(*) FROM events WHERE room_id = ? AND outlier",
        (room_id,),
    )
    outlier_count = _count_from_row(txn.fetchone())

    txn.execute(
        """
        SELECT COUNT(*) FROM events AS e
        JOIN rejections AS r USING (event_id)
        WHERE e.room_id = ?
        """,
        (room_id,),
    )
    rejected_count = _count_from_row(txn.fetchone())

    if getattr(store, "_embedded_event_edges_enabled", False):
        from synapse.storage.databases.main.embedded_event_edges import (
            get_event_edges_backward_batch,
        )

        txn.execute("SELECT event_id FROM events WHERE room_id = ?", (room_id,))
        room_event_ids = [row[0] for row in txn]
        backward = get_event_edges_backward_batch(
            store._embedded_db_namespace, room_event_ids
        )
        prev_event_ids = [
            prev_event_id
            for edges in backward.values()
            if edges
            for prev_event_id, _is_state in edges
        ]
        missing_prev_event_edges = 0
        if prev_event_ids:
            distinct_prev_event_ids = list(set(prev_event_ids))
            existing: set[str] = set()
            # Chunk the lookup: SQLite binds one placeholder per ID and a large
            # room can exceed its bind-variable limit.
            for start in range(0, len(distinct_prev_event_ids), 500):
                clause, args = make_in_list_sql_clause(
                    store.database_engine,
                    "event_id",
                    distinct_prev_event_ids[start : start + 500],
                )
                txn.execute(f"SELECT event_id FROM events WHERE {clause}", args)
                existing.update(row[0] for row in txn)
            missing_prev_event_edges = sum(
                1 for prev_event_id in prev_event_ids if prev_event_id not in existing
            )
    else:
        txn.execute(
            """
            SELECT COUNT(*) FROM event_edges AS edge
            LEFT JOIN events AS prev ON prev.event_id = edge.prev_event_id
            WHERE edge.room_id = ? AND prev.event_id IS NULL
            """,
            (room_id,),
        )
        missing_prev_event_edges = _count_from_row(txn.fetchone())

    return {
        "room_id": room_id,
        "exists": room_row is not None,
        "room_version": room_row[0] if room_row else None,
        "partial_state": partial_state,
        "eligible_for_dag_replay": room_row is not None and not partial_state,
        "forward_extremities": forward_extremities,
        "forward_extremity_count": len(forward_extremities),
        "missing_prev_event_edges": missing_prev_event_edges,
        "outlier_count": outlier_count,
        "rejected_count": rejected_count,
        "state_edge_count": state_edge_count,
        "state_event_count": state_event_count,
    }


def _list_rejected_txn(
    txn: LoggingTransaction, room_id: str, limit: int, reverse: bool
) -> list[dict[str, Any]]:
    direction = "DESC" if reverse else "ASC"
    txn.execute(
        """
        SELECT
            e.event_id,
            e.type,
            e.state_key,
            e.sender,
            e.depth,
            e.topological_ordering,
            e.stream_ordering,
            e.origin_server_ts,
            e.received_ts,
            r.reason
        FROM rejections AS r
        JOIN events AS e USING (event_id)
        WHERE e.room_id = ?
        ORDER BY e.topological_ordering %s, e.stream_ordering %s
        LIMIT ?
        """
        % (direction, direction),
        (room_id, limit),
    )
    return _event_rows(txn.fetchall())


def _list_outliers_txn(
    txn: LoggingTransaction, room_id: str, limit: int, reverse: bool
) -> list[dict[str, Any]]:
    direction = "DESC" if reverse else "ASC"
    txn.execute(
        """
        SELECT
            e.event_id,
            e.type,
            e.state_key,
            e.sender,
            e.depth,
            e.topological_ordering,
            e.stream_ordering,
            e.origin_server_ts,
            e.received_ts,
            r.reason
        FROM events AS e
        LEFT JOIN rejections AS r USING (event_id)
        WHERE e.room_id = ? AND e.outlier
        ORDER BY e.topological_ordering %s, e.stream_ordering %s
        LIMIT ?
        """
        % (direction, direction),
        (room_id, limit),
    )
    return _event_rows(txn.fetchall())


async def _repair_member(
    hs: HomeServer,
    room_id: str,
    event_id: str,
    via: str,
    publish: bool,
) -> dict[str, Any]:
    """Repoint one `m.room.member` state key at a verified event.

    The event is fetched from `via` through the normal federation path, so its
    signatures and hashes are checked against the keyring, and (when publishing)
    it is persisted as an outlier through the normal auth/rejection path.

    A repair that only patched `current_state_events` would be undone by the next
    event in the room, because new events inherit the state group of their prev
    events. So, per distinct state group of the forward extremities, this stores
    a new state group (`prev_group` + a one-key delta), repoints the extremities
    at it, and then recalculates current state with the normal persistence code.
    Old state groups are left in place.

    Dry-run by default: the same checks run, but nothing is persisted.
    """
    store = hs.get_datastores().main
    controllers = hs.get_storage_controllers()
    report: dict[str, Any] = {
        "room_id": room_id,
        "event_id": event_id,
        "via": via,
        "published": False,
        "refused": None,
    }

    def refuse(reason: str) -> dict[str, Any]:
        report["refused"] = reason
        return report

    if await store.is_partial_state_room(room_id):
        return refuse("room is partial-state")
    room_version = await store.get_room_version(room_id)
    if room_version.msc4242_state_dags:
        return refuse("MSC4242 state-DAG rooms are not supported by repair-member")

    event = await store.get_event(event_id, allow_none=True, allow_rejected=True)
    fetched = event is None
    if event is None:
        pulled = await hs.get_federation_client().get_pdu([via], event_id, room_version)
        if pulled is None:
            return refuse(f"{via} did not return {event_id}")
        event = pulled.pdu
        report["signatures_verified_against_keyring"] = True
    report["already_persisted"] = not fetched

    if event.room_id != room_id:
        return refuse(f"event belongs to {event.room_id}")
    if event.type != "m.room.member" or not event.is_state():
        return refuse("event is not an m.room.member state event")
    if event.rejected_reason is not None:
        return refuse(f"event is rejected locally: {event.rejected_reason}")
    key = (event.type, event.state_key)
    report["state_key"] = list(key)
    report["membership"] = event.content.get("membership")

    latest_event_ids = await store.get_latest_event_ids_in_room(room_id)
    group_by_extremity = await store._get_state_group_for_events(latest_event_ids)
    report["forward_extremities"] = sorted(latest_event_ids)

    auth_event_ids = set(event.auth_event_ids())
    plan: list[dict[str, Any]] = []
    for group in sorted(set(group_by_extremity.values())):
        state = await controllers.state.get_state_ids_for_group(
            group, StateFilter.from_types([key])
        )
        current = state.get(key)
        if current == event_id:
            continue
        if current is not None and current not in auth_event_ids:
            return refuse(
                f"state group {group} has {current} for {key}, which is not in the "
                "auth_events of the replacement event, so it does not supersede it"
            )
        plan.append(
            {
                "old_state_group": group,
                "from": current,
                "to": event_id,
                "extremities": sorted(
                    e for e, g in group_by_extremity.items() if g == group
                ),
            }
        )
    report["plan"] = plan
    if not plan:
        report["note"] = "nothing to do: every extremity already has this event"
        return report

    if not publish:
        report["note"] = "dry run: pass --publish to apply"
        return report

    if fetched:
        # The helper below only fetches the requested events; it does not
        # recursively fetch their auth chains. Pull the full chain first so
        # every imported auth event can be checked against its own auth state.
        await hs.get_federation_event_handler()._get_remote_auth_chain_for_event(
            via, room_id, event_id
        )
        await hs.get_federation_event_handler()._get_events_and_persist(
            via, room_id, [event_id]
        )
        persisted = await store.get_event(
            event_id, allow_none=True, allow_rejected=True
        )
        if persisted is None:
            return refuse("event could not be persisted")
        if persisted.rejected_reason is not None:
            return refuse(f"event was rejected on persist: {persisted.rejected_reason}")

    for step in plan:
        new_group = await controllers.state.store_state_group(
            step["extremities"][0],
            room_id,
            room_version,
            prev_group=step["old_state_group"],
            delta_ids={key: event_id},
            current_state_ids=None,
        )
        step["new_state_group"] = new_group
        for extremity in step["extremities"]:
            await store.repoint_event_state_group(extremity, new_group)

    assert controllers.persistence is not None
    await controllers.persistence.update_current_state(room_id)
    report["published"] = True
    return report


async def _plan_state_deltas(
    hs: HomeServer,
    room_id: str,
    deltas: dict[tuple[str, str], str],
) -> list[dict[str, Any]]:
    """Work out the state-group rewrites needed to apply `deltas`.

    Returns one entry per distinct forward-extremity state group, with the
    one-key (or few-key) delta that must be written to repair that group. The
    plan is read-only; `_apply_state_deltas` performs the writes.
    """
    store = hs.get_datastores().main
    controllers = hs.get_storage_controllers()

    latest_event_ids = await store.get_latest_event_ids_in_room(room_id)
    group_by_extremity = await store._get_state_group_for_events(latest_event_ids)

    extremities_by_group: dict[int, list[str]] = {}
    for extremity, group in group_by_extremity.items():
        extremities_by_group.setdefault(group, []).append(extremity)

    plan: list[dict[str, Any]] = []
    for group, extremities in sorted(extremities_by_group.items()):
        state = await controllers.state.get_state_ids_for_group(group)
        group_delta = {
            key: event_id
            for key, event_id in deltas.items()
            if state.get(key) != event_id
        }
        if not group_delta:
            continue

        plan.append(
            {
                "old_state_group": group,
                "extremities": sorted(extremities),
                "delta": group_delta,
                "changes": [
                    {
                        "type": key[0],
                        "state_key": key[1],
                        "from": state.get(key),
                        "to": event_id,
                    }
                    for key, event_id in sorted(group_delta.items())
                ],
            }
        )

    return plan


async def _apply_state_deltas(
    hs: HomeServer,
    room_id: str,
    plan: list[dict[str, Any]],
) -> None:
    """Write the state groups in `plan`, repoint the extremities, and recalc
    current state through the normal persistence controller."""
    if not plan:
        return

    store = hs.get_datastores().main
    controllers = hs.get_storage_controllers()
    room_version = await store.get_room_version(room_id)

    for step in plan:
        new_group = await controllers.state.store_state_group(
            step["extremities"][0],
            room_id,
            room_version,
            prev_group=step["old_state_group"],
            delta_ids=step["delta"],
            current_state_ids=None,
        )
        step["new_state_group"] = new_group
        for extremity in step["extremities"]:
            await store.repoint_event_state_group(extremity, new_group)

    assert controllers.persistence is not None
    await controllers.persistence.update_current_state(room_id)


async def _import_state(
    hs: HomeServer,
    room_id: str,
    via: str,
    at_event: str | None,
    max_diff: int,
    publish: bool,
) -> dict[str, Any]:
    """Compare local state with a remote server's state at our tip.

    The comparison is by state event ID, so it finds keys where the remote has a
    different (or newer) event than we do. Only the symmetric difference is
    fetched and signature-checked; unchanged keys are not re-fetched.

    Dry-run by default. `--publish` imports the remote events that are missing
    locally (or differ), then applies their state keys to the forward
    extremities through the same state-group rewrite as `repair-member`.
    Keys that exist only locally are reported but never removed.
    """
    store = hs.get_datastores().main
    controllers = hs.get_storage_controllers()
    report: dict[str, Any] = {
        "room_id": room_id,
        "via": via,
        "at_event": at_event,
        "published": False,
        "refused": None,
    }

    def refuse(reason: str) -> dict[str, Any]:
        report["refused"] = reason
        return report

    if await store.is_partial_state_room(room_id):
        return refuse("room is partial-state")
    room_version = await store.get_room_version(room_id)
    if room_version.msc4242_state_dags:
        return refuse("MSC4242 state-DAG rooms are not supported by repair-state")

    latest_event_ids = await store.get_latest_event_ids_in_room(room_id)
    if at_event is None:
        if len(latest_event_ids) != 1:
            return refuse(
                "room has %d forward extremities; pass --at-event to choose one"
                % len(latest_event_ids)
            )
        at_event = next(iter(latest_event_ids))
    elif at_event not in latest_event_ids:
        return refuse(
            "--at-event must be the room's single forward extremity; use "
            "repair-state sweep for historical ranges"
        )
    report["at_event"] = at_event

    group_by_event = await store._get_state_group_for_events({at_event})
    state_group = group_by_event.get(at_event)
    if state_group is None:
        return refuse(f"no state group for {at_event}")

    local_state = await controllers.state.get_state_ids_for_group(state_group)
    local_event_ids = set(local_state.values())

    try:
        (
            remote_ids,
            _remote_auth_ids,
        ) = await hs.get_federation_client().get_room_state_ids(via, room_id, at_event)
    except Exception as e:
        return refuse(f"failed to fetch state from {via}: {e}")

    remote_event_ids = set(remote_ids)
    only_remote_ids = remote_event_ids - local_event_ids
    only_local_ids = local_event_ids - remote_event_ids

    report["counts"] = {
        "local_state_events": len(local_event_ids),
        "remote_state_events": len(remote_event_ids),
        "only_remote": len(only_remote_ids),
        "only_local": len(only_local_ids),
    }

    if len(only_remote_ids) > max_diff or len(only_local_ids) > max_diff:
        return refuse(
            "state difference exceeds --max-diff (%d); refusing to fetch a "
            "large diff" % max_diff
        )

    tip_event = await store.get_event(at_event, allow_none=True)
    if (
        tip_event is not None
        and tip_event.is_state()
        and at_event not in remote_event_ids
    ):
        # The remote did not return the event we asked for in the state at that
        # event, so it probably does not know it. Its answer is not a state at
        # our tip and must not be applied.
        report["warning"] = (
            f"{via} did not include {at_event} in the state it returned for "
            "that event; it may not know the event"
        )
        if publish:
            return refuse(report["warning"])

    errors: list[dict[str, str]] = []
    remote_events: dict[tuple[str, str], EventBase] = {}
    for event_id in sorted(only_remote_ids):
        # If we already hold the event (for example as an outlier from an
        # earlier send_join), reuse it instead of asking the remote again. It
        # was verified when it was first persisted.
        event = await store.get_event(event_id, allow_none=True, allow_rejected=True)
        if event is None:
            pulled = await hs.get_federation_client().get_pdu(
                [via], event_id, room_version
            )
            if pulled is None:
                errors.append({"event_id": event_id, "error": "not returned"})
                continue
            event = pulled.pdu
        if event.room_id != room_id:
            errors.append(
                {"event_id": event_id, "error": f"belongs to {event.room_id}"}
            )
            continue
        if not event.is_state():
            errors.append({"event_id": event_id, "error": "not a state event"})
            continue
        if event.rejected_reason is not None:
            errors.append(
                {
                    "event_id": event_id,
                    "error": f"rejected: {event.rejected_reason}",
                }
            )
            continue
        remote_events[(event.type, event.state_key)] = event

    local_events: dict[tuple[str, str], EventBase | None] = {}
    for key, event_id in local_state.items():
        local_events[key] = await store.get_event(
            event_id, allow_none=True, allow_rejected=True
        )

    # Keys whose event ID is present on both sides are unchanged. We only have
    # the remote event objects for the symmetric difference, so map the shared
    # IDs back through their local keys.
    remote_state_by_key: dict[tuple[str, str], str] = {
        key: event_id
        for key, event_id in local_state.items()
        if event_id in remote_event_ids
    }
    remote_state_by_key.update(
        {key: event.event_id for key, event in remote_events.items()}
    )

    changes: list[dict[str, Any]] = []
    deltas: dict[tuple[str, str], str] = {}
    for key in sorted(set(local_state) | set(remote_state_by_key)):
        local_id = local_state.get(key)
        remote_event = remote_events.get(key)
        remote_id = remote_state_by_key.get(key)
        if local_id == remote_id:
            continue

        if remote_event is None:
            action = "keep-local"
        elif local_id is None:
            action = "import"
            deltas[key] = remote_event.event_id
        else:
            action = "replace"
            deltas[key] = remote_event.event_id

        changes.append(
            {
                "type": key[0],
                "state_key": key[1],
                "action": action,
                "local": _event_summary(local_events.get(key)),
                "remote": _event_summary(remote_event),
            }
        )

    report["changes"] = changes
    report["errors"] = errors
    if errors:
        report["note"] = (
            "some remote state events could not be fetched and verified; "
            "refusing to publish a partial diff"
        )
        if publish:
            return refuse(report["note"])

    if not changes:
        report["note"] = "nothing to do: local and remote state agree at this event"
        return report

    plan = await _plan_state_deltas(hs, room_id, deltas)
    report["plan"] = plan
    if not plan:
        report["note"] = "nothing to do: every state group already has these events"
        return report

    if not publish:
        report["note"] = "dry run: pass --publish to import and apply"
        return report

    import_event_ids = sorted(set(deltas.values()))
    missing_event_ids = [
        event_id
        for event_id in import_event_ids
        if await store.get_event(event_id, allow_none=True) is None
    ]
    if missing_event_ids:
        federation_event_handler = hs.get_federation_event_handler()
        for event_id in missing_event_ids:
            await federation_event_handler._get_remote_auth_chain_for_event(
                via, room_id, event_id
            )
        await federation_event_handler._get_events_and_persist(
            via, room_id, missing_event_ids
        )

    rejected: list[dict[str, str]] = []
    for event_id in import_event_ids:
        persisted = await store.get_event(
            event_id, allow_none=True, allow_rejected=True
        )
        if persisted is None:
            rejected.append({"event_id": event_id, "error": "not persisted"})
        elif persisted.rejected_reason is not None:
            rejected.append({"event_id": event_id, "error": persisted.rejected_reason})
    if rejected:
        report["rejected_on_persist"] = rejected
        return refuse("one or more imported events were rejected on persist")

    await _apply_state_deltas(hs, room_id, plan)
    report["published"] = True
    return report


async def _run_command(
    hs: HomeServer,
    command: Command,
    room_ids: list[str],
    limit: int,
    reverse: bool,
    repair: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = hs.get_datastores().main

    if command == "repair-member":
        assert repair is not None
        result = await _repair_member(hs, room_ids[0], **repair)
        return {
            "command": command,
            "mode": "publish" if repair["publish"] else "dry_run",
            "rooms": [result],
        }
    if command == "repair-state-import":
        assert repair is not None
        result = await _import_state(hs, room_ids[0], **repair)
        return {
            "command": command,
            "mode": "publish" if repair["publish"] else "dry_run",
            "rooms": [result],
        }

    rooms = []
    for room_id in room_ids:
        if command == "check-room":
            rooms.append(
                await store.db_pool.runInteraction(
                    "synapse_state_repair_check_room",
                    _discover_room_txn,
                    room_id,
                    store,
                )
            )
        elif command == "list-rejected":
            events = await store.db_pool.runInteraction(
                "synapse_state_repair_list_rejected",
                _list_rejected_txn,
                room_id,
                limit,
                reverse,
            )
            rooms.append({"room_id": room_id, "events": events, "limited_to": limit})
        elif command == "list-outliers":
            events = await store.db_pool.runInteraction(
                "synapse_state_repair_list_outliers",
                _list_outliers_txn,
                room_id,
                limit,
                reverse,
            )
            rooms.append({"room_id": room_id, "events": events, "limited_to": limit})
        else:
            raise AssertionError(f"Unknown command: {command}")

    return {
        "command": command,
        "mode": "dry_run",
        "rooms": rooms,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnostics and narrow in-place repairs for room state."
    )
    parser.add_argument("-v", action="store_true")
    parser.add_argument(
        "--config",
        type=argparse.FileType("r"),
        required=True,
        help="Synapse homeserver config.",
    )
    parser.add_argument(
        "--write-report",
        help="Write JSON discovery report to this path instead of stdout.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Apply a repair (repair-member or repair-state). Default is a dry run.",
    )
    parser.add_argument(
        "--homeserver-is-stopped",
        action="store_true",
        help="Confirm the homeserver is not running; required with --publish.",
    )
    subparsers = parser.add_subparsers(dest="command")

    def add_room_args(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--room",
            action="append",
            default=[],
            help="Room ID to inspect. May be repeated.",
        )
        subparser.add_argument(
            "--room-file",
            help="File containing room IDs to inspect, one per line.",
        )

    check_room = subparsers.add_parser("check-room", help="Summarise repair inputs.")
    add_room_args(check_room)

    list_rejected = subparsers.add_parser(
        "list-rejected", help="List rejected events in a room."
    )
    add_room_args(list_rejected)
    list_rejected.add_argument("--limit", type=int, default=100)
    list_rejected.add_argument("--reverse", action="store_true")

    repair_member = subparsers.add_parser(
        "repair-member",
        help="Repoint one m.room.member state key at an event fetched from a server.",
    )
    repair_member.add_argument("--room", required=True)
    repair_member.add_argument("--event-id", required=True)
    repair_member.add_argument(
        "--via", required=True, help="Server to fetch the event from."
    )

    repair_state = subparsers.add_parser(
        "repair-state",
        help="Compare and import state from another server.",
    )
    repair_state_sub = repair_state.add_subparsers(dest="repair_state_command")
    repair_state_import = repair_state_sub.add_parser(
        "import",
        help=(
            "Import state keys that differ from a remote server's state at our "
            "single forward extremity."
        ),
    )
    repair_state_import.add_argument("--room", required=True)
    repair_state_import.add_argument(
        "--via", required=True, help="Server to compare with and fetch events from."
    )
    repair_state_import.add_argument(
        "--at-event",
        help=(
            "Local forward extremity to compare at. Defaults to the room's "
            "single forward extremity."
        ),
    )
    repair_state_import.add_argument(
        "--max-diff",
        type=int,
        default=500,
        help="Refuse if more than this many state events differ (default: 500).",
    )

    list_outliers = subparsers.add_parser(
        "list-outliers", help="List outlier events in a room."
    )
    add_room_args(list_outliers)
    list_outliers.add_argument("--limit", type=int, default=100)
    list_outliers.add_argument("--reverse", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.v else logging.INFO,
        format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
    )

    if not args.command:
        parser.print_help()
        parser.error(
            "a subcommand is required "
            "(check-room, list-rejected, list-outliers, repair-member, "
            "repair-state import)"
        )

    command: Command
    if args.command == "repair-state":
        if args.repair_state_command != "import":
            repair_state.print_help()
            parser.error("a repair-state subcommand is required (import)")
        command = "repair-state-import"
    else:
        command = cast(Command, args.command)

    if args.publish and command not in ("repair-member", "repair-state-import"):
        parser.error("--publish only applies to repair-member or repair-state import")
    if args.publish and not args.homeserver_is_stopped:
        parser.error(
            "--publish requires --homeserver-is-stopped: this tool allocates "
            "stream IDs and must not run alongside a live writer"
        )

    repair: dict[str, Any] | None = None
    if command == "repair-member":
        room_ids = [args.room]
        repair = {
            "event_id": args.event_id,
            "via": args.via,
            "publish": bool(args.publish),
        }
    elif command == "repair-state-import":
        room_ids = [args.room]
        repair = {
            "at_event": args.at_event,
            "via": args.via,
            "max_diff": max(0, int(args.max_diff)),
            "publish": bool(args.publish),
        }
    else:
        room_ids = _load_room_ids(args.room, getattr(args, "room_file", None))
    if not room_ids:
        parser.error("at least one --room or --room-file entry is required")
    limit = max(0, int(getattr(args, "limit", 100)))
    reverse = bool(getattr(args, "reverse", False))

    hs_config = yaml.safe_load(args.config)
    config = HomeServerConfig()
    config.parse_config_dict(hs_config, "", "")

    hs = RepairHomeserver(config)
    hs.setup()

    exit_code = 0

    async def run() -> None:
        nonlocal exit_code
        try:
            report = await _run_command(hs, command, room_ids, limit, reverse, repair)
            report_json = json.dumps(report, indent=2, sort_keys=True)
            if args.write_report:
                with open(args.write_report, "w") as f:
                    f.write(report_json)
                    f.write("\n")
            else:
                print(report_json)
        except Exception:
            logger.exception("State repair command failed")
            exit_code = 1
        finally:
            reactor.stop()

    hs.get_clock().call_when_running(lambda: defer.ensureDeferred(run()))
    reactor.run()
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
