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

        # The tool never runs the homeserver, but some stores change what they
        # register or schedule based on these flags. Force them off so a repair
        # invocation cannot start pushers or background work, whatever the
        # config says.
        self.config.worker.run_background_tasks = False
        self.config.worker.start_pushers = False


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
        "state_key": event.get_state_key(),
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
        if publish:
            assert controllers.persistence is not None
            await controllers.persistence.update_current_state(room_id)
        report["note"] = "nothing to do: every extremity already has this event"
        return report

    unrepresentable = await _unrepresentable_self_rewrite(
        hs, plan, "extremities", fallback_delta={key: event_id}
    )
    if unrepresentable is not None:
        report["unrepresentable"] = unrepresentable
        return refuse(
            "a selected state event would be repointed to a state group that "
            "changes its own key; a repair cannot preserve both historical "
            "mappings"
        )

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
    selected_extremity: str | None = None,
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
        if selected_extremity is not None and selected_extremity not in extremities:
            continue
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


async def _unrepresentable_self_rewrite(
    hs: HomeServer,
    plan: list[dict[str, Any]],
    events_key: str,
    fallback_delta: dict[tuple[str, str], str] | None = None,
) -> dict[str, Any] | None:
    """Find a state event whose own state key the repair would rewrite.

    `event_to_state_groups` maps each event to the state *after* it. Repointing
    a state event to a group where its own key names a different event would
    make historical state reads claim that event replaced itself. Message
    events have no own key and are always safe to repoint.
    """
    store = hs.get_datastores().main
    for step in plan:
        delta = step.get("delta", fallback_delta or {})
        if not delta:
            continue
        event_ids = step.get(events_key, ())
        events = await store.get_events(event_ids, allow_rejected=True)
        for event_id in event_ids:
            event = events.get(event_id)
            if event is None or not event.is_state():
                continue
            key = (event.type, event.state_key)
            replacement = delta.get(key)
            if replacement is not None and replacement != event_id:
                return {
                    "event_id": event_id,
                    "type": event.type,
                    "state_key": event.get_state_key(),
                    "would_be": replacement,
                }
    return None


class _PropagationError(Exception):
    """The propagation plan cannot be built safely."""


def _descendant_state_groups_txn(txn: LoggingTransaction, seed_group: int) -> set[int]:
    """Return the seed group and every group that descends from it.

    `state_group_edges` records the group each new group was derived from, so
    this finds the groups whose state is a descendant of the state at the seed
    even when the leave itself is not in the local event DAG.
    """
    txn.execute(
        """
        WITH RECURSIVE descendants(state_group) AS (
            SELECT CAST(? AS BIGINT)
            UNION
            SELECT edges.state_group
            FROM state_group_edges AS edges
            JOIN descendants ON edges.prev_state_group = descendants.state_group
        )
        SELECT state_group FROM descendants
        """,
        (seed_group,),
    )
    return {int(row[0]) for row in txn.fetchall()}


def _event_stream_ordering_txn(txn: LoggingTransaction, event_id: str) -> int | None:
    txn.execute("SELECT stream_ordering FROM events WHERE event_id = ?", (event_id,))
    row = txn.fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def _room_events_from_txn(
    txn: LoggingTransaction,
    room_id: str,
    stream_ordering: int,
    limit: int,
) -> list[tuple[str, int]]:
    """Return the non-outlier events persisted at or after `stream_ordering`."""
    txn.execute(
        """
        SELECT event_id, stream_ordering
        FROM events
        WHERE room_id = ? AND NOT outlier AND stream_ordering >= ?
        ORDER BY stream_ordering, event_id
        LIMIT ?
        """,
        (room_id, stream_ordering, limit + 1),
    )
    return [(row[0], int(row[1])) for row in txn.fetchall()]


def _rejoin_candidates_txn(
    txn: LoggingTransaction, room_id: str, auth_event_id: str, limit: int
) -> list[str]:
    """Events whose stored auth_events name `auth_event_id`."""
    txn.execute(
        """
        SELECT ea.event_id
        FROM event_auth AS ea
        JOIN events AS e USING (event_id)
        WHERE ea.room_id = ? AND ea.auth_id = ? AND e.room_id = ?
        ORDER BY e.stream_ordering
        LIMIT ?
        """,
        (room_id, auth_event_id, room_id, limit),
    )
    return [row[0] for row in txn.fetchall()]


async def _events_authenticated_by(
    store: Any, room_id: str, auth_event_id: str, limit: int
) -> list[str]:
    """Event IDs that name `auth_event_id` in their auth_events.

    `event_auth` covers imported edges; when the embedded event store is in
    use, recently persisted children only exist there, so union both.
    """
    event_ids = set(
        await store.db_pool.runInteraction(
            "synapse_state_repair_auth_children",
            _rejoin_candidates_txn,
            room_id,
            auth_event_id,
            limit,
        )
    )

    if getattr(store, "_embedded_event_json_enabled", False):
        try:
            from synapse.storage.databases.main.embedded_event_auth_chains import (
                get_embedded_auth_children_batch,
                resolve_event_ids_to_short_ids,
                resolve_namespace,
                resolve_short_ids_to_event_ids,
            )

            namespace = resolve_namespace(store)
            engine_name = getattr(store, "_embedded_db_engine", None)
            if namespace is not None:
                short_ids = resolve_event_ids_to_short_ids(
                    engine_name, namespace, room_id, [auth_event_id]
                )
                if short_ids and short_ids[0] is not None:
                    children = get_embedded_auth_children_batch(
                        engine_name, namespace, room_id, [short_ids[0]]
                    ).get(short_ids[0], [])
                    for child in resolve_short_ids_to_event_ids(
                        engine_name, namespace, room_id, children[:limit]
                    ):
                        if child is not None:
                            event_ids.add(child)
        except Exception:
            logger.warning(
                "could not read embedded auth children for %s; the auth review "
                "may be incomplete",
                auth_event_id,
                exc_info=True,
            )

    return sorted(event_ids)[:limit]


async def _plan_propagation(
    hs: HomeServer,
    room_id: str,
    seed_event: EventBase,
    seed_group: int,
    seed_state: dict[tuple[str, str], str],
    deltas: dict[tuple[str, str], str],
    max_events: int,
) -> dict[str, Any]:
    """Plan the state-group rewrites that carry `deltas` down from the seed.

    Every descendant group whose value for a changed key is still the seed's
    value is rewritten as `old_group + delta`. A group with a different value
    for that key is a later legitimate change: it is left alone and reported
    as a stop. Groups with no local event need no rewrite and are skipped.
    """
    store = hs.get_datastores().main
    controllers = hs.get_storage_controllers()

    descendant_groups = await store.db_pool.runInteraction(
        "synapse_state_repair_descendant_groups",
        _descendant_state_groups_txn,
        seed_group,
    )

    seed_stream_ordering = await store.db_pool.runInteraction(
        "synapse_state_repair_seed_stream_ordering",
        _event_stream_ordering_txn,
        seed_event.event_id,
    )
    if seed_stream_ordering is None:
        raise _PropagationError(f"{seed_event.event_id} has no local stream ordering")

    rows = await store.db_pool.runInteraction(
        "synapse_state_repair_room_events",
        _room_events_from_txn,
        room_id,
        seed_stream_ordering,
        max_events,
    )
    if len(rows) > max_events:
        raise _PropagationError(
            "more than --max-events (%d) local events follow the seed; raise the "
            "limit or seed at a later event" % max_events
        )

    event_ids = [event_id for event_id, _ in rows]
    group_by_event: dict[str, int] = {}
    for start in range(0, len(event_ids), 1000):
        group_by_event.update(
            await store._get_state_group_for_events(
                event_ids[start : start + 1000], raise_on_missing=False
            )
        )

    events_by_group: dict[int, list[str]] = {}
    not_descendant = 0
    without_group = 0
    for event_id in event_ids:
        group = group_by_event.get(event_id)
        if group is None:
            without_group += 1
        elif group not in descendant_groups:
            not_descendant += 1
        else:
            events_by_group.setdefault(group, []).append(event_id)

    keys = list(deltas)
    plan: list[dict[str, Any]] = []
    stops: list[dict[str, Any]] = []
    stop_markers: set[tuple[tuple[str, str], str | None]] = set()
    already_correct = 0
    empty_groups = 0

    for group in sorted(descendant_groups):
        state = await controllers.state.get_state_ids_for_group(
            group, StateFilter.from_types(keys)
        )
        group_delta: dict[tuple[str, str], str] = {}
        for key in keys:
            current = state.get(key)
            if current == deltas[key]:
                already_correct += 1
            elif current == seed_state.get(key):
                group_delta[key] = deltas[key]
            else:
                marker = (key, current)
                if marker not in stop_markers:
                    stop_markers.add(marker)
                    stops.append(
                        {
                            "type": key[0],
                            "state_key": key[1],
                            "state_group": group,
                            "current": current,
                            "seed_value": seed_state.get(key),
                            "would_be": deltas[key],
                        }
                    )
        if not group_delta:
            continue

        events = sorted(events_by_group.get(group, ()))
        if not events:
            empty_groups += 1
            continue

        plan.append(
            {
                "old_state_group": group,
                "events": events,
                "event_count": len(events),
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

    review: list[dict[str, Any]] = []
    for key, new_event_id in sorted(deltas.items()):
        old_event_id = seed_state.get(key)
        if old_event_id is None:
            continue
        candidates = await _events_authenticated_by(store, room_id, old_event_id, 50)
        if not candidates:
            continue
        auth_events = []
        for event_id in candidates:
            event = await store.get_event(
                event_id, allow_none=True, allow_rejected=True
            )
            if event is not None:
                summary = _event_summary(event)
                if summary is not None:
                    auth_events.append(summary)
        review.append(
            {
                "type": key[0],
                "state_key": key[1],
                "replaced": old_event_id,
                "replacement": new_event_id,
                "events": auth_events,
            }
        )

    return {
        "groups": plan,
        "group_count": len(plan),
        "event_count": sum(step["event_count"] for step in plan),
        "already_correct": already_correct,
        "stops": stops,
        "groups_without_events": empty_groups,
        "events_without_state_group": without_group,
        "events_not_descended_from_seed": not_descendant,
        "auth_review": review,
        "note": "auth_review is report-only; existing rejections are not changed",
    }


_MAX_LOGGED_REPOINT_FAILURES = 5


def _serializable_plan_steps(steps: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Plan steps for the JSON report.

    Keeps each step's group IDs, affected event IDs and changes for auditing and
    rollback. Only the internal `delta` is dropped: it is keyed by
    `(type, state_key)` tuples, which JSON cannot represent.
    """
    return [{k: v for k, v in step.items() if k != "delta"} for step in steps]


async def _apply_propagation(
    hs: HomeServer, room_id: str, plan: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Write planned groups and return any work that could not be completed."""
    if not plan:
        return []

    store = hs.get_datastores().main
    controllers = hs.get_storage_controllers()
    room_version = await store.get_room_version(room_id)

    incomplete: list[dict[str, Any]] = []
    for step in plan:
        try:
            new_group = await controllers.state.store_state_group(
                step["events"][0],
                room_id,
                room_version,
                prev_group=step["old_state_group"],
                delta_ids=step["delta"],
                current_state_ids=None,
            )
        except Exception as e:
            logger.exception(
                "Failed to create propagated state group from %d",
                step["old_state_group"],
            )
            incomplete.append(
                {
                    "old_state_group": step["old_state_group"],
                    "events": step["events"],
                    "error": str(e),
                }
            )
            continue
        step["new_state_group"] = new_group
        failed_events: list[str] = []
        for event_id in step["events"]:
            try:
                await store.repoint_event_state_group(event_id, new_group)
            except Exception:
                # Bound the tracebacks: a systemic failure over a large plan
                # would otherwise log one per event. Every failure is still
                # recorded in `incomplete_work`.
                if len(failed_events) < _MAX_LOGGED_REPOINT_FAILURES:
                    logger.exception(
                        "Failed to repoint event %s to propagated state group %d",
                        event_id,
                        new_group,
                    )
                failed_events.append(event_id)
        if failed_events:
            incomplete.append(
                {
                    "old_state_group": step["old_state_group"],
                    "new_state_group": new_group,
                    "events": failed_events,
                    "error": "one or more event mappings could not be repointed",
                }
            )

    assert controllers.persistence is not None
    try:
        await controllers.persistence.update_current_state(room_id)
    except Exception as e:
        logger.exception("Failed to recalculate current state after propagation")
        incomplete.append({"room_id": room_id, "error": str(e)})
    return incomplete


async def _import_state(
    hs: HomeServer,
    room_id: str,
    via: str,
    at_event: str | None,
    max_diff: int,
    publish: bool,
    propagate: bool = False,
    max_events: int = 200_000,
) -> dict[str, Any]:
    """Compare local state with a remote server's state at our tip.

    The comparison is by state event ID, so it finds keys where the remote has a
    different (or newer) event than we do. Only the symmetric difference is
    fetched and signature-checked; unchanged keys are not re-fetched.

    Dry-run by default. `--publish` imports the remote events that are missing
    locally (or differ), then applies their state keys to the forward
    extremities through the same state-group rewrite as `repair-member`.
    Keys that exist only locally are reported but never removed.

    With `propagate=True`, `at_event` may be any local non-outlier event, and
    the changed keys are carried forward: every descendant state group that
    still holds the seed value is rewritten as `old_group + delta`, and its
    events are repointed. Groups with a later value for a key are left alone
    and reported as stops. This repairs historical state maps, not just the
    tip. The leave itself is still only an outlier, so it is not inserted into
    the timeline.
    """
    store = hs.get_datastores().main
    controllers = hs.get_storage_controllers()
    report: dict[str, Any] = {
        "room_id": room_id,
        "via": via,
        "at_event": at_event,
        "propagate": propagate,
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
    else:
        event = await store.get_event(at_event, allow_none=True, allow_rejected=True)
        if event is None or event.room_id != room_id:
            return refuse(f"{at_event} is not a local event in this room")
        if at_event not in latest_event_ids and not propagate:
            return refuse(
                "--at-event must be a forward extremity unless --propagate is used"
            )
    report["at_event"] = at_event

    group_by_event = await store._get_state_group_for_events(
        {at_event}, raise_on_missing=False
    )
    state_group = group_by_event.get(at_event)
    if state_group is None:
        return refuse(
            f"no state group for {at_event}; an outlier has no state to compare from"
        )

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
    tip_was_returned = at_event in remote_event_ids
    tip_event = await store.get_event(at_event, allow_none=True)
    tip_key: tuple[str, str] | None = None
    if tip_event is not None and tip_event.is_state():
        # The state event the tip supersedes is recorded locally in
        # `replaces_state` (set when the event's context was computed).
        replaced = tip_event.unsigned.get("replaces_state")
        if isinstance(replaced, str) and not tip_was_returned:
            # The requested event is absent because /state_ids reports the
            # before-event snapshot. Its predecessor is therefore obsolete in
            # the equivalent after-event snapshot: exclude it before applying
            # --max-diff or fetching remote bodies.
            remote_event_ids.discard(replaced)
        # /state_ids is the state before the requested event. Include the
        # accepted state event to compare the same after-event snapshot locally.
        remote_event_ids.add(at_event)
        tip_key = (tip_event.type, tip_event.state_key)
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
        if event.event_id != event_id:
            errors.append({"event_id": event_id, "error": f"returned {event.event_id}"})
            continue
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
        key = (event.type, event.state_key)
        if key == tip_key and not tip_was_returned:
            # /state_ids returns the state *before* the requested event, so an
            # event for the tip's own key is the value the tip supersedes.
            continue
        previous = remote_events.get(key)
        if previous is not None and previous.event_id != event.event_id:
            errors.append(
                {
                    "event_id": event_id,
                    "error": f"conflicts with {previous.event_id} for state key {key}",
                }
            )
            continue
        remote_events[key] = event

    # Keys whose event ID is present on both sides are unchanged. We only have
    # the remote event objects for the symmetric difference, so map the shared
    # IDs back through their local keys.
    remote_state_by_key: dict[tuple[str, str], str] = {
        key: event_id
        for key, event_id in local_state.items()
        if event_id in remote_event_ids
    }
    for key, event in remote_events.items():
        shared_id = remote_state_by_key.get(key)
        if shared_id is not None and shared_id != event.event_id:
            errors.append(
                {
                    "event_id": event.event_id,
                    "error": f"conflicts with shared event {shared_id} for state key {key}",
                }
            )
        else:
            remote_state_by_key[key] = event.event_id

    changed_local_ids = {
        local_id
        for key, local_id in local_state.items()
        if remote_state_by_key.get(key) != local_id
    }
    local_events = await store.get_events(changed_local_ids, allow_rejected=True)

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
                "local": (
                    _event_summary(local_events.get(local_id))
                    if local_id is not None
                    else None
                ),
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
        # Do not reconcile other forward-extremity groups against this remote
        # snapshot here. With no seed diff there is no repair plan to resume:
        # the groups and delta of an interrupted publish are not recorded, and
        # the snapshot describes `at_event`, so applying it to another
        # extremity would also roll back keys that changed legitimately after
        # the seed. This applies equally to --propagate, whose snapshot is
        # historical.
        report["plan"] = []
        report["note"] = (
            "nothing to do: local and remote state agree at this event; "
            "other groups are not re-examined without a seed diff"
        )
        return report

    if propagate:
        seed_event = await store.get_event(
            at_event, allow_none=True, allow_rejected=True
        )
        if seed_event is None:
            return refuse(f"{at_event} is no longer available locally")
        try:
            propagation = await _plan_propagation(
                hs,
                room_id,
                seed_event,
                state_group,
                dict(local_state),
                deltas,
                max_events,
            )
        except _PropagationError as e:
            return refuse(str(e))
        report["propagation"] = {
            **propagation,
            "groups": _serializable_plan_steps(propagation["groups"]),
        }
        plan = propagation["groups"]
        report["plan"] = _serializable_plan_steps(plan)
        if not plan:
            incomplete = bool(
                propagation["groups_without_events"] or propagation["stops"]
            )
            report["incomplete"] = incomplete
            if publish:
                assert controllers.persistence is not None
                await controllers.persistence.update_current_state(room_id)
            report["note"] = (
                "incomplete: some descendant state groups need review"
                if incomplete
                else "nothing to do: no descendant state group still has the seed value"
            )
            return report
    else:
        plan = await _plan_state_deltas(
            hs, room_id, deltas, selected_extremity=at_event
        )
        report["plan"] = _serializable_plan_steps(plan)
        if not plan:
            report["note"] = "nothing to do: every state group already has these events"
            return report

    unrepresentable = await _unrepresentable_self_rewrite(
        hs, plan, "events" if propagate else "extremities"
    )
    if unrepresentable is not None:
        report["unrepresentable"] = unrepresentable
        return refuse(
            "a state event would be repointed to a state group that changes its "
            "own key; a repair cannot preserve both historical mappings"
        )

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

    if propagate:
        apply_incomplete = await _apply_propagation(hs, room_id, plan)
        # `new_state_group` is filled in while applying; refresh the report.
        report["propagation"]["groups"] = _serializable_plan_steps(plan)
        report["plan"] = _serializable_plan_steps(plan)
        report["propagation"]["incomplete_work"] = apply_incomplete
        report["incomplete"] = bool(
            apply_incomplete
            or report["propagation"]["groups_without_events"]
            or report["propagation"]["stops"]
        )
        if report["incomplete"]:
            if apply_incomplete:
                report["partial"] = True
            report["note"] = (
                "partially published; see propagation.incomplete_work"
                if apply_incomplete
                else "published planned groups; some descendant groups remain incomplete"
            )
    else:
        await _apply_state_deltas(hs, room_id, plan)
        # `new_state_group` is filled in while applying; refresh the report.
        report["plan"] = _serializable_plan_steps(plan)
    # Some propagation work did not complete: the writes that succeeded stay
    # in place (see `partial` and `propagation.incomplete_work`), but this must
    # not read as a clean publish to anything checking `published` alone.
    report["published"] = not report.get("partial", False)
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
            "Local event to compare at. Defaults to the room's single forward "
            "extremity. Any local event is accepted with --propagate."
        ),
    )
    repair_state_import.add_argument(
        "--propagate",
        action="store_true",
        help=(
            "Carry the imported key changes forward through every descendant "
            "state group, repairing historical state maps rather than only the "
            "chosen event's group. Requires --at-event to be the historical "
            "seed (usually the local join)."
        ),
    )
    repair_state_import.add_argument(
        "--max-events",
        type=int,
        default=200_000,
        help=(
            "Refuse --propagate if more than this many local events follow the "
            "seed (default: 200000)."
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
            "propagate": bool(args.propagate),
            "max_events": max(0, int(args.max_events)),
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
