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
from synapse.server import HomeServer
from synapse.storage import DataStore
from synapse.storage.database import LoggingTransaction
from synapse.types import ISynapseReactor

reactor = cast(ISynapseReactor, reactor_)
logger = logging.getLogger("synapse_state_repair")


class RepairHomeserver(HomeServer):
    DATASTORE_CLASS = DataStore

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


Command = Literal["check-room", "list-rejected", "list-outliers"]


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


def _discover_room_txn(txn: LoggingTransaction, room_id: str) -> dict[str, Any]:
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


async def _run_command(
    hs: HomeServer,
    command: Command,
    room_ids: list[str],
    limit: int,
    reverse: bool,
) -> dict[str, Any]:
    store = hs.get_datastores().main

    rooms = []
    for room_id in room_ids:
        if command == "check-room":
            rooms.append(
                await store.db_pool.runInteraction(
                    "synapse_state_repair_check_room",
                    _discover_room_txn,
                    room_id,
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
        description="Read-only diagnostics for future in-place state repair."
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
        help="Reserved for the future write-capable repair mode.",
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

    if args.publish:
        parser.error(
            "--publish is not implemented; this command is currently read-only"
        )

    if not args.command:
        parser.print_help()
        parser.error(
            "a subcommand is required (check-room, list-rejected, list-outliers)"
        )

    command = cast(Command, args.command)
    room_ids = _load_room_ids(args.room, args.room_file)
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
            report = await _run_command(hs, command, room_ids, limit, reverse)
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
