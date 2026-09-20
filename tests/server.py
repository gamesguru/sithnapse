#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2018-2021 The Matrix.org Foundation C.I.C.
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#
import atexit
import hashlib
import ipaddress
import json
import logging
import os
import os.path
import queue
import sqlite3
import sys
import threading
import time
import uuid
import warnings
import weakref
from collections import defaultdict, deque
from io import SEEK_END, BytesIO
from typing import (
    IO,
    Any,
    Awaitable,
    Callable,
    Iterable,
    MutableMapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)
from unittest.mock import Mock, patch

import attr
from incremental import Version
from typing_extensions import ParamSpec
from zope.interface import implementer

import twisted
from twisted.enterprise import adbapi
from twisted.internet import address, defer, tcp, threads, udp
from twisted.internet._resolver import SimpleResolverComplexifier
from twisted.internet.address import IPv4Address, IPv6Address
from twisted.internet.defer import Deferred, fail, maybeDeferred, succeed
from twisted.internet.error import DNSLookupError
from twisted.internet.interfaces import (
    IAddress,
    IConnector,
    IConsumer,
    IHostnameResolver,
    IListeningPort,
    IProducer,
    IProtocol,
    IPullProducer,
    IPushProducer,
    IReactorPluggableNameResolver,
    IReactorTime,
    IResolverSimple,
    ITCPTransport,
    ITransport,
)
from twisted.internet.protocol import ClientFactory, DatagramProtocol, Factory
from twisted.internet.testing import AccumulatingProtocol, MemoryReactorClock
from twisted.python import threadpool
from twisted.python.failure import Failure
from twisted.web.http_headers import Headers
from twisted.web.resource import IResource
from twisted.web.server import Request, Site

from synapse.api.constants import MAX_REQUEST_SIZE
from synapse.config.database import DatabaseConnectionConfig
from synapse.config.homeserver import HomeServerConfig
from synapse.events.auto_accept_invites import InviteAutoAccepter
from synapse.events.presence_router import load_legacy_presence_router
from synapse.handlers.auth import load_legacy_password_auth_providers
from synapse.http.site import SynapseRequest
from synapse.logging.context import ContextResourceUsage
from synapse.module_api.callbacks.spamchecker_callbacks import load_legacy_spam_checkers
from synapse.module_api.callbacks.third_party_event_rules_callbacks import (
    load_legacy_third_party_event_rules,
)
from synapse.server import HomeServer
from synapse.server_notices.consent_server_notices import ConfigError
from synapse.storage import DataStore
from synapse.storage.database import LoggingDatabaseConnection, make_pool
from synapse.storage.engines import BaseDatabaseEngine, create_engine
from synapse.storage.prepare_database import prepare_database
from synapse.types import ISynapseReactor, JsonDict
from synapse.util.clock import Clock
from synapse.util.duration import Duration
from synapse.util.json import json_encoder

from tests.utils import (
    LEAVE_DB,
    POSTGRES_BASE_DB,
    POSTGRES_DBNAME_FOR_INITIAL_CREATE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    SQLITE_PERSIST_DB,
    USE_POSTGRES_FOR_TESTS,
    default_config,
    get_postgres_clone_strategy,
)

logger = logging.getLogger(__name__)

R = TypeVar("R")
P = ParamSpec("P")

# the type of thing that can be passed into `make_request` in the headers list
CustomHeaderType = tuple[str | bytes, str | bytes]

# A pre-prepared SQLite DB that is used as a template when creating new SQLite
# DB each test run. This dramatically speeds up test set up when using SQLite.
PREPPED_SQLITE_DB_CONN: LoggingDatabaseConnection | None = None

# ── Postgres per-test lifecycle timing (opt-in via SYNAPSE_PG_TIMINGS=1) ────
_PG_TIMINGS: dict[str, float] = defaultdict(float)
_PG_TIMING_COUNTS: dict[str, int] = defaultdict(int)
_PG_TIMING_MAX: dict[str, float] = defaultdict(float)
_PG_LIFECYCLE_COUNTERS: dict[str, int] = defaultdict(int)
_PG_TEARDOWN_TEST_TIMINGS: dict[str, dict[str, float]] = defaultdict(
    lambda: defaultdict(float)
)

# Guards the timing dicts: `_pg_timing` is fed from the database layer
# (potentially a different thread than the reactor), while the
# SIGTERM/atexit flushers below sort and iterate it.
_PG_TIMINGS_LOCK = threading.Lock()

_timings_file: IO[str] | None = None
if os.environ.get("SYNAPSE_PG_TIMINGS"):
    _timings_path = os.environ.get("SYNAPSE_PG_TIMINGS_FILE")
    if _timings_path:
        try:
            _timings_file = open(_timings_path, "a")
        except OSError:
            pass


def _timings_print(*args: object) -> None:
    print(*args, file=sys.stderr)
    if _timings_file is not None:
        print(*args, file=_timings_file)


def _pg_timing(tag: str, elapsed: float, test_name: str | None = None) -> None:
    with _PG_TIMINGS_LOCK:
        _PG_TIMINGS[tag] += elapsed
        _PG_TIMING_COUNTS[tag] += 1
        if elapsed > _PG_TIMING_MAX[tag]:
            _PG_TIMING_MAX[tag] = elapsed
        if test_name:
            _PG_TEARDOWN_TEST_TIMINGS[test_name][tag] += elapsed


def _pg_counter(tag: str, count: int = 1) -> None:
    with _PG_TIMINGS_LOCK:
        _PG_LIFECYCLE_COUNTERS[tag] += count


# ── Background Postgres Test DB Dropper & Database Recycler ──────────────────
_DB_DROP_PID: int = os.getpid()
# Keep only a small number of disposable databases waiting to be dropped.
# The test cluster may live on tmpfs, so letting test setup outrun the single
# dropper can otherwise retain an unbounded number of full database clones.
_DB_DROP_QUEUE_MAXSIZE = 2
_DB_DROP_QUEUE: "queue.Queue[tuple[str, str | None, Any] | None]" = queue.Queue(
    maxsize=_DB_DROP_QUEUE_MAXSIZE
)
_DB_DROP_THREAD: threading.Thread | None = None
_DB_DROP_LOCK = threading.Lock()

_RECYCLED_PG_DB: str | None = None
_RECYCLED_PG_DB_IN_USE: bool = False
_PREV_TEST_HAD_DDL: bool = False

_RESEED_SQL = """
INSERT INTO appservice_stream_position VALUES ('X', 0);
INSERT INTO event_push_summary_last_receipt_stream_id VALUES ('X', 0);
INSERT INTO event_push_summary_stream_ordering VALUES ('X', 0);
INSERT INTO stats_incremental_position VALUES ('X', 1);
INSERT INTO user_directory_stream_pos VALUES ('X', 1);
INSERT INTO federation_stream_position VALUES ('federation', -1, 'master'), ('events', -1, 'master');
INSERT INTO device_lists_changes_in_room_max_pruned_stream_id (stream_id) VALUES (0);
INSERT INTO device_lists_changes_converted_stream_position (stream_id, room_id) VALUES (1, '');
INSERT INTO delayed_events_stream_pos (stream_id) VALUES (1);
INSERT INTO room_forgetter_stream_pos (stream_id) VALUES (1);
"""

_RESET_SEQUENCES_SQL = """
SELECT setval('thread_subscriptions_sequence', 2, false);
SELECT setval('events_stream_seq', 1, false);
SELECT setval('receipts_sequence', 1, false);
SELECT setval('presence_stream_sequence', 1, false);
SELECT setval('device_inbox_sequence', 1, false);
SELECT setval('account_data_sequence', 1, false);
SELECT setval('device_lists_sequence', 1, false);
SELECT setval('push_rules_stream_sequence', 1, false);
SELECT setval('pushers_sequence', 1, false);
SELECT setval('cache_invalidation_stream_seq', 1, false);
SELECT setval('un_partial_stated_room_stream_sequence', 1, false);
SELECT setval('un_partial_stated_event_stream_sequence', 1, false);
SELECT setval('e2e_cross_signing_keys_sequence', 1, false);
SELECT setval('sticky_events_sequence', 1, false);
SELECT setval('quarantined_media_id_seq', 1, false);
SELECT setval('profile_updates_sequence', 1, false);
SELECT setval('events_backfill_stream_seq', 1, false);
"""

_METADATA_TABLES_IGNORE = {
    "applied_schema_deltas",
    "schema_version",
    "schema_compat_version",
}


def _reset_recycled_postgres_db(
    test_db: str,
    db_engine: Any,
    test_name: str | None = None,
) -> bool:
    """Reset a recycled test DB. Returns True on success."""

    _t0 = time.monotonic()
    try:
        conn = db_engine.module.connect(
            dbname=test_db,
            user=POSTGRES_USER,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            password=POSTGRES_PASSWORD,
        )
        db_engine.attempt_to_set_autocommit(conn, True)
        cur = conn.cursor()

        try:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid();",
                (test_db,),
            )
        except Exception:
            pass

        # Dirty-table tracking is process-global and SQL-shape-dependent, so it
        # cannot safely determine which rows belong to this particular DB.
        # Truncate every public table except schema identity metadata instead.
        cur.execute(
            "SELECT quote_ident(tablename) FROM pg_tables "
            "WHERE schemaname = 'public' ORDER BY tablename"
        )
        tables_to_truncate = [
            row[0] for row in cur.fetchall() if row[0] not in _METADATA_TABLES_IGNORE
        ]
        if tables_to_truncate:
            cur.execute(
                "TRUNCATE TABLE "
                + ", ".join(tables_to_truncate)
                + " RESTART IDENTITY CASCADE;"
            )

        cur.execute(_RESEED_SQL)

        cur.execute(_RESET_SEQUENCES_SQL)
        cur.close()
        conn.close()
        _pg_timing("db_recycle_reset", time.monotonic() - _t0, test_name=test_name)
        _pg_counter("recycle_reset_success")
        return True
    except Exception as e:
        _pg_counter("recycle_reset_failed")
        warnings.warn(
            f"Failed to reset recycled DB {test_db}: {e}. Falling back to fresh clone.",
            category=UserWarning,
            stacklevel=2,
        )
        return False


def _reset_db_drop_state_after_fork() -> None:
    global _DB_DROP_PID, _DB_DROP_QUEUE, _DB_DROP_THREAD, _DB_DROP_LOCK
    global _RECYCLED_PG_DB, _RECYCLED_PG_DB_IN_USE, _PREV_TEST_HAD_DDL
    _DB_DROP_PID = os.getpid()
    _DB_DROP_QUEUE = queue.Queue(maxsize=_DB_DROP_QUEUE_MAXSIZE)
    _DB_DROP_THREAD = None
    _DB_DROP_LOCK = threading.Lock()
    _RECYCLED_PG_DB = None
    _RECYCLED_PG_DB_IN_USE = False
    _PREV_TEST_HAD_DDL = False


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_db_drop_state_after_fork)


def _ensure_db_drop_worker() -> None:
    global _DB_DROP_PID, _DB_DROP_THREAD
    if os.getpid() != _DB_DROP_PID:
        _reset_db_drop_state_after_fork()
    with _DB_DROP_LOCK:
        if _DB_DROP_THREAD is None or not _DB_DROP_THREAD.is_alive():
            _DB_DROP_THREAD = threading.Thread(
                target=_db_drop_worker_loop,
                name=f"synapse-test-db-dropper-{os.getpid()}",
                daemon=True,
            )
            _DB_DROP_THREAD.start()


def _drop_test_db(
    test_db: str,
    test_name: str | None,
    db_engine: Any,
    conn: Any | None = None,
    cur: Any | None = None,
) -> tuple[bool, Any, Any]:
    """Drop a test database. Reuses or creates the base connection/cursor."""
    import psycopg2

    _drop_t0 = time.monotonic()
    dropped = False

    if conn is None or conn.closed != 0 or cur is None or cur.closed:
        _t_conn = time.monotonic()
        conn = db_engine.module.connect(
            dbname=POSTGRES_DBNAME_FOR_INITIAL_CREATE,
            user=POSTGRES_USER,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            password=POSTGRES_PASSWORD,
        )
        db_engine.attempt_to_set_autocommit(conn, True)
        cur = conn.cursor()
        _pg_timing("db_drop_connect", time.monotonic() - _t_conn, test_name=test_name)

    _t_term = time.monotonic()
    try:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid();",
            (test_db,),
        )
    except psycopg2.Error:
        warnings.warn(
            "Could not terminate backends for %s (non-superuser?)" % (test_db,),
            category=UserWarning,
            stacklevel=2,
        )
    _pg_timing(
        "db_drop_terminate_backends",
        time.monotonic() - _t_term,
        test_name=test_name,
    )

    for attempt in range(5):
        _t_stmt = time.monotonic()
        try:
            cur.execute("DROP DATABASE IF EXISTS %s;" % (test_db,))
            dropped = True
            _pg_timing(
                "db_drop_statement",
                time.monotonic() - _t_stmt,
                test_name=test_name,
            )
            break
        except psycopg2.OperationalError as e:
            _pg_timing(
                "db_drop_statement",
                time.monotonic() - _t_stmt,
                test_name=test_name,
            )
            if attempt < 4:
                warnings.warn(
                    "Couldn't drop old db: " + str(e),
                    category=UserWarning,
                    stacklevel=2,
                )
                _t_sleep = time.monotonic()
                time.sleep(0.5)
                _pg_timing(
                    "db_drop_retry_sleep",
                    time.monotonic() - _t_sleep,
                    test_name=test_name,
                )
                try:
                    conn.close()
                except Exception:
                    pass
                conn = db_engine.module.connect(
                    dbname=POSTGRES_DBNAME_FOR_INITIAL_CREATE,
                    user=POSTGRES_USER,
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    password=POSTGRES_PASSWORD,
                )
                db_engine.attempt_to_set_autocommit(conn, True)
                cur = conn.cursor()

    _pg_timing(
        "db_drop_total",
        time.monotonic() - _drop_t0,
        test_name=test_name,
    )

    if dropped:
        _pg_counter("db_drop_success")
    else:
        _pg_counter("db_drop_failed")
        warnings.warn(
            "Failed to drop old DB %s." % (test_db,),
            category=UserWarning,
            stacklevel=2,
        )

    return dropped, conn, cur


def _db_drop_worker_loop() -> None:
    conn = None
    cur = None
    while True:
        try:
            try:
                item = _DB_DROP_QUEUE.get(timeout=2.0)
            except queue.Empty:
                # Close connection when idle to free Postgres client slots
                if cur and not cur.closed:
                    try:
                        cur.close()
                    except Exception:
                        pass
                    cur = None
                if conn and conn.closed == 0:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                continue

            if item is None:
                _DB_DROP_QUEUE.task_done()
                break
            test_db, test_name, db_engine = item
            try:
                _, conn, cur = _drop_test_db(
                    test_db, test_name, db_engine, conn=conn, cur=cur
                )
            except Exception as e:
                warnings.warn(
                    f"Error in background drop worker for {test_db}: {e}",
                    category=UserWarning,
                    stacklevel=2,
                )
                if cur and not cur.closed:
                    try:
                        cur.close()
                    except Exception:
                        pass
                if conn and conn.closed == 0:
                    try:
                        conn.close()
                    except Exception:
                        pass
                conn, cur = None, None
            finally:
                _DB_DROP_QUEUE.task_done()
        except Exception:
            try:
                _DB_DROP_QUEUE.task_done()
            except ValueError:
                pass

    if cur and not cur.closed:
        try:
            cur.close()
        except Exception:
            pass
    if conn and conn.closed == 0:
        try:
            conn.close()
        except Exception:
            pass


def _drain_db_drop_queue() -> None:
    global _DB_DROP_PID, _RECYCLED_PG_DB
    if os.getpid() != _DB_DROP_PID:
        _reset_db_drop_state_after_fork()
        return

    # Drop the process's recycled database on shutdown
    if _RECYCLED_PG_DB is not None:
        _pg_counter("cleanup_dropped_worker_shutdown")
        db_to_drop = _RECYCLED_PG_DB
        _RECYCLED_PG_DB = None
        _drop_test_db(
            db_to_drop,
            "process_shutdown",
            create_engine({"name": "psycopg2", "args": {}}),
        )

    with _DB_DROP_LOCK:
        thread = _DB_DROP_THREAD
    if thread is not None and thread.is_alive():
        _DB_DROP_QUEUE.join()


def _print_pg_timings() -> None:
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        return
    _drain_db_drop_queue()
    with _PG_TIMINGS_LOCK:
        if not _PG_TIMINGS and not _PG_LIFECYCLE_COUNTERS:
            return
        # Snapshot under the lock so the SIGTERM/atexit flusher never races a
        # concurrent `_pg_timing` on these dicts.
        timings = dict(_PG_TIMINGS)
        counts = dict(_PG_TIMING_COUNTS)
        maxs = dict(_PG_TIMING_MAX)
        lifecycle_counters = dict(_PG_LIFECYCLE_COUNTERS)
        test_timings = {k: dict(v) for k, v in _PG_TEARDOWN_TEST_TIMINGS.items()}

    _, strategy_name = get_postgres_clone_strategy()
    run_dir = os.environ.get("SYNAPSE_TIMINGS_RUN_DIR")
    if run_dir:
        tmp_path = os.path.join(run_dir, f"lifecycle_{os.getpid()}.tmp")
        final_path = os.path.join(run_dir, f"lifecycle_{os.getpid()}.json")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "timings": timings,
                        "counts": counts,
                        "maxs": maxs,
                        "counters": lifecycle_counters,
                        "test_timings": test_timings,
                        "strategy": strategy_name,
                    },
                    f,
                )
            os.replace(tmp_path, final_path)
        except OSError:
            pass
        return

    _timings_print(
        f"\n=== Postgres test-DB lifecycle timings (strategy: {strategy_name}) ==="
    )
    _timings_print("")
    _timings_print(
        f"  {'':44s}  {'total':>10s}  {'calls':>6s}  {'avg':>12s}  {'max':>12s}",
    )
    if "hs_setup_wall" in timings:
        wall_s = timings["hs_setup_wall"]
        wall_cnt = counts["hs_setup_wall"]
        wall_max = maxs.get("hs_setup_wall", 0.0)
        _timings_print(
            f"  {'hs_setup_wall (outer)':44s}  {wall_s * 1000:8.1f}ms  {wall_cnt:6d}  {(wall_s / wall_cnt) * 1000:10.3f}ms  {wall_max * 1000:10.3f}ms"
        )
        if "create_database" in timings:
            cd_s = timings["create_database"]
            cd_cnt = counts["create_database"]
            cd_max = maxs.get("create_database", 0.0)
            _timings_print(
                f"    ├── {'create_database':38s}  {cd_s * 1000:8.1f}ms  {cd_cnt:6d}  {(cd_s / cd_cnt) * 1000:10.3f}ms  {cd_max * 1000:10.3f}ms"
            )
        if "hs_setup_total" in timings:
            st_s = timings["hs_setup_total"]
            st_cnt = counts["hs_setup_total"]
            st_max = maxs.get("hs_setup_total", 0.0)
            _timings_print(
                f"    ├── {'hs_setup_total':38s}  {st_s * 1000:8.1f}ms  {st_cnt:6d}  {(st_s / st_cnt) * 1000:10.3f}ms  {st_max * 1000:10.3f}ms"
            )
            # Tags emitted by Databases.__init__ for each per-test homeserver.
            # Shown in call order so the tree matches the actual execution path.
            _STORE_INIT_TAGS: tuple[str, ...] = (
                "create_engine",
                "make_conn",
                "check_database",
                "prepare_database",
                "database_pool_init",
                "main_store_init",
                "persist_events_store_init",
                "state_deletion_store_init",
                "state_store_init",
                "databases_init_commit",
            )
            sub_inner = 0.0
            for inner_tag in _STORE_INIT_TAGS:
                if inner_tag in timings:
                    it_s = timings[inner_tag]
                    it_cnt = counts[inner_tag]
                    it_max = maxs.get(inner_tag, 0.0)
                    sub_inner += it_s
                    _timings_print(
                        f"    │     ├── {inner_tag:32s}  {it_s * 1000:8.1f}ms  {it_cnt:6d}  {(it_s / it_cnt) * 1000:10.3f}ms  {it_max * 1000:10.3f}ms"
                    )
            store_res = max(0.0, st_s - sub_inner)
            _timings_print(
                f"    │     └── {'store_init (unattributed)':32s}  {store_res * 1000:8.1f}ms  {st_cnt:6d}  {(store_res / st_cnt) * 1000:10.3f}ms"
            )
        sub_wall = timings.get("create_database", 0.0) + timings.get(
            "hs_setup_total", 0.0
        )
        unatt_wall = max(0.0, wall_s - sub_wall)
        _timings_print(
            f"    └── {'hs_unattributed':38s}  {unatt_wall * 1000:8.1f}ms  {wall_cnt:6d}  {(unatt_wall / wall_cnt) * 1000:10.3f}ms"
        )

        # ── Teardown phase breakdown ─────────────────────────────────────────
        teardown_tags = (
            "hs_shutdown",
            "db_drop_total",
            "db_drop_connect",
            "db_drop_terminate_backends",
            "db_drop_statement",
            "db_drop_retry_sleep",
        )
        has_teardown = any(t in timings for t in teardown_tags)
        teardown_total_s = 0.0

        if has_teardown:
            _timings_print("\n=== Teardown Phase Timings ===")
            _timings_print("")
            _timings_print(
                f"  {'':44s}  {'total':>10s}  {'calls':>6s}  {'avg':>12s}  {'max':>12s}",
            )
            if "hs_shutdown" in timings:
                sd_s = timings["hs_shutdown"]
                sd_cnt = counts["hs_shutdown"]
                sd_max = maxs.get("hs_shutdown", 0.0)
                teardown_total_s += sd_s
                _timings_print(
                    f"  {'hs_shutdown (async)':44s}  {sd_s * 1000:8.1f}ms  {sd_cnt:6d}  {(sd_s / sd_cnt) * 1000:10.3f}ms  {sd_max * 1000:10.3f}ms"
                )
            if "db_drop_total" in timings:
                dd_s = timings["db_drop_total"]
                dd_cnt = counts["db_drop_total"]
                dd_max = maxs.get("db_drop_total", 0.0)
                teardown_total_s += dd_s
                _timings_print(
                    f"  {'db_drop_total':44s}  {dd_s * 1000:8.1f}ms  {dd_cnt:6d}  {(dd_s / dd_cnt) * 1000:10.3f}ms  {dd_max * 1000:10.3f}ms"
                )
                if "db_drop_connect" in timings:
                    dbc_s = timings["db_drop_connect"]
                    dbc_cnt = counts["db_drop_connect"]
                    dbc_max = maxs.get("db_drop_connect", 0.0)
                    _timings_print(
                        f"    ├── {'db_drop_connect':38s}  {dbc_s * 1000:8.1f}ms  {dbc_cnt:6d}  {(dbc_s / dbc_cnt) * 1000:10.3f}ms  {dbc_max * 1000:10.3f}ms"
                    )
                if "db_drop_terminate_backends" in timings:
                    dbt_s = timings["db_drop_terminate_backends"]
                    dbt_cnt = counts["db_drop_terminate_backends"]
                    dbt_max = maxs.get("db_drop_terminate_backends", 0.0)
                    _timings_print(
                        f"    ├── {'db_drop_terminate_backends':38s}  {dbt_s * 1000:8.1f}ms  {dbt_cnt:6d}  {(dbt_s / dbt_cnt) * 1000:10.3f}ms  {dbt_max * 1000:10.3f}ms"
                    )
                if "db_drop_statement" in timings:
                    dbs_s = timings["db_drop_statement"]
                    dbs_cnt = counts["db_drop_statement"]
                    dbs_max = maxs.get("db_drop_statement", 0.0)
                    _timings_print(
                        f"    ├── {'db_drop_statement':38s}  {dbs_s * 1000:8.1f}ms  {dbs_cnt:6d}  {(dbs_s / dbs_cnt) * 1000:10.3f}ms  {dbs_max * 1000:10.3f}ms"
                    )
                if "db_drop_retry_sleep" in timings:
                    dbr_s = timings["db_drop_retry_sleep"]
                    dbr_cnt = counts["db_drop_retry_sleep"]
                    dbr_max = maxs.get("db_drop_retry_sleep", 0.0)
                    _timings_print(
                        f"    └── {'db_drop_retry_sleep':38s}  {dbr_s * 1000:8.1f}ms  {dbr_cnt:6d}  {(dbr_s / dbr_cnt) * 1000:10.3f}ms  {dbr_max * 1000:10.3f}ms"
                    )

            _timings_print("")
            _timings_print(
                f"  {'TOTAL TEARDOWN WALL TIME':44s}  {teardown_total_s * 1000:8.1f}ms"
            )

        known = {
            "hs_setup_wall",
            "create_database",
            "hs_setup_total",
            "make_conn",
            "check_database",
            "prepare_database",
            "create_engine",
            "database_pool_init",
            "main_store_init",
            "persist_events_store_init",
            "state_deletion_store_init",
            "state_store_init",
            "databases_init_commit",
            "hs_shutdown",
            "db_drop_total",
            "db_drop_connect",
            "db_drop_terminate_backends",
            "db_drop_statement",
            "db_drop_retry_sleep",
        }
        for tag in sorted(timings):
            if tag not in known:
                t_s = timings[tag]
                cnt = counts[tag]
                m_s = maxs.get(tag, 0.0)
                _timings_print(
                    f"  {tag:44s}  {t_s * 1000:8.1f}ms  {cnt:6d}  {(t_s / cnt) * 1000:10.3f}ms  {m_s * 1000:10.3f}ms"
                )
        non_overlap = wall_s + teardown_total_s
        _timings_print("")
        _timings_print(
            f"  {'TOTAL NON-OVERLAPPING LIFECYCLE':44s}  {non_overlap * 1000:8.1f}ms"
        )

        # ── Slowest tests in teardown ────────────────────────────────────────
        if test_timings:
            slowest: list[tuple[float, str, dict[str, float]]] = []
            for tname, ptimings in test_timings.items():
                tot = ptimings.get("hs_shutdown", 0.0) + ptimings.get(
                    "db_drop_total", 0.0
                )
                if tot > 0:
                    slowest.append((tot, tname, ptimings))
            slowest.sort(key=lambda item: item[0], reverse=True)
            if slowest:
                _timings_print("\n=== Slowest Tests in Teardown (Top 10) ===")
                for rank, (tot_s, tname, ptimings) in enumerate(slowest[:10], 1):
                    details = []
                    for ptag in (
                        "hs_shutdown",
                        "db_drop_connect",
                        "db_drop_terminate_backends",
                        "db_drop_statement",
                        "db_drop_retry_sleep",
                    ):
                        if ptag in ptimings and ptimings[ptag] > 0:
                            details.append(f"{ptag}={ptimings[ptag] * 1000:.1f}ms")
                    detail_str = f" ({', '.join(details)})" if details else ""
                    _timings_print(f"  {rank:2d}. {tname}")
                    _timings_print(f"      total: {tot_s * 1000:8.1f}ms{detail_str}")
    else:
        for tag in sorted(timings):
            total_s = timings[tag]
            count = counts[tag]
            total_ms = total_s * 1000
            avg_ms = (total_s / count) * 1000 if count else 0.0
            max_ms = maxs.get(tag, 0.0) * 1000
            _timings_print(
                f"  {tag:44s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms  {max_ms:10.3f}ms",
            )
        total_s = sum(timings.values())
        total_ms = total_s * 1000
        _timings_print("")
        _timings_print(
            f"  {'TOTAL':44s}  {total_ms:8.1f}ms",
        )
    _timings_print("==========================================")
    _timings_print("")


def flush_pg_timings() -> None:
    _print_pg_timings()


atexit.register(_drain_db_drop_queue)
atexit.register(flush_pg_timings)

if os.environ.get("SYNAPSE_PG_TIMINGS"):
    import signal as _signal
    from types import FrameType as _FrameType

    _original_sigterm_pg_timings = _signal.getsignal(_signal.SIGTERM)

    def _flush_pg_timings_on_sigterm(signum: int, frame: _FrameType | None) -> None:
        _print_pg_timings()
        if callable(_original_sigterm_pg_timings):
            _original_sigterm_pg_timings(signum, frame)
        elif _original_sigterm_pg_timings == _signal.SIG_DFL:
            _signal.signal(_signal.SIGTERM, _signal.SIG_DFL)
            _signal.raise_signal(_signal.SIGTERM)

    _signal.signal(_signal.SIGTERM, _flush_pg_timings_on_sigterm)


class TimedOutException(Exception):
    """
    A web query timed out.
    """


@implementer(ITransport, IPushProducer, IConsumer)
@attr.s(auto_attribs=True)
class FakeChannel:
    """
    A fake Twisted Web Channel (the part that interfaces with the
    wire).

    See twisted.web.http.HTTPChannel.
    """

    site: Union[Site, "FakeSite"]
    _reactor: MemoryReactorClock
    result: dict = attr.Factory(dict)
    _ip: str = "127.0.0.1"
    _producer: Optional[Union[IPullProducer, IPushProducer]] = None
    resource_usage: ContextResourceUsage | None = None
    _request: Request | None = None

    @property
    def request(self) -> Request:
        assert self._request is not None
        return self._request

    @request.setter
    def request(self, request: Request) -> None:
        assert self._request is None
        self._request = request

    @property
    def json_body(self) -> JsonDict:
        body = json.loads(self.text_body)
        assert isinstance(body, dict)
        return body

    @property
    def json_list(self) -> list[JsonDict]:
        body = json.loads(self.text_body)
        assert isinstance(body, list)
        return body

    @property
    def text_body(self) -> str:
        """The body of the result, utf-8-decoded.

        Raises an exception if the request has not yet completed.
        """
        if not self.is_finished():
            raise Exception("Request not yet completed")
        return self.result["body"].decode("utf8")

    def is_finished(self) -> bool:
        """check if the response has been completely received"""
        return self.result.get("done", False)

    @property
    def code(self) -> int:
        if not self.result:
            raise Exception("No result yet.")
        return int(self.result["code"])

    @property
    def headers(self) -> Headers:
        if not self.result:
            raise Exception("No result yet.")

        h = self.result["headers"]
        assert isinstance(h, Headers)
        return h

    def writeHeaders(
        self,
        version: bytes,
        code: bytes,
        reason: bytes,
        headers: Headers | list[tuple[bytes, bytes]],
    ) -> None:
        self.result["version"] = version
        self.result["code"] = code
        self.result["reason"] = reason

        if isinstance(headers, list):
            # Support prior to Twisted 24.7.0rc1
            new_headers = Headers()
            for k, v in headers:
                assert isinstance(k, bytes), f"key is not of type bytes: {k!r}"
                assert isinstance(v, bytes), f"value is not of type bytes: {v!r}"
                new_headers.addRawHeader(k, v)
            headers = new_headers

        assert isinstance(headers, Headers), (
            f"headers are of the wrong type: {headers!r}"
        )

        self.result["headers"] = headers

    def write(self, data: bytes) -> None:
        assert isinstance(data, bytes), "Should be bytes! " + repr(data)

        if "body" not in self.result:
            self.result["body"] = b""

        self.result["body"] += data

    def writeSequence(self, data: Iterable[bytes]) -> None:
        for x in data:
            self.write(x)

    def loseConnection(self) -> None:
        self.unregisterProducer()

    # Type ignore: mypy doesn't like the fact that producer isn't an IProducer.
    def registerProducer(self, producer: IProducer, streaming: bool) -> None:
        # TODO This should ensure that the IProducer is an IPushProducer or
        # IPullProducer, unfortunately twisted.protocols.basic.FileSender does
        # implement those, but doesn't declare it.
        self._producer = cast(Union[IPushProducer, IPullProducer], producer)
        self.producerStreaming = streaming

        def _produce() -> None:
            if self._producer:
                self._producer.resumeProducing()
                self._reactor.callLater(0.0, _produce)

        if not streaming:
            self._reactor.callLater(0.0, _produce)

    def unregisterProducer(self) -> None:
        if self._producer is None:
            return

        self._producer = None

    def stopProducing(self) -> None:
        if self._producer is not None:
            self._producer.stopProducing()

    def pauseProducing(self) -> None:
        raise NotImplementedError()

    def resumeProducing(self) -> None:
        raise NotImplementedError()

    def requestDone(self, _self: Request) -> None:
        self.result["done"] = True
        if isinstance(_self, SynapseRequest):
            assert _self.logcontext is not None
            self.resource_usage = _self.logcontext.get_resource_usage()

    def getPeer(self) -> IAddress:
        # We give an address so that getClientAddress/getClientIP returns a non null entry,
        # causing us to record the MAU
        return address.IPv4Address("TCP", self._ip, 3423)

    def getHost(self) -> IAddress:
        # this is called by Request.__init__ to configure Request.host.
        return address.IPv4Address("TCP", "127.0.0.1", 8888)

    def isSecure(self) -> bool:
        return False

    @property
    def transport(self) -> "FakeChannel":
        return self

    def await_result(self, timeout_ms: int = 1000) -> None:
        """
        Wait until the request is finished.

        Advances the Twisted reactor clock by 0.1s and suspending execution of the
        Python thread (to allow other threads to do work) in a loop until we see a
        result. We timeout when both the Twisted reactor clock has been advanced enough
        AND we've done at-least 100 iterations (round-trips for other threads to get
        work done).

        The loop 1) allows `clock.call_later` scheduled callbacks to run if they are
        scheduled to run now and 2) will also allow other threads to make progress. This
        could be things spawned on the Twisted reactor threadpool or Tokio runtime
        (async Rust code).

        Args:
            timeout_ms: The Twisted reactor time we wait until we raise a `TimedOutException`
        """
        timeout = Duration(milliseconds=timeout_ms)

        # TODO: Why?
        self._reactor.run()

        # First, run anything that's scheduled now before we start looping and advancing
        # non-zero time increments.
        #
        # Without this, if some request handler had some database queries followed by
        # `self.hs.get_clock().sleep(Duration(seconds=1))`, and called
        # `channel.await_result(timeout_ms=1000)`, it wouldn't be called because the
        # first `self._reactor.advance(0.1)` would be first spent driving the database
        # queries, and only leaving 0.9s remaining (0.1s shy of the sleep finishing) so
        # the request would timeout.
        #
        # The goal is to remove the foot-guns and having to think about this for the
        # standard cases.
        #
        # FIXME: Ideally, we'd advance by `0` but there is a handful of tests that
        # assume that time advances in between requests and many requests complete from
        # a single advance. Second best, we'd just advance by minuscule amount of time
        # (`CLOCK_SCHEDULE_EPSILON`) but some tests assume at-least a millisecond in
        # between as our timestamps are often recorded at the millisecond granularity
        # (`origin_server_ts`, etc). It's a balance between test convenience of this
        # helper and materializing test expectations so we may never fix this.
        self._reactor.advance(Duration(milliseconds=1).as_secs())

        # We only count the looping time (record the start after we advance once above)
        start_time_seconds = self._reactor.seconds()
        loop_count = 0
        while not self.is_finished():
            if (
                # Exceeded the Twisted reactor time timeout
                #
                # We use `>=` for the reactor time condition as it's possible we advance
                # exactly the `timeout` amount and we don't want to get stuck in an
                # infinite loop
                self._reactor.seconds() >= start_time_seconds + timeout.as_secs()
                # 100 loops is arbitrary. This also makes the assumption that any work
                # on other threads will finish before we give up after sleeping ~0.1s of
                # real-time (100 * 0.001).
                and loop_count > 100
            ):
                raise TimedOutException("Timed out waiting for request to finish.")

            # Suspend execution of this thread to allow other threads to do work. This
            # could be things spawned on the Twisted reactor threadpool or Tokio thread
            # pool (async Rust code).
            #
            # Note: Python has a default thread switch interval (5ms for cpython) (see
            # `sys.setswitchinterval(interval)`) but we still want this here as we're
            # able to preempt and cause the thread context switch to happen faster.
            # Also, without any real-time sleeping, this function would complete before
            # the 5ms switch ever happened.
            #
            # After a few cycles, we use `time.sleep(0.001)` instead of `time.sleep(0)`
            # to avoid tightlooping on the main thread (CPU 100%) because it's wasteful
            # and may starve out other threads. 10 is arbitrary but many cases will have
            # none or only a few round-trips so we can just try to go as fast as
            # possible.
            if loop_count < 10:
                time.sleep(0)
            else:
                time.sleep(0.001)

            # Advance the Twisted reactor and run any scheduled callbacks
            #
            # Don't advance the Twisted reactor clock further than the timeout duration
            # as someone should increase the timeout if they expect things to take
            # longer.
            if self._reactor.seconds() < start_time_seconds + timeout.as_secs():
                self._reactor.advance(0.1)
            else:
                # But we want to still keep running whatever might be getting scheduled
                # to run now.
                #
                # For example from other threads, they may have scheduled something on
                # the reactor to run (like `reactor.callFromThread(...)`)
                self._reactor.advance(0)

            loop_count += 1

    def extract_cookies(self, cookies: MutableMapping[str, str]) -> None:
        """Process the contents of any Set-Cookie headers in the response

        Any cookines found are added to the given dict
        """
        headers = self.headers.getRawHeaders("Set-Cookie")
        if not headers:
            return

        for h in headers:
            parts = h.split(";")
            k, v = parts[0].split("=", maxsplit=1)
            cookies[k] = v


class FakeSite:
    """
    A fake Twisted Web Site, with mocks of the extra things that
    Synapse adds.
    """

    server_version_string = b"1"
    site_tag = "test"
    access_logger = logging.getLogger("synapse.access.http.fake")

    def __init__(
        self,
        resource: IResource,
        reactor: IReactorTime,
        *,
        parsePOSTFormSubmission: bool = True,
    ):
        """

        Args:
            resource: the resource to be used for rendering all requests
        """
        self._resource = resource
        self.reactor = reactor
        self._parsePOSTFormSubmission = parsePOSTFormSubmission

    def getResourceFor(self, request: Request) -> IResource:
        return self._resource


def make_request(
    reactor: MemoryReactorClock,
    site: Site | FakeSite,
    method: bytes | str,
    path: bytes | str,
    content: bytes | str | JsonDict = b"",
    access_token: str | None = None,
    request: type[Request] = SynapseRequest,
    shorthand: bool = True,
    federation_auth_origin: bytes | None = None,
    content_type: bytes | None = None,
    content_is_form: bool = False,
    await_result: bool = True,
    custom_headers: Iterable[CustomHeaderType] | None = None,
    client_ip: str = "127.0.0.1",
) -> FakeChannel:
    """
    Make a web request using the given method, path and content, and render it

    Returns the fake Channel object which records the response to the request.

    Args:
        reactor:
        site: The twisted Site to use to render the request
        method: The HTTP request method ("verb").
        path: The HTTP path, suitably URL encoded (e.g. escaped UTF-8 & spaces and such).
        content: The body of the request. JSON-encoded, if a str of bytes.
        access_token: The access token to add as authorization for the request.
        request: The request class to create.
        shorthand: Whether to try and be helpful and prefix the given URL
            with the usual REST API path, if it doesn't contain it.
        federation_auth_origin: if set to not-None, we will add a fake
            Authorization header pretenting to be the given server name.
        content_type: The content-type to use for the request. If not set then will default to
            application/json unless content_is_form is true.
        content_is_form: Whether the content is URL encoded form data. Adds the
            'Content-Type': 'application/x-www-form-urlencoded' header.
        await_result: whether to wait for the request to complete rendering. If true,
             will pump the reactor until the the renderer tells the channel the request
             is finished.
        custom_headers: (name, value) pairs to add as request headers
        client_ip: The IP to use as the requesting IP. Useful for testing
            ratelimiting.

    Returns:
        channel
    """
    if not isinstance(method, bytes):
        method = method.encode("ascii")

    if not isinstance(path, bytes):
        path = path.encode("ascii")

    # Decorate it to be the full path, if we're using shorthand
    if (
        shorthand
        and not path.startswith(b"/_matrix")
        and not path.startswith(b"/_synapse")
    ):
        if path.startswith(b"/"):
            path = path[1:]
        path = b"/_matrix/client/r0/" + path

    if not path.startswith(b"/"):
        path = b"/" + path

    if isinstance(content, dict):
        content = json_encoder.encode(content).encode("utf8")
    if isinstance(content, str):
        content = content.encode("utf8")

    channel = FakeChannel(site, reactor, ip=client_ip)

    req = request(
        channel,
        site,
        our_server_name="test_server",
        max_request_body_size=MAX_REQUEST_SIZE,
    )
    channel.request = req

    req.content = BytesIO(content)
    # Twisted expects to be at the end of the content when parsing the request.
    req.content.seek(0, SEEK_END)

    # If `Content-Length` was passed in as a custom header, don't automatically add it
    # here.
    if custom_headers is None or not any(
        (k if isinstance(k, bytes) else k.encode("ascii")) == b"Content-Length"
        for k, _ in custom_headers
    ):
        # Old version of Twisted (<20.3.0) have issues with parsing x-www-form-urlencoded
        # bodies if the Content-Length header is missing
        req.requestHeaders.addRawHeader(
            b"Content-Length", str(len(content)).encode("ascii")
        )

    if access_token:
        req.requestHeaders.addRawHeader(
            b"Authorization", b"Bearer " + access_token.encode("ascii")
        )

    if federation_auth_origin is not None:
        req.requestHeaders.addRawHeader(
            b"Authorization",
            b"X-Matrix origin=%s,key=,sig=" % (federation_auth_origin,),
        )

    if content:
        if content_type is not None:
            req.requestHeaders.addRawHeader(b"Content-Type", content_type)
        elif content_is_form:
            req.requestHeaders.addRawHeader(
                b"Content-Type", b"application/x-www-form-urlencoded"
            )
        else:
            # Assume the body is JSON
            req.requestHeaders.addRawHeader(b"Content-Type", b"application/json")

    if custom_headers:
        for k, v in custom_headers:
            req.requestHeaders.addRawHeader(k, v)

    req.parseCookies()
    req.requestReceived(method, path, b"1.1")

    if await_result:
        channel.await_result()

    return channel


# ISynapseReactor implies IReactorPluggableNameResolver, but explicitly
# marking this as an implementer of the latter seems to keep mypy-zope happier.
@implementer(IReactorPluggableNameResolver, ISynapseReactor)
class ThreadedMemoryReactorClock(MemoryReactorClock):
    """
    A MemoryReactorClock that supports callFromThread.
    """

    def __init__(self) -> None:
        self.threadpool = ThreadPool(self)

        self._tcp_callbacks: dict[tuple[str, int], Callable] = {}
        self._udp: list[udp.Port] = []
        self.lookups: dict[str, str] = {}
        self._thread_callbacks: deque[Callable[..., R]] = deque()

        lookups = self.lookups

        @implementer(IResolverSimple)
        class FakeResolver:
            def getHostByName(
                self, name: str, timeout: Sequence[int] | None = None
            ) -> "Deferred[str]":
                if name not in lookups:
                    return fail(DNSLookupError("OH NO: unknown %s" % (name,)))
                return succeed(lookups[name])

        # In order for the TLS protocol tests to work, modify _get_default_clock
        # on newer Twisted versions to use the test reactor's clock.
        #
        # This is *super* dirty since it is never undone and relies on the next
        # test to overwrite it.
        if twisted.version > Version("Twisted", 23, 8, 0):
            from twisted.protocols import tls

            tls._get_default_clock = lambda: self

        super().__init__()

        # Override the default name resolver with our fake resolver. This must
        # happen after `super().__init__()` so that the base class doesn't
        # overwrite it again.
        self.nameResolver = SimpleResolverComplexifier(FakeResolver())

    def run(self) -> None:
        """
        Override the call from `MemoryReactorClock` to add an additional step that
        cleans up any `whenRunningHooks` that have been called.
        This is necessary for a clean shutdown to occur as these hooks can hold
        references to the `SynapseHomeServer`.
        """
        super().run()

        # `MemoryReactorClock` never clears the hooks that have already been called.
        # So manually clear the hooks here after they have been run.
        self.whenRunningHooks.clear()

    def installNameResolver(self, resolver: IHostnameResolver) -> IHostnameResolver:
        raise NotImplementedError()

    def listenUDP(
        self,
        port: int,
        protocol: DatagramProtocol,
        interface: str = "",
        maxPacketSize: int = 8196,
    ) -> udp.Port:
        p = udp.Port(port, protocol, interface, maxPacketSize, self)
        p.startListening()
        self._udp.append(p)
        return p

    def callFromThread(
        self, callable: Callable[..., Any], *args: object, **kwargs: object
    ) -> None:
        """
        Make the callback fire in the next reactor iteration.
        """
        cb = lambda: callable(*args, **kwargs)
        # it's not safe to call callLater() here, so we append the callback to a
        # separate queue.
        self._thread_callbacks.append(cb)

    def callInThread(
        self, callable: Callable[..., Any], *args: object, **kwargs: object
    ) -> None:
        raise NotImplementedError()

    def suggestThreadPoolSize(self, size: int) -> None:
        raise NotImplementedError()

    def getThreadPool(self) -> "threadpool.ThreadPool":
        # Cast to match super-class.
        return cast(threadpool.ThreadPool, self.threadpool)

    def add_tcp_client_callback(
        self, host: str, port: int, callback: Callable[[], None]
    ) -> None:
        """Add a callback that will be invoked when we receive a connection
        attempt to the given IP/port using `connectTCP`.

        Note that the callback gets run before we return the connection to the
        client, which means callbacks cannot block while waiting for writes.
        """
        self._tcp_callbacks[(host, port)] = callback

    def connectUNIX(
        self,
        address: str,
        factory: ClientFactory,
        timeout: float = 30,
        checkPID: int = 0,
    ) -> IConnector:
        """
        Unix sockets aren't supported for unit tests yet. Make it obvious to any
        developer trying it out that they will need to do some work before being able
        to use it in tests.
        """
        raise Exception("Unix sockets are not implemented for tests yet, sorry.")

    def listenUNIX(
        self,
        address: str,
        factory: Factory,
        backlog: int = 50,
        mode: int = 0o666,
        wantPID: int = 0,
    ) -> IListeningPort:
        """
        Unix sockets aren't supported for unit tests yet. Make it obvious to any
        developer trying it out that they will need to do some work before being able
        to use it in tests.
        """
        raise Exception("Unix sockets are not implemented for tests, sorry")

    def connectTCP(
        self,
        host: str,
        port: int,
        factory: ClientFactory,
        timeout: float = 30,
        bindAddress: tuple[str, int] | None = None,
    ) -> IConnector:
        """Fake L{IReactorTCP.connectTCP}."""

        conn = super().connectTCP(
            host, port, factory, timeout=timeout, bindAddress=None
        )
        if self.lookups and host in self.lookups:
            validate_connector(conn, self.lookups[host])

        callback = self._tcp_callbacks.get((host, port))
        if callback:
            callback()

        return conn

    def advance(self, amount: float) -> None:
        # first advance our reactor's time, and run any "callLater" callbacks that
        # makes ready
        super().advance(amount)

        # now run any "callFromThread" callbacks
        while True:
            try:
                callback = self._thread_callbacks.popleft()
            except IndexError:
                break
            callback()

            # check for more "callLater" callbacks added by the thread callback
            # This isn't required in a regular reactor, but it ends up meaning that
            # our database queries can complete in a single call to `advance` [1] which
            # simplifies tests.
            #
            # [1]: we replace the threadpool backing the db connection pool with a
            # mock ThreadPool which doesn't really use threads; but we still use
            # reactor.callFromThread to feed results back from the db functions to the
            # main thread.
            super().advance(0)


def cleanup_test_reactor_system_event_triggers(
    reactor: ThreadedMemoryReactorClock,
) -> None:
    """Cleanup any registered system event triggers.
    The `twisted.internet.test.ThreadedMemoryReactor` does not implement
    `removeSystemEventTrigger` so won't clean these triggers up on it's own properly.
    When trying to override `removeSystemEventTrigger` in `ThreadedMemoryReactorClock`
    in order to implement this functionality, twisted complains about the reactor being
    unclean and fails some tests.
    """
    reactor.triggers.clear()


def validate_connector(connector: tcp.Connector, expected_ip: str) -> None:
    """Try to validate the obtained connector as it would happen when
    synapse is running and the conection will be established.

    This method will raise a useful exception when necessary, else it will
    just do nothing.

    This is in order to help catch quirks related to reactor.connectTCP,
    since when called directly, the connector's destination will be of type
    IPv4Address, with the hostname as the literal host that was given (which
    could be an IPv6-only host or an IPv6 literal).

    But when called from reactor.connectTCP *through* e.g. an Endpoint, the
    connector's destination will contain the specific IP address with the
    correct network stack class.

    Note that testing code paths that use connectTCP directly should not be
    affected by this check, unless they specifically add a test with a
    matching reactor.lookups[HOSTNAME] = "IPv6Literal", where reactor is of
    type ThreadedMemoryReactorClock.
    For an example of implementing such tests, see test/handlers/send_email.py.
    """
    destination = connector.getDestination()

    # We use address.IPv{4,6}Address to check what the reactor thinks it is
    # is sending but check for validity with ipaddress.IPv{4,6}Address
    # because they fail with IPs on the wrong network stack.
    cls_mapping = {
        address.IPv4Address: ipaddress.IPv4Address,
        address.IPv6Address: ipaddress.IPv6Address,
    }

    cls = cls_mapping.get(destination.__class__)

    if cls is not None:
        try:
            cls(expected_ip)
        except Exception as exc:
            raise ValueError(
                "Invalid IP type and resolution for %s. Expected %s to be %s"
                % (destination, expected_ip, cls.__name__)
            ) from exc
    else:
        raise ValueError(
            "Unknown address type %s for %s"
            % (destination.__class__.__name__, destination)
        )


def make_fake_db_pool(
    reactor: ISynapseReactor,
    db_config: DatabaseConnectionConfig,
    engine: BaseDatabaseEngine,
    server_name: str,
) -> adbapi.ConnectionPool:
    """Wrapper for `make_pool` which builds a pool which runs db queries synchronously.

    For more deterministic testing, we don't use a regular db connection pool: instead
    we run all db queries synchronously on the test reactor's main thread. This function
    is a drop-in replacement for the normal `make_pool` which builds such a connection
    pool.
    """
    pool = make_pool(
        reactor=reactor, db_config=db_config, engine=engine, server_name=server_name
    )

    def runWithConnection(
        func: Callable[..., R], *args: Any, **kwargs: Any
    ) -> Awaitable[R]:
        return threads.deferToThreadPool(
            pool._reactor,
            pool.threadpool,
            pool._runWithConnection,
            func,
            *args,
            **kwargs,
        )

    def runInteraction(
        desc: str, func: Callable[..., R], *args: Any, **kwargs: Any
    ) -> Awaitable[R]:
        return threads.deferToThreadPool(
            pool._reactor,
            pool.threadpool,
            pool._runInteraction,
            desc,
            func,
            *args,
            **kwargs,
        )

    pool.runWithConnection = runWithConnection  # type: ignore[method-assign]
    pool.runInteraction = runInteraction  # type: ignore[assignment]
    # Replace the thread pool with a threadless 'thread' pool
    pool.threadpool = ThreadPool(reactor)
    pool.running = True
    return pool


class ThreadPool:
    """
    Threadless thread pool.

    See twisted.python.threadpool.ThreadPool
    """

    def __init__(self, reactor: IReactorTime):
        self._reactor = reactor

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def callInThreadWithCallback(
        self,
        onResult: Callable[[bool, Failure | R], None],
        function: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "Deferred[None]":
        def run() -> None:
            try:
                result = function(*args, **kwargs)
            except (KeyboardInterrupt, SystemExit):
                # This test pool runs its "worker" on the main thread. Do not
                # route process-control exceptions through a Deferred: Twisted
                # converts them to a test failure, which makes Ctrl-C unable to
                # stop a PostgreSQL test run.
                raise
            except Exception:
                onResult(False, Failure())
            else:
                onResult(True, result)

        # mypy ignored here because:
        #   - this is part of the test infrastructure (outside of Synapse) so tracking
        #     these calls for for homeserver shutdown doesn't make sense.
        self._reactor.callLater(0, run)  # type: ignore[call-later-not-tracked]
        return succeed(None)


def get_clock() -> tuple[ThreadedMemoryReactorClock, Clock]:
    # Ignore the linter error since this is an expected usage of creating a `Clock` for
    # testing purposes.
    reactor = ThreadedMemoryReactorClock()
    hs_clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
    return reactor, hs_clock


@implementer(ITCPTransport)
@attr.s(cmp=False, auto_attribs=True)
class FakeTransport:
    """
    A twisted.internet.interfaces.ITransport implementation which sends all its data
    straight into an IProtocol object: it exists to connect two IProtocols together.

    To use it, instantiate it with the receiving IProtocol, and then pass it to the
    sending IProtocol's makeConnection method:

        server = HTTPChannel()
        client.makeConnection(FakeTransport(server, self.reactor))

    If you want bidirectional communication, you'll need two instances.
    """

    other: IProtocol
    """The Protocol object which will receive any data written to this transport.
    """

    _reactor: IReactorTime
    """Test reactor
    """

    _protocol: Optional[IProtocol] = None
    """The Protocol which is producing data for this transport. Optional, but if set
    will get called back for connectionLost() notifications etc.
    """

    _peer_address: IPv4Address | IPv6Address = attr.Factory(
        lambda: address.IPv4Address("TCP", "127.0.0.1", 5678)
    )
    """The value to be returned by getPeer"""

    _host_address: IPv4Address | IPv6Address = attr.Factory(
        lambda: address.IPv4Address("TCP", "127.0.0.1", 1234)
    )
    """The value to be returned by getHost"""

    disconnecting = False
    disconnected = False
    connected = True
    buffer: bytes = b""
    producer: Optional[IPushProducer] = None
    autoflush: bool = True

    def getPeer(self) -> IPv4Address | IPv6Address:
        return self._peer_address

    def getHost(self) -> IPv4Address | IPv6Address:
        return self._host_address

    def loseConnection(self) -> None:
        if not self.disconnecting:
            logger.info("FakeTransport: loseConnection()")
            self.disconnecting = True
            if self._protocol:
                self._protocol.connectionLost(
                    Failure(RuntimeError("FakeTransport.loseConnection()"))
                )

            # if we still have data to write, delay until that is done
            if self.buffer:
                logger.info(
                    "FakeTransport: Delaying disconnect until buffer is flushed"
                )
            else:
                self.connected = False
                self.disconnected = True

    def abortConnection(self) -> None:
        logger.info("FakeTransport: abortConnection()")

        if not self.disconnecting:
            self.disconnecting = True
            if self._protocol:
                self._protocol.connectionLost(None)  # type: ignore[arg-type]

        self.disconnected = True

    def pauseProducing(self) -> None:
        if not self.producer:
            return

        self.producer.pauseProducing()

    def resumeProducing(self) -> None:
        if not self.producer:
            return
        self.producer.resumeProducing()

    def unregisterProducer(self) -> None:
        if not self.producer:
            return

        self.producer = None

    def registerProducer(self, producer: IPushProducer, streaming: bool) -> None:
        self.producer = producer
        self.producerStreaming = streaming

        def _produce() -> None:
            if not self.producer:
                # we've been unregistered
                return
            # some implementations of IProducer (for example, FileSender)
            # don't return a deferred.
            d = maybeDeferred(self.producer.resumeProducing)
            # mypy ignored here because:
            #   - this is part of the test infrastructure (outside of Synapse) so tracking
            #     these calls for for homeserver shutdown doesn't make sense.
            d.addCallback(lambda x: self._reactor.callLater(0.0, _produce))  # type: ignore[call-later-not-tracked,call-overload]

        if not streaming:
            # mypy ignored here because:
            #   - this is part of the test infrastructure (outside of Synapse) so tracking
            #     these calls for for homeserver shutdown doesn't make sense.
            self._reactor.callLater(0.0, _produce)  # type: ignore[call-later-not-tracked]

    def write(self, byt: bytes) -> None:
        if self.disconnecting:
            raise Exception("Writing to disconnecting FakeTransport")

        self.buffer = self.buffer + byt

        # always actually do the write asynchronously. Some protocols (notably the
        # TLSMemoryBIOProtocol) get very confused if a read comes back while they are
        # still doing a write. Doing a callLater here breaks the cycle.
        if self.autoflush:
            # mypy ignored here because:
            #   - this is part of the test infrastructure (outside of Synapse) so tracking
            #     these calls for for homeserver shutdown doesn't make sense.
            self._reactor.callLater(0.0, self.flush)  # type: ignore[call-later-not-tracked]

    def writeSequence(self, seq: Iterable[bytes]) -> None:
        for x in seq:
            self.write(x)

    def flush(self, maxbytes: int | None = None) -> None:
        if not self.buffer:
            # nothing to do. Don't write empty buffers: it upsets the
            # TLSMemoryBIOProtocol
            return

        if self.disconnected:
            return

        if maxbytes is not None:
            to_write = self.buffer[:maxbytes]
        else:
            to_write = self.buffer

        logger.info("%s->%s: %s", self._protocol, self.other, to_write)

        try:
            self.other.dataReceived(to_write)
        except Exception as e:
            logger.exception("Exception writing to protocol: %s", e)
            return

        self.buffer = self.buffer[len(to_write) :]
        if self.buffer and self.autoflush:
            # mypy ignored here because:
            #   - this is part of the test infrastructure (outside of Synapse) so tracking
            #     these calls for for homeserver shutdown doesn't make sense.
            self._reactor.callLater(0.0, self.flush)  # type: ignore[call-later-not-tracked]

        if not self.buffer and self.disconnecting:
            logger.info("FakeTransport: Buffer now empty, completing disconnect")
            self.disconnected = True

    ## ITCPTransport methods. ##

    def loseWriteConnection(self) -> None:
        """
        Half-close the write side of a TCP connection.

        If the protocol instance this is attached to provides
        IHalfCloseableProtocol, it will get notified when the operation is
        done. When closing write connection, as with loseConnection this will
        only happen when buffer has emptied and there is no registered
        producer.
        """
        raise NotImplementedError()

    def getTcpNoDelay(self) -> bool:
        """
        Return if C{TCP_NODELAY} is enabled.
        """
        return False

    def setTcpNoDelay(self, enabled: bool) -> None:
        """
        Enable/disable C{TCP_NODELAY}.

        Enabling C{TCP_NODELAY} turns off Nagle's algorithm. Small packets are
        sent sooner, possibly at the expense of overall throughput.
        """
        # Ignore setting this.

    def getTcpKeepAlive(self) -> bool:
        """
        Return if C{SO_KEEPALIVE} is enabled.
        """
        return False

    def setTcpKeepAlive(self, enabled: bool) -> None:
        """
        Enable/disable C{SO_KEEPALIVE}.

        Enabling C{SO_KEEPALIVE} sends packets periodically when the connection
        is otherwise idle, usually once every two hours. They are intended
        to allow detection of lost peers in a non-infinite amount of time.
        """
        # Ignore setting this.


def connect_client(
    reactor: ThreadedMemoryReactorClock, client_id: int
) -> tuple[IProtocol, AccumulatingProtocol]:
    """
    Connect a client to a fake TCP transport.

    Args:
        reactor
        factory: The connecting factory to build.
    """
    factory = reactor.tcpClients.pop(client_id)[2]
    client = factory.buildProtocol(None)
    server = AccumulatingProtocol()
    server.makeConnection(FakeTransport(client, reactor))
    client.makeConnection(FakeTransport(server, reactor))

    return client, server


class TestHomeServer(HomeServer):
    DATASTORE_CLASS = DataStore


def setup_test_homeserver(
    *,
    cleanup_func: Callable[[Callable[[], Optional["Deferred[None]"]]], None],
    server_name: str = "test",
    config: HomeServerConfig | None = None,
    reactor: Optional[ISynapseReactor] = None,
    homeserver_to_use: type[HomeServer] = TestHomeServer,
    db_txn_limit: int | None = None,
    test_name: str | None = None,
    **extra_homeserver_attributes: Any,
) -> HomeServer:
    """
    Setup a homeserver suitable for running tests against.  Keyword arguments
    are passed to the Homeserver constructor.

    If no datastore is supplied, one is created and given to the homeserver.

    Args:
        cleanup_func: The function used to register a cleanup routine for
            after the test. If the function returns a Deferred, the
            test case will wait until the Deferred has fired before
            proceeding to the next cleanup function.
        server_name: Homeserver name
        config: Homeserver config
        reactor: Twisted reactor
        homeserver_to_use: Homeserver class to instantiate.
        db_txn_limit: Gives the maximum number of database transactions to run per
            connection before reconnecting. 0 means no limit. If unset, defaults to None
            here which will default upstream to `0`.
        test_name: Optional test identifier used for attributing teardown timings.
        **extra_homeserver_attributes: Additional keyword arguments to install as
            `@cache_in_self` attributes on the homeserver. For example, `clock` will be
            installed as `hs._clock`.

    Calling this method directly is deprecated: you should instead derive from
    HomeserverTestCase.
    """
    _t0_wall = time.monotonic()
    if reactor is None:
        reactor = ThreadedMemoryReactorClock()

    if config is None:
        config = default_config(server_name=server_name, parse=True)

    server_name = config.server.server_name
    if not isinstance(server_name, str):
        raise ConfigError("Must be a string", ("server_name",))

    if "clock" not in extra_homeserver_attributes:
        # Ignore `multiple-internal-clocks` linter error here since we are creating a `Clock`
        # for testing purposes (i.e. outside of Synapse).
        extra_homeserver_attributes["clock"] = Clock(reactor, server_name=server_name)  # type: ignore[multiple-internal-clocks]

    config.caches.resize_all_caches()

    if USE_POSTGRES_FOR_TESTS:
        global _RECYCLED_PG_DB, _RECYCLED_PG_DB_IN_USE, _PREV_TEST_HAD_DDL
        from synapse.storage.database import pop_dirty_tables

        old_recycled_to_drop = None
        is_recycled = False
        if (
            _RECYCLED_PG_DB is not None
            and not _RECYCLED_PG_DB_IN_USE
            and not _PREV_TEST_HAD_DDL
            and os.environ.get("SYNAPSE_TEST_NO_RECYCLE_DB") != "1"
        ):
            # Primary test DB reuse
            _pg_counter("recycle_hit")
            pop_dirty_tables()
            test_db = _RECYCLED_PG_DB
            _RECYCLED_PG_DB_IN_USE = True
            is_recycled = True
        else:
            if os.environ.get("SYNAPSE_TEST_NO_RECYCLE_DB") == "1":
                _pg_counter("recycle_miss_env_disabled")
            elif _RECYCLED_PG_DB is None:
                _pg_counter("recycle_miss_no_worker_db")
            elif _RECYCLED_PG_DB_IN_USE:
                _pg_counter("recycle_miss_cached_db_in_use")
            elif _PREV_TEST_HAD_DDL:
                _pg_counter("recycle_miss_prev_had_ddl")
            else:
                _pg_counter("recycle_miss_other")

            test_db = "synapse_test_%s" % uuid.uuid4().hex
            if not _RECYCLED_PG_DB_IN_USE:
                if _RECYCLED_PG_DB is not None and _RECYCLED_PG_DB != test_db:
                    old_recycled_to_drop = _RECYCLED_PG_DB
                pop_dirty_tables()
                _RECYCLED_PG_DB = test_db
                _RECYCLED_PG_DB_IN_USE = True
                _PREV_TEST_HAD_DDL = False

        database_config: JsonDict = {
            "name": "psycopg2",
            "args": {
                "dbname": test_db,
                "host": POSTGRES_HOST,
                "password": POSTGRES_PASSWORD,
                "user": POSTGRES_USER,
                "port": POSTGRES_PORT,
                "cp_min": 1,
                "cp_max": 1,
            },
        }
    else:
        if SQLITE_PERSIST_DB:
            # The current working directory is in _trial_temp, so this gets created within that directory.
            test_db_location = os.path.abspath("test.db")
            logger.debug("Will persist db to %s", test_db_location)
            # Ensure each test gets a clean database.
            try:
                os.remove(test_db_location)
            except FileNotFoundError:
                pass
            else:
                logger.debug("Removed existing DB at %s", test_db_location)
        else:
            test_db_location = ":memory:"

        database_config = {
            "name": "sqlite3",
            "args": {"database": test_db_location, "cp_min": 1, "cp_max": 1},
        }

        # Check if we have set up a DB that we can use as a template.
        global PREPPED_SQLITE_DB_CONN
        if PREPPED_SQLITE_DB_CONN is None:
            temp_engine = create_engine(database_config)
            prepped_conn = LoggingDatabaseConnection(
                conn=sqlite3.connect(":memory:"),
                engine=temp_engine,
                default_txn_name="PREPPED_CONN",
                server_name=server_name,
            )

            prepare_database(
                prepped_conn,
                create_engine(database_config),
                # We pass `config=None` here so that the template database is prepared the
                # same way regardless of which test happens to be the first one to run.
                #
                # Notably, `prepare_database` refuses to initialise an empty database
                # when given a worker config, which would otherwise make any test using
                # `homeserver_to_use=GenericWorkerServer` fail when run on its own.
                #
                # Each test still runs `prepare_database` with its own config against its own
                # copy of this template (via `hs.setup()`), so anything config specific (like
                # module schemas) is still applied per-test.
                config=None,
            )

            # Only publish the template once it's fully prepared. Previously, this was
            # assigned before `prepare_database(...)` ran which meant that if
            # `prepare_database(...)` failed, we ended up with an unitialized/partial
            # database state and never tried to re-create it for subsequent tests.
            PREPPED_SQLITE_DB_CONN = prepped_conn

        database_config["_TEST_PREPPED_CONN"] = PREPPED_SQLITE_DB_CONN

    if db_txn_limit is not None:
        database_config["txn_limit"] = db_txn_limit

    database = DatabaseConnectionConfig("master", database_config)
    config.database.databases = [database]

    db_engine = create_engine(database.config)

    # Create or reset the database before we actually try and connect to it
    if USE_POSTGRES_FOR_TESTS:
        if old_recycled_to_drop is not None:
            if os.environ.get("SYNAPSE_TEST_SYNC_DROP_DB") == "1":
                _drop_test_db(old_recycled_to_drop, test_name, db_engine)
            else:
                _ensure_db_drop_worker()
                _DB_DROP_QUEUE.put((old_recycled_to_drop, test_name, db_engine))

        if is_recycled:
            if not _reset_recycled_postgres_db(test_db, db_engine, test_name=test_name):
                old_failed_db = test_db
                # Fallback if reset failed: generate new test_db and clone
                test_db = "synapse_test_%s" % uuid.uuid4().hex
                _RECYCLED_PG_DB = test_db
                _RECYCLED_PG_DB_IN_USE = True
                database_config["args"]["dbname"] = test_db
                database = DatabaseConnectionConfig("master", database_config)
                config.database.databases = [database]
                is_recycled = False
                if os.environ.get("SYNAPSE_TEST_SYNC_DROP_DB") == "1":
                    _drop_test_db(old_failed_db, test_name, db_engine)
                else:
                    _ensure_db_drop_worker()
                    _DB_DROP_QUEUE.put((old_failed_db, test_name, db_engine))

        try:
            if not is_recycled:
                _t0 = time.monotonic()
                db_conn = db_engine.module.connect(
                    dbname=POSTGRES_DBNAME_FOR_INITIAL_CREATE,
                    user=POSTGRES_USER,
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    password=POSTGRES_PASSWORD,
                )
                db_engine.attempt_to_set_autocommit(db_conn, True)
                cur = db_conn.cursor()
                create_db_strategy, _ = get_postgres_clone_strategy()
                cur.execute(
                    "CREATE DATABASE %s WITH TEMPLATE %s%s;"
                    % (test_db, POSTGRES_BASE_DB, create_db_strategy)
                )
                cur.close()
                db_conn.close()
                _pg_timing(
                    "create_database", time.monotonic() - _t0, test_name=test_name
                )

            database_config["_TEST_DB_IS_FRESH"] = True
            database = DatabaseConnectionConfig("master", database_config)
            config.database.databases = [database]

            def cleanup() -> None:
                global _RECYCLED_PG_DB, _RECYCLED_PG_DB_IN_USE, _PREV_TEST_HAD_DDL
                if test_db == _RECYCLED_PG_DB:
                    # Dirty-table tracking is process-global, not per DB. Only
                    # the primary DB cleanup may consume it; secondary
                    # homeserver cleanups run first (Trial cleanups are LIFO)
                    # and would otherwise erase the table list before the
                    # recycled primary is reset for the next test.
                    from synapse.storage.database import pop_dirty_tables

                    _, had_ddl, ddl_triggers = pop_dirty_tables()
                    if (
                        had_ddl
                        and ddl_triggers
                        and os.environ.get("SYNAPSE_DEBUG_DDL_TRIGGERS") == "1"
                    ):
                        import sys

                        print(
                            f"[DDL-TRIGGER] test={test_name} triggers={ddl_triggers!r}",
                            file=sys.stderr,
                        )
                    _RECYCLED_PG_DB_IN_USE = False
                    if had_ddl:
                        _pg_counter("cleanup_dropped_primary_had_ddl")
                        _RECYCLED_PG_DB = None
                        _PREV_TEST_HAD_DDL = False
                        if os.environ.get("SYNAPSE_TEST_SYNC_DROP_DB") == "1":
                            _drop_test_db(test_db, test_name, db_engine)
                        else:
                            _ensure_db_drop_worker()
                            _DB_DROP_QUEUE.put((test_db, test_name, db_engine))
                    elif os.environ.get("SYNAPSE_TEST_NO_RECYCLE_DB") == "1":
                        _pg_counter("cleanup_dropped_primary_env_disabled")
                        _RECYCLED_PG_DB = None
                        _PREV_TEST_HAD_DDL = False
                        if os.environ.get("SYNAPSE_TEST_SYNC_DROP_DB") == "1":
                            _drop_test_db(test_db, test_name, db_engine)
                        else:
                            _ensure_db_drop_worker()
                            _DB_DROP_QUEUE.put((test_db, test_name, db_engine))
                    else:
                        _pg_counter("cleanup_recycled_primary")
                        _PREV_TEST_HAD_DDL = False
                else:
                    _pg_counter("cleanup_dropped_secondary_hs")
                    # Extra homeserver in a multi-homeserver test (e.g. worker / federated peer)
                    if os.environ.get("SYNAPSE_TEST_SYNC_DROP_DB") == "1":
                        _drop_test_db(test_db, test_name, db_engine)
                    else:
                        _ensure_db_drop_worker()
                        _DB_DROP_QUEUE.put((test_db, test_name, db_engine))

            if not LEAVE_DB:
                # Register the cleanup hook
                cleanup_func(cleanup)
        except Exception:
            if test_db == _RECYCLED_PG_DB:
                _RECYCLED_PG_DB_IN_USE = False
                _RECYCLED_PG_DB = None
            # If setup failed after a fresh clone was created, drop it now so
            # it doesn't leak.  (If it was a recycled DB we just cleared the
            # pointer above; it will be re-cloned on the next test.)
            if not is_recycled:
                if os.environ.get("SYNAPSE_TEST_SYNC_DROP_DB") == "1":
                    _drop_test_db(test_db, test_name, db_engine)
                else:
                    _ensure_db_drop_worker()
                    _DB_DROP_QUEUE.put((test_db, test_name, db_engine))
            raise

    hs = homeserver_to_use(
        server_name,
        config=config,
        reactor=reactor,
    )

    # A weakref, not a strong reference: tests/app/test_homeserver_shutdown.py
    # explicitly shuts a homeserver down and checks it becomes garbage
    # collectible *before* Trial ever invokes this cleanup, which would be
    # impossible if this closure held a strong reference for the whole test.
    # Worker/secondary homeservers created via `make_worker_hs` are kept
    # alive independently for the test's duration (see
    # `BaseMultiWorkerStreamTestCase._worker_homeservers`), so this no longer
    # goes stale before it fires for them.
    cleanup_hs_ref = weakref.ref(hs)

    def shutdown_hs_on_cleanup() -> "Deferred[None]":
        cleanup_hs = cleanup_hs_ref()
        if cleanup_hs is None:
            return defer.succeed(None)
        _sd0 = time.monotonic()
        deferred = defer.ensureDeferred(cleanup_hs.shutdown())
        if USE_POSTGRES_FOR_TESTS:

            def _record_shutdown_timing(result: Any) -> Any:
                _pg_timing("hs_shutdown", time.monotonic() - _sd0, test_name=test_name)
                return result

            deferred.addBoth(_record_shutdown_timing)
        return deferred

    # Install @cache_in_self attributes
    for key, val in extra_homeserver_attributes.items():
        setattr(hs, "_" + key, val)

    # Mock TLS
    hs.tls_server_context_factory = Mock()

    # Patch `make_pool` before initialising the database, to make database transactions
    # synchronous for testing.
    _t0 = time.monotonic()

    # Set up PG timing callback for database timing profiling.
    from synapse.storage.databases import set_pg_timing_callback

    if os.environ.get("SYNAPSE_PG_TIMINGS"):
        set_pg_timing_callback(_pg_timing)

    with patch("synapse.storage.database.make_pool", side_effect=make_fake_db_pool):
        hs.setup()
    if USE_POSTGRES_FOR_TESTS:
        # Only counted for PG: the "Postgres test-DB lifecycle timings"
        # summary must not silently fold SQLite setup into PG numbers.
        _pg_timing("hs_setup_total", time.monotonic() - _t0)

    if os.environ.get("SYNAPSE_PG_TIMINGS"):
        set_pg_timing_callback(None)

    # Ideally, setup/start would be separated but since this is historically used
    # throughout tests, we keep the existing behavior for now. We probably just need to
    # rename this function.
    start_test_homeserver(hs=hs, cleanup_func=cleanup_func, reactor=reactor)

    # Cleanups run in reverse registration order. Register this after
    # `start_test_homeserver`, which registers the PostgreSQL pool cleanup, so
    # teardown is: homeserver shutdown, pool close, database drop. The pool is
    # closed by `shutdown()` rather than by a separate cleanup registered in
    # `start_test_homeserver`, so the ordering matters: shutting down after the
    # pool has already been closed can leave a live PostgreSQL session behind
    # and make DROP DATABASE fail.
    cleanup_func(shutdown_hs_on_cleanup)

    if USE_POSTGRES_FOR_TESTS:
        # Whole-function wall time: homeserver construction + DB lifecycle +
        # `hs.setup()` + `start_test_homeserver`. `hs_setup_total` above only
        # measures the database-initialisation slice of this, so the difference
        # between the two tags is the pure Python-side construction cost --
        # that's the slice the per-table/lifecycle timers have never covered.
        _pg_timing("hs_setup_wall", time.monotonic() - _t0_wall)

    return hs


def start_test_homeserver(
    *,
    hs: HomeServer,
    cleanup_func: Callable[[Callable[[], Optional["Deferred[None]"]]], None],
    reactor: ISynapseReactor,
) -> None:
    """
    Start a homeserver for testing.

    Args:
        hs: The homeserver to start.
        cleanup_func: The function used to register a cleanup routine for
            after the test. If the function returns a Deferred, the
            test case will wait until the Deferred has fired before
            proceeding to the next cleanup function.
        reactor: Twisted reactor
    """

    # Register background tasks required by this server. This must be done
    # somewhat manually due to the background tasks not being registered
    # unless handlers are instantiated.
    #
    # Since, we don't have to worry about `daemonize` (forking the process) in tests, we
    # can just start the background tasks straight away after `hs.setup`. (compare this
    # with where we call `hs.start_background_tasks()` outside of the test environment).
    if hs.config.worker.run_background_tasks:
        hs.start_background_tasks()

    # Since we've changed the databases to run DB transactions on the same
    # thread, we need to stop the event fetcher hogging that one thread.
    hs.get_datastores().main.USE_DEDICATED_DB_THREADS_FOR_EVENT_FETCHING = False

    # bcrypt is far too slow to be doing in unit tests
    # Need to let the HS build an auth handler and then mess with it
    # because AuthHandler's constructor requires the HS, so we can't make one
    # beforehand and pass it in to the HS's constructor (chicken / egg)
    async def hash(p: str) -> str:
        return hashlib.md5(p.encode("utf8")).hexdigest()

    hs.get_auth_handler().hash = hash  # type: ignore[assignment]

    async def validate_hash(p: str, h: str) -> bool:
        return hashlib.md5(p.encode("utf8")).hexdigest() == h

    hs.get_auth_handler().validate_hash = validate_hash  # type: ignore[assignment]

    # We need to replace the media threadpool with the fake test threadpool.
    def thread_pool() -> threadpool.ThreadPool:
        return reactor.getThreadPool()

    hs.get_media_sender_thread_pool = thread_pool  # type: ignore[method-assign]

    # Load the OIDC provider metadatas, if OIDC is enabled.
    # This matches `start` in synapse/app/_base.py
    #
    # TODO: Extract common startup logic somewhere cleaner
    if hs.config.oidc.oidc_enabled:
        oidc = hs.get_oidc_handler()
        # Preload the provider metadata.
        # This will spawn fire-and-forget background processes.
        oidc.preload_metadata()

    # Load any configured modules into the homeserver
    module_api = hs.get_module_api()
    for module, module_config in hs.config.modules.loaded_modules:
        module(config=module_config, api=module_api)

    if hs.config.auto_accept_invites.enabled:
        # Start the local auto_accept_invites module.
        m = InviteAutoAccepter(hs.config.auto_accept_invites, module_api)
        logger.info("Loaded local module %s", m)

    load_legacy_spam_checkers(hs)
    load_legacy_third_party_event_rules(hs)
    load_legacy_presence_router(hs)
    load_legacy_password_auth_providers(hs)
