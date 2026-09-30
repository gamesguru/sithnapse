#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

import asyncio
import atexit
import shutil
import sqlite3
import tempfile
import threading
from typing import Any
from unittest import mock, skipUnless
from uuid import uuid4

from twisted.test.proto_helpers import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main import (
    embedded_common,
    embedded_event_edges as embedded_event_edges_module,
)
from synapse.storage.databases.main.embedded_common import (
    FLUSH_DELAY_SECS,
    _clear_coalescer,
    _FlushCoalescer,
    _set_coalescer,
    enable_ffi_counting,
    get_ffi_count,
    suppress_diagnostic_timings,
)
from synapse.storage.databases.main.embedded_event_edges import (
    check_event_edges_migration_complete,
    delete_event_edges_batch,
    drain_edge_index_outbox,
    flush_edge_writes,
    get_event_edges_backward_batch,
    get_event_edges_forward_batch,
    put_event_edges_batch,
    queue_edge_write,
    queued_edge_write_count,
)
from synapse.synapse_rust import mtxdb_engine
from synapse.util.clock import Clock

from tests import unittest
from tests.server import ThreadedMemoryReactorClock
from tests.unittest import HomeserverTestCase
from tests.utils import EMBEDDED_DB_ENGINE, EMBEDDED_DB_PATH

if EMBEDDED_DB_ENGINE and EMBEDDED_DB_PATH:
    # The test harness has already selected a worker-specific store. Reuse
    # that path instead of opening a private store before the homeserver is
    # created, which would win the Rust process-global OnceCell.
    _TEST_ENGINE_TMPDIR = EMBEDDED_DB_PATH
else:
    _TEST_ENGINE_TMPDIR = tempfile.mkdtemp(prefix="test-embedded-event-edges-")
    # mtxdb's Python binding owns process-global pools. Keep this directory
    # alive until process exit; deleting it during a test class teardown leaves
    # the OnceCell-backed engine pointing at an unlinked store.
    atexit.register(shutil.rmtree, _TEST_ENGINE_TMPDIR, ignore_errors=True)
mtxdb_engine.open_client(_TEST_ENGINE_TMPDIR)


class EmbeddedEventEdgesTestCase(unittest.TestCase):
    """Pure unit tests for the embedded event-edges FFI layer.

    mtxdb_engine.open_client() is backed by a Rust OnceCell: the engine is
    opened exactly once per process and subsequent calls are no-ops.  The
    engine's data directory must therefore survive for the life of the
    process; test setup and teardown must not delete it.  The module-level
    fixture opens one store and removes it only at process exit.
    """

    def setUp(self) -> None:
        self.namespace = "test-edges-ns"
        self.room_id = "!room:example.org"

    def tearDown(self) -> None:
        # Drain every namespace's coalescing queue so rows queued by
        # queue_edge_write (never flushed here -- the unit test has no flush
        # coalescer driving a timer) do not leak into the next test.
        flush_edge_writes()

    def test_outbox_ack_is_idempotent_after_mtxdb_commit(self) -> None:
        """Rows survive the commit/SQL-ack gap and are removed on retry."""

        class _Txn:
            def __init__(self, connection: sqlite3.Connection) -> None:
                self._cursor = connection.cursor()

            def execute(self, sql: str, args: tuple[Any, ...] = ()) -> None:
                self._cursor.execute(sql, args)

            def fetchone(self) -> Any:
                return self._cursor.fetchone()

            def __iter__(self) -> Any:
                return iter(self._cursor)

            @property
            def rowcount(self) -> int:
                return self._cursor.rowcount

        class _DBPool:
            def __init__(self) -> None:
                self.connection = sqlite3.connect(":memory:")
                self.connection.executescript(
                    """
                    CREATE TABLE edge_index_outbox (
                        room_id TEXT, source_version INTEGER, event_id TEXT,
                        prev_event_id TEXT, operation TEXT
                    );
                    CREATE TABLE room_edge_rebuild_checkpoints (
                        room_id TEXT, last_replayed_source_version INTEGER,
                        lease_expires_at_ms INTEGER
                    );
                    """
                )

            async def runInteraction(
                self, _name: str, callback: Any, *args: Any
            ) -> Any:
                txn = _Txn(self.connection)
                result = callback(txn, *args)
                self.connection.commit()
                return result

        class _Lock:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *_args: Any) -> None:
                return None

        class _Locks:
            def acquire_lock(self, *_args: Any) -> _Lock:
                return _Lock()

        class _HS:
            def get_worker_locks_handler(self) -> _Locks:
                return _Locks()

        class _Store:
            def __init__(self) -> None:
                self.db_pool = _DBPool()
                self.hs = _HS()

        store = _Store()
        room_id = "!outbox-idempotent:test"
        store.db_pool.connection.execute(
            "INSERT INTO edge_index_outbox VALUES (?, ?, ?, ?, ?)",
            (room_id, 1, "$child", "$parent", "insert"),
        )
        store.db_pool.connection.execute(
            "INSERT INTO room_edge_rebuild_checkpoints VALUES (?, ?, ?)",
            (room_id, 0, 9_999_999_999_999),
        )
        store.db_pool.connection.commit()

        with (
            mock.patch(
                "synapse.synapse_rust.mtxdb_engine.room_forward_meta_get",
                return_value=(0, 1),
            ),
            mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_apply_forward_outbox"
            ) as apply,
        ):
            # The mtxdb commit is already reflected by the watermark, but the
            # active rebuild checkpoint prevents the SQL acknowledgement.
            self.assertFalse(asyncio.run(drain_edge_index_outbox(store)))
            apply.assert_not_called()
            self.assertEqual(
                store.db_pool.connection.execute(
                    "SELECT COUNT(*) FROM edge_index_outbox"
                ).fetchone()[0],
                1,
            )

            # Once the consumer lease is gone, a retry must acknowledge the
            # already-published row without applying it a second time.
            store.db_pool.connection.execute(
                "DELETE FROM room_edge_rebuild_checkpoints"
            )
            store.db_pool.connection.commit()
            self.assertTrue(asyncio.run(drain_edge_index_outbox(store)))
            apply.assert_not_called()
            self.assertEqual(
                store.db_pool.connection.execute(
                    "SELECT COUNT(*) FROM edge_index_outbox"
                ).fetchone()[0],
                0,
            )

    def test_expired_rebuild_checkpoint_does_not_block_ack(self) -> None:
        """An expired rebuild lease cannot retain published outbox rows."""

        class _Txn:
            def __init__(self, connection: sqlite3.Connection) -> None:
                self._cursor = connection.cursor()

            def execute(self, sql: str, args: tuple[Any, ...] = ()) -> None:
                self._cursor.execute(sql, args)

            def fetchone(self) -> Any:
                return self._cursor.fetchone()

            def __iter__(self) -> Any:
                return iter(self._cursor)

            @property
            def rowcount(self) -> int:
                return self._cursor.rowcount

        class _DBPool:
            def __init__(self) -> None:
                self.connection = sqlite3.connect(":memory:")
                self.connection.executescript(
                    """
                    CREATE TABLE edge_index_outbox (
                        room_id TEXT, source_version INTEGER, event_id TEXT,
                        prev_event_id TEXT, operation TEXT
                    );
                    CREATE TABLE room_edge_rebuild_checkpoints (
                        room_id TEXT, last_replayed_source_version INTEGER,
                        lease_expires_at_ms INTEGER
                    );
                    """
                )

            async def runInteraction(
                self, _name: str, callback: Any, *args: Any
            ) -> Any:
                result = callback(_Txn(self.connection), *args)
                self.connection.commit()
                return result

        class _Lock:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *_args: Any) -> None:
                return None

        class _Locks:
            def acquire_lock(self, *_args: Any) -> _Lock:
                return _Lock()

        class _HS:
            def get_worker_locks_handler(self) -> _Locks:
                return _Locks()

        class _Store:
            def __init__(self) -> None:
                self.db_pool = _DBPool()
                self.hs = _HS()

        store = _Store()
        room_id = "!outbox-expired:test"
        store.db_pool.connection.execute(
            "INSERT INTO edge_index_outbox VALUES (?, ?, ?, ?, ?)",
            (room_id, 2, "$child", "$parent", "insert"),
        )
        store.db_pool.connection.execute(
            "INSERT INTO room_edge_rebuild_checkpoints VALUES (?, ?, ?)",
            (room_id, 0, 1),
        )
        store.db_pool.connection.commit()

        with (
            mock.patch(
                "synapse.synapse_rust.mtxdb_engine.room_forward_meta_get",
                return_value=(0, 2),
            ),
            mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_apply_forward_outbox"
            ) as apply,
        ):
            self.assertTrue(asyncio.run(drain_edge_index_outbox(store)))
            apply.assert_not_called()
            self.assertEqual(
                store.db_pool.connection.execute(
                    "SELECT COUNT(*) FROM edge_index_outbox"
                ).fetchone()[0],
                0,
            )

    def test_event_dag_barrier_does_not_drain_edges(self) -> None:
        """The per-event JSON barrier must leave edge batching untouched."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            with (
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._drain_edge_writes"
                ) as drain,
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._do_sync_pools"
                ) as sync,
            ):
                coalescer.sync_event_dag_now()
                drain.assert_not_called()
                sync.assert_called_once_with({embedded_common.Pool.EVENT_DAG})
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_event_dag_barrier_failure_reschedules(self) -> None:
        """A failed narrow barrier retains dirty state and schedules retry."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            coalescer.mark_dirty(embedded_common.Pool.EVENT_DAG)
            with mock.patch(
                "synapse.storage.databases.main.embedded_common._do_sync_pools",
                side_effect=RuntimeError("simulated sync failure"),
            ):
                with self.assertRaises(RuntimeError):
                    coalescer.sync_event_dag_now()
            self.assertIn(embedded_common.Pool.EVENT_DAG, coalescer._dirty)
            self.assertIsNotNone(coalescer._delayed_call)
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_event_dag_barrier_failure_does_not_reset_existing_timer(self) -> None:
        """A failed narrow barrier must not cancel-and-reschedule an
        already-pending debounce timer (armed at ``_FLUSH_DELAY`` by
        ``mark_dirty``). Doing so under sustained failures -- e.g. calls
        arriving faster than ``_RETRY_DELAY`` apart, plausible since this
        barrier runs per persisted event -- would perpetually push the timer
        out and starve ``_flush`` forever: the same livelock shape the
        success path avoids, just triggered by errors instead of successes.
        An existing timer, whatever its delay, must be left to fire on its
        own; only the absence of any timer should cause a new one to be
        armed at ``_RETRY_DELAY``."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            coalescer.mark_dirty(embedded_common.Pool.EVENT_DAG)
            assert coalescer._delayed_call is not None
            pending_at_flush_delay = coalescer._delayed_call.getTime()
            self.assertAlmostEqual(
                pending_at_flush_delay, reactor.seconds() + 0.5, places=3
            )

            with mock.patch(
                "synapse.storage.databases.main.embedded_common._do_sync_pools",
                side_effect=RuntimeError("simulated sync failure"),
            ):
                with self.assertRaises(RuntimeError):
                    coalescer.sync_event_dag_now()

            self.assertIsNotNone(coalescer._delayed_call)
            assert coalescer._delayed_call is not None
            # Unchanged: the pre-existing _FLUSH_DELAY timer was left alone,
            # not cancelled and re-armed at _RETRY_DELAY.
            self.assertEqual(coalescer._delayed_call.getTime(), pending_at_flush_delay)
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_sync_now_drain_failure_reschedules_and_raises(self) -> None:
        """A failing ``_drain_edge_writes()`` inside ``sync_now`` must not
        strand EVENT_DAG: the timer is cancelled at the top of the method, so
        a raise from the drain has to still reach the retry-scheduling
        ``finally`` (via the outer try/except) or nothing would ever retry
        it -- the same stranded-work shape as the narrow-barrier livelock
        fixed earlier, just via the drain instead of the sync call itself."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            with (
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._drain_edge_writes",
                    side_effect=RuntimeError("simulated drain failure"),
                ),
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._do_sync_pools"
                ) as sync,
            ):
                with self.assertRaises(RuntimeError):
                    coalescer.sync_now(pools=[embedded_common.Pool.EVENT_DAG])
                # The sync call must never be reached: the drain failed first.
                sync.assert_not_called()
            self.assertIn(embedded_common.Pool.EVENT_DAG, coalescer._dirty)
            self.assertIsNotNone(coalescer._delayed_call)
            assert coalescer._delayed_call is not None
            self.assertAlmostEqual(
                coalescer._delayed_call.getTime(), reactor.seconds() + 1.0, places=3
            )
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_sync_now_attributes_failure_to_failing_pool_only(self) -> None:
        """Per-pool sync failures must not cross-contaminate: if EVENT_DAG
        syncs cleanly but STATE fails, only STATE should end up re-dirtied,
        and the retry timer should use the failure backoff since the call
        did not fully succeed."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:

            def fake_sync(pools: set) -> None:
                if embedded_common.Pool.STATE in pools:
                    raise RuntimeError("simulated state sync failure")

            with (
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._drain_edge_writes",
                    return_value=False,
                ),
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._do_sync_pools",
                    side_effect=fake_sync,
                ),
            ):
                with self.assertRaises(RuntimeError):
                    coalescer.sync_now(
                        pools=[
                            embedded_common.Pool.EVENT_DAG,
                            embedded_common.Pool.STATE,
                        ]
                    )
            self.assertNotIn(embedded_common.Pool.EVENT_DAG, coalescer._dirty)
            self.assertIn(embedded_common.Pool.STATE, coalescer._dirty)
            self.assertIsNotNone(coalescer._delayed_call)
            assert coalescer._delayed_call is not None
            self.assertAlmostEqual(
                coalescer._delayed_call.getTime(), reactor.seconds() + 1.0, places=3
            )
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_sync_now_success_with_leftover_dirty_uses_flush_delay(self) -> None:
        """When ``sync_now`` fully succeeds but an unrelated pool is left
        dirty (e.g. a concurrent write raced in), the rearmed timer should
        use the normal debounce delay, not the failure backoff -- that pool
        never failed anything this call."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            coalescer._dirty.add(embedded_common.Pool.AUTH_CHAIN)
            with (
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._drain_edge_writes",
                    return_value=False,
                ),
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._do_sync_pools"
                ),
            ):
                coalescer.sync_now(pools=[embedded_common.Pool.EVENT_DAG])
            self.assertIn(embedded_common.Pool.AUTH_CHAIN, coalescer._dirty)
            self.assertIsNotNone(coalescer._delayed_call)
            assert coalescer._delayed_call is not None
            self.assertAlmostEqual(
                coalescer._delayed_call.getTime(), reactor.seconds() + 0.5, places=3
            )
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_put_get_backward_and_forward(self) -> None:
        # No locator seeding: `event_edges_put` publishes locators for the
        # events it touches, so an edge written with no preceding
        # `event_json_put` -- the legacy/repair case -- still resolves on read.
        # e1 has prev_events p1 and p2
        rows = [
            (self.room_id, "$e1", "$p1", False),
            (self.room_id, "$e1", "$p2", True),
        ]
        put_event_edges_batch(self.namespace, rows)

        # Backward edges for $e1
        backward = get_event_edges_backward_batch(
            self.namespace, ["$e1", "$nonexistent"]
        )
        self.assertIn("$e1", backward)
        self.assertEqual(backward["$e1"], [("$p1", False), ("$p2", True)])
        self.assertIsNone(backward["$nonexistent"])

        # Forward edges for $p1 and $p2
        forward = get_event_edges_forward_batch(
            self.namespace, ["$p1", "$p2", "$nonexistent"]
        )
        self.assertEqual(forward["$p1"], ["$e1"])
        self.assertEqual(forward["$p2"], ["$e1"])
        self.assertIsNone(forward["$nonexistent"])

        # Append another event $e2 whose prev_event is $p1
        put_event_edges_batch(self.namespace, [(self.room_id, "$e2", "$p1", False)])
        forward2 = get_event_edges_forward_batch(self.namespace, ["$p1"])
        self.assertCountEqual(forward2["$p1"] or [], ["$e1", "$e2"])

        # Delete $e1 tombstones backward edge and removes $e1 from parents' forward edges
        delete_event_edges_batch(self.namespace, ["$e1"])
        backward_after = get_event_edges_backward_batch(self.namespace, ["$e1"])
        self.assertIsNone(backward_after["$e1"])

        forward_after = get_event_edges_forward_batch(self.namespace, ["$p1", "$p2"])
        self.assertEqual(forward_after["$p1"], ["$e2"])
        self.assertIsNone(forward_after["$p2"])

    def test_queues_are_namespace_partitioned(self) -> None:
        """Rows queued under one namespace never flush under another, and a
        no-namespace flush drains every namespace separately."""
        ns_a = "test-edges-part-a"
        ns_b = "test-edges-part-b"
        try:
            queue_edge_write(ns_a, [(self.room_id, "$a1", "$ap", False)])
            queue_edge_write(ns_b, [(self.room_id, "$b1", "$bp", False)])
            self.assertEqual(queued_edge_write_count(), 2)

            # Flushing A must only write A's rows; B's stay queued.
            self.assertTrue(flush_edge_writes(ns_a))
            self.assertEqual(queued_edge_write_count(ns_b), 1)

            back_a = get_event_edges_backward_batch(ns_a, ["$a1"])
            self.assertEqual(back_a["$a1"], [("$ap", False)])
            back_b = get_event_edges_backward_batch(ns_b, ["$b1"])
            self.assertIsNone(back_b["$b1"])

            # The no-namespace drain covers B as well.
            self.assertTrue(flush_edge_writes())
            self.assertEqual(queued_edge_write_count(ns_b), 0)
            back_b2 = get_event_edges_backward_batch(ns_b, ["$b1"])
            self.assertEqual(back_b2["$b1"], [("$bp", False)])
        finally:
            flush_edge_writes()

    def test_flush_failure_retains_queued_rows(self) -> None:
        """A failed FFI write re-queues the drained rows instead of dropping
        them, and a later flush succeeds."""
        ns = "test-edges-ffi-failure"
        rows = [(self.room_id, f"$fail{i}", f"$failp{i}", False) for i in range(3)]
        queue_edge_write(ns, rows)
        self.assertEqual(queued_edge_write_count(ns), 3)

        with mock.patch(
            "synapse.storage.databases.main.embedded_event_edges.put_event_edges_batch",
            side_effect=RuntimeError("simulated FFI failure"),
        ):
            with self.assertRaises(RuntimeError):
                flush_edge_writes(ns)
            # Committed rows survive the failure.
            self.assertEqual(queued_edge_write_count(ns), 3)

        # A subsequent drain writes them.
        self.assertTrue(flush_edge_writes(ns))
        self.assertEqual(queued_edge_write_count(ns), 0)
        back = get_event_edges_backward_batch(ns, [rows[0][1]])
        self.assertEqual(back[rows[0][1]], [(rows[0][2], False)])

    def test_concurrent_flushes_do_not_lose_rows(self) -> None:
        """Concurrent queued flushes on the same namespace are serialized;
        every committed row is written exactly once after they settle."""
        ns = "test-edges-concurrent"
        num_threads = 4
        rows_per_thread = 32

        def worker(seed: int) -> None:
            rows = [
                (self.room_id, f"$c{seed}_{i}", f"$cp{seed}", False)
                for i in range(rows_per_thread)
            ]
            for chunk in range(0, len(rows), 8):
                queue_edge_write(ns, rows[chunk : chunk + 8])
                try:
                    flush_edge_writes(ns)
                except Exception:
                    pass

        threads = [
            threading.Thread(target=worker, args=(t,)) for t in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(queued_edge_write_count(ns), 0)
        # Backward edges for every row written, and $cp<N>'s forward lists
        # carry every child.
        for seed in range(num_threads):
            for i in range(rows_per_thread):
                back = get_event_edges_backward_batch(ns, [f"$c{seed}_{i}"])
                self.assertEqual(back[f"$c{seed}_{i}"], [(f"$cp{seed}", False)])
            fwd = get_event_edges_forward_batch(ns, [f"$cp{seed}"])
            children = fwd.get(f"$cp{seed}") or []
            self.assertEqual(
                set(children), {f"$c{seed}_{i}" for i in range(rows_per_thread)}
            )

    def test_purge_cancels_queued_edges_for_purged_events(self) -> None:
        """A purge races queued writes: rows for the purged event must not be
        flushed (which would then be erased) and must not resurrect the
        tombstone later."""
        ns = "test-edges-purge-race"
        # SQL committed these edges but the mirror hasn't flushed them yet.
        queue_edge_write(ns, [(self.room_id, "$victim", "$parent", False)])
        queue_edge_write(ns, [(self.room_id, "$live", "$other", False)])
        self.assertEqual(queued_edge_write_count(ns), 2)

        # Purge races the queue: $victim's row is cancelled (not written then
        # erased), while the unrelated $live row remains queued.
        delete_event_edges_batch(ns, ["$victim"])
        self.assertEqual(queued_edge_write_count(ns), 1)

        back = get_event_edges_backward_batch(ns, ["$victim"])
        self.assertIsNone(back["$victim"])
        fwd = get_event_edges_forward_batch(ns, ["$parent"])
        self.assertNotIn("$victim", fwd.get("$parent") or [])

        # A later drain must not resurrect $victim's edges.
        flush_edge_writes()
        back_after = get_event_edges_backward_batch(ns, ["$victim"])
        self.assertIsNone(back_after["$victim"])
        fwd_after = get_event_edges_forward_batch(ns, ["$parent"])
        self.assertNotIn("$victim", fwd_after.get("$parent") or [])

        # The unrelated edge survived the purge barrier.
        back_live = get_event_edges_backward_batch(ns, ["$live"])
        self.assertEqual(back_live["$live"], [("$other", False)])

    def test_enqueue_after_purge_cancel_is_tombstoned(self) -> None:
        """The racy ordering the cancel step alone cannot cover: a transaction
        that committed just before the purge runs its post-commit enqueue AFTER
        the purge cancelled the queue.  The tombstone set must drop that row so
        a later flush cannot resurrect the purged event's edges."""
        ns = "test-edges-purge-late-enqueue"
        try:
            # Purge runs first: the event is gone from SQL and its mtxdb edge
            # is tombstoned.
            delete_event_edges_batch(ns, ["$late"])

            # Now the racing enqueue lands, after cancellation.
            queue_edge_write(ns, [(self.room_id, "$late", "$lateparent", False)])
            self.assertEqual(
                queued_edge_write_count(ns),
                0,
                "a tombstoned event must not be re-queued",
            )

            # Even an explicit drain must not resurrect it.
            flush_edge_writes(ns)
            back = get_event_edges_backward_batch(ns, ["$late"])
            self.assertIsNone(back["$late"])
            fwd = get_event_edges_forward_batch(ns, ["$lateparent"])
            self.assertNotIn("$late", fwd.get("$lateparent") or [])
        finally:
            flush_edge_writes()

    def test_backward_extremity_survives_purge(self) -> None:
        """A queued forward row (live_child → purged_parent) is a backward
        extremity and must survive purge.  Synapse's SQL purge only deletes
        rows whose own event_id is purged; live children referencing a purged
        prev_event_id are preserved.  The mtxdb coalescer must match."""
        ns = "test-edges-backward-extremity"
        parent = "$purged_parent"
        child = "$live_child"
        try:
            queue_edge_write(ns, [(self.room_id, child, parent, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            delete_event_edges_batch(ns, [parent])

            # The forward row (child → parent) was NOT cancelled — child
            # (row[1]) is live — and remains queued for the coalescer.
            self.assertEqual(queued_edge_write_count(ns), 1)
            flush_edge_writes(ns)

            # The edge exists in mtxdb: backward and forward both present.
            back = get_event_edges_backward_batch(ns, [child])
            self.assertEqual(back[child], [(parent, False)])
            fwd = get_event_edges_forward_batch(ns, [parent])
            self.assertIn(child, fwd.get(parent) or [])
        finally:
            flush_edge_writes(ns)

    def test_unrelated_queued_write_does_not_resurrect_deleted_sibling(self) -> None:
        """The queue-drain-free purge (delete_event_edges_batch no longer
        flushes the whole namespace first) leaves unrelated queued rows in
        place. This only stays correct if a later coalescer flush of that row
        appends against the *post-delete* forward list rather than replaying
        a stale snapshot taken when it was enqueued. Regression guard for
        that invariant: delete a sibling that is already durable in mtxdb,
        then flush an unrelated queued write to the same parent, and confirm
        the deleted sibling is not resurrected while the new child lands."""
        ns = "test-edges-no-resurrection-on-drainless-purge"
        parent = "$shared_parent"
        victim = "$purged_sibling"
        sibling = "$new_sibling"
        try:
            # Seed victim's edge as already-durable mtxdb state (not queued).
            queue_edge_write(ns, [(self.room_id, victim, parent, False)])
            flush_edge_writes(ns)
            fwd = get_event_edges_forward_batch(ns, [parent])
            self.assertIn(victim, fwd.get(parent) or [])

            # Queue an unrelated write to the same parent's forward list.
            # It must stay queued across the purge below (drain-free purge).
            queue_edge_write(ns, [(self.room_id, sibling, parent, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            delete_event_edges_batch(ns, [victim])

            # Unrelated row was not touched by the drain-free delete.
            self.assertEqual(queued_edge_write_count(ns), 1)

            # Now let the coalescer apply the queued row.
            flush_edge_writes(ns)

            fwd = get_event_edges_forward_batch(ns, [parent])
            children = fwd.get(parent) or []
            self.assertIn(sibling, children)
            self.assertNotIn(victim, children)
        finally:
            flush_edge_writes(ns)

    def test_purge_enqueue_race_is_serialized(self) -> None:
        """Force the dangerous ordering: an enqueue arriving while the purge is
        paused between its cancel step and its FFI delete.  The namespace flush
        lock parks the enqueue until the purge has recorded its tombstone, so
        the row can never land in the cancel->delete window and a later drain
        cannot resurrect the purged event."""
        ns = "test-edges-purge-barrier"
        victim = "$barrier_victim"
        parent = "$barrier_parent"
        real_delete = mtxdb_engine.event_edges_delete
        delete_entered = threading.Event()
        delete_release = threading.Event()
        enqueue_started = threading.Event()
        enqueue_lock_attempted = threading.Event()
        enqueue_finished = threading.Event()
        thread_role = threading.local()

        real_namespace_flush_lock = embedded_event_edges_module._namespace_flush_lock

        class TrackingLock:
            def __init__(self, lock: threading.Lock) -> None:
                self._lock = lock

            def acquire(self, *args: Any, **kwargs: Any) -> Any:
                if getattr(thread_role, "is_enqueue", False):
                    enqueue_lock_attempted.set()
                return self._lock.acquire(*args, **kwargs)

            def release(self) -> None:
                self._lock.release()

            def __enter__(self) -> "TrackingLock":
                self.acquire()
                return self

            def __exit__(self, *args: Any) -> None:
                self.release()

        def tracking_namespace_flush_lock(namespace: str) -> TrackingLock:
            return TrackingLock(real_namespace_flush_lock(namespace))

        def slow_delete(*args: Any, **kwargs: Any) -> Any:
            delete_entered.set()
            if not delete_release.wait(timeout=5):
                raise TimeoutError("mocked delete was never released")
            return real_delete(*args, **kwargs)

        def purge() -> None:
            delete_event_edges_batch(ns, [victim])

        def enqueue() -> None:
            enqueue_started.set()
            thread_role.is_enqueue = True
            try:
                queue_edge_write(ns, [(self.room_id, victim, parent, False)])
                enqueue_finished.set()
            finally:
                thread_role.is_enqueue = False

        try:
            with (
                suppress_diagnostic_timings(),
                mock.patch(
                    "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                    side_effect=slow_delete,
                ),
                mock.patch.object(
                    embedded_event_edges_module,
                    "_namespace_flush_lock",
                    side_effect=tracking_namespace_flush_lock,
                ),
            ):
                purge_thread = threading.Thread(target=purge)
                purge_thread.start()
                self.assertTrue(
                    delete_entered.wait(timeout=5), "purge never reached the delete"
                )

                enqueue_thread = threading.Thread(target=enqueue)
                enqueue_thread.start()
                self.assertTrue(enqueue_started.wait(timeout=5))
                # The enqueue must be parked on the namespace flush lock for
                # the whole cancel -> delete window.
                self.assertTrue(
                    enqueue_lock_attempted.wait(timeout=5),
                    "enqueue never attempted the namespace flush lock",
                )
                self.assertFalse(
                    enqueue_finished.is_set(),
                    "enqueue landed between the purge cancel and delete",
                )

                delete_release.set()
                purge_thread.join(timeout=5)
                enqueue_thread.join(timeout=5)

            self.assertFalse(purge_thread.is_alive())
            self.assertFalse(enqueue_thread.is_alive())

            # The (post-delete) enqueue was tombstoned, not applied.
            self.assertEqual(queued_edge_write_count(ns), 0)
            flush_edge_writes(ns)
            back = get_event_edges_backward_batch(ns, [victim])
            self.assertIsNone(back[victim])
            fwd = get_event_edges_forward_batch(ns, [parent])
            self.assertNotIn(victim, fwd.get(parent) or [])
        finally:
            delete_release.set()
            flush_edge_writes(ns)

    def test_purge_delete_failure_does_not_block_repair(self) -> None:
        """If the FFI delete fails the purge must not leave a permanent
        tombstone: the stale edges are still in mtxdb and a tombstone would
        suppress the repair writes that recover from the failure.  The
        cancelled rows from the purge are also restored so the original data
        remains reachable."""
        ns = "test-edges-purge-delete-fail"
        try:
            # A committed-but-unflushed row for the victim is cancelled ...
            queue_edge_write(ns, [(self.room_id, "$victim", "$parent", False)])

            with mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                side_effect=RuntimeError("simulated FFI delete failure"),
            ):
                with self.assertRaises(RuntimeError):
                    delete_event_edges_batch(ns, ["$victim"])

            # ... but no tombstone was recorded, so a repair/backfill write for
            # the same event is accepted rather than suppressed for the TTL.
            # The cancelled row was also restored, so we have 2 rows queued.
            queue_edge_write(ns, [(self.room_id, "$victim", "$repaired", False)])
            self.assertEqual(queued_edge_write_count(ns), 2)
            flush_edge_writes(ns)
            back = get_event_edges_backward_batch(ns, ["$victim"])
            # Both the restored original and the repair write are present:
            # the restored row keeps the original edge reachable, and the
            # repair write adds the new edge.
            self.assertIn(("$parent", False), back["$victim"])
            self.assertIn(("$repaired", False), back["$victim"])
        finally:
            flush_edge_writes(ns)

    def test_shutdown_drain_failure_retains_rows(self) -> None:
        """The shutdown drain retries on FFI failure, never drops rows, and
        close() still completes."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        ns = "test-edges-shutdown-fail"
        try:
            queue_edge_write(ns, [(self.room_id, "$s1", "$sp", False)])
            with mock.patch(
                "synapse.storage.databases.main.embedded_event_edges.put_event_edges_batch",
                side_effect=RuntimeError("simulated shutdown FFI failure"),
            ):
                with self.assertLogs(
                    "synapse.storage.databases.main.embedded_common", level="WARNING"
                ) as logs:
                    coalescer.close()
                self.assertTrue(
                    any("edge-write drain failed" in line for line in logs.output),
                    f"expected drain retry log, got: {logs.output}",
                )
            # Rows are retained after shutdown despite the failed drain.
            self.assertEqual(queued_edge_write_count(ns), 1)
        finally:
            _clear_coalescer(coalescer)
            flush_edge_writes()

    def test_edge_drain_failure_schedules_retry(self) -> None:
        """A failed timer drain retains rows and schedules a retry."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        ns = "test-edges-drain-retry"
        try:
            queue_edge_write(ns, [(self.room_id, "$retry", "$retry_parent", False)])

            with mock.patch(
                "synapse.storage.databases.main.embedded_event_edges.put_event_edges_batch",
                side_effect=RuntimeError("simulated timer FFI failure"),
            ):
                reactor.advance(FLUSH_DELAY_SECS + 0.1)

            self.assertEqual(queued_edge_write_count(ns), 1)
            self.assertIsNotNone(coalescer._delayed_call)

            reactor.advance(1.1)
            self.assertEqual(queued_edge_write_count(ns), 0)
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()
            flush_edge_writes()

    def test_purge_cancels_row_owned_by_purged_event(self) -> None:
        """A queued row whose event_id (row[1]) is purged is cancelled."""
        ns = "test-edges-purge-row1"
        victim = "$purged_owner"
        parent = "$owner_parent"
        try:
            queue_edge_write(ns, [(self.room_id, victim, parent, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            delete_event_edges_batch(ns, [victim])

            # victim is row[1] and purged → row cancelled.
            self.assertEqual(queued_edge_write_count(ns), 0)
        finally:
            flush_edge_writes(ns)

    def test_purge_delete_failure_restores_cancelled_rows(self) -> None:
        """When the FFI delete fails, only rows owned by purged events (row[1])
        are restored — live-child → purged-parent rows were never cancelled."""
        ns = "test-edges-purge-fail-restore"
        victim = "$fail_victim"
        parent = "$fail_parent"
        try:
            # Row owned by the purged event (row[1] = victim).
            queue_edge_write(ns, [(self.room_id, victim, parent, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            with mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                side_effect=RuntimeError("simulated FFI delete failure"),
            ):
                with self.assertRaises(RuntimeError):
                    delete_event_edges_batch(ns, [victim])

            # The cancelled row was restored to the queue for repair.
            self.assertEqual(queued_edge_write_count(ns), 1)
            flush_edge_writes(ns)
            back = get_event_edges_backward_batch(ns, [victim])
            self.assertIn((parent, False), back.get(victim) or [])
        finally:
            flush_edge_writes(ns)

    def test_restore_regression_row1_cancellation_and_timer_retry(self) -> None:
        """Regression: rows owned by purged events (row[1]) are cancelled,
        restored on delete failure, and later flushed by the coalescer timer.
        Live-child → purged-parent rows (backward extremities) survive purge."""
        ns = "test-edges-restore-regression"
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            # --- Cycle 1: successful purge cancels row[1]-owned row, ---
            # --- backward-extremity row (row[2] match) survives.     ---
            victim1 = "$reg_v1"
            parent1 = "$reg_p1"
            extremity1 = "$reg_ext1"
            queue_edge_write(
                ns,
                [
                    (
                        self.room_id,
                        victim1,
                        parent1,
                        False,
                    ),  # row[1] = victim → cancelled
                    (
                        self.room_id,
                        extremity1,
                        victim1,
                        False,
                    ),  # row[2] = victim → kept
                ],
            )
            self.assertEqual(queued_edge_write_count(ns), 2)

            delete_event_edges_batch(ns, [victim1])
            # The row-owned-by-victim1 was cancelled; the extremity was
            # remains queued for the normal coalescer.
            self.assertEqual(queued_edge_write_count(ns), 1)
            flush_edge_writes(ns)
            back_ext = get_event_edges_backward_batch(ns, [extremity1])
            self.assertEqual(back_ext[extremity1], [(victim1, False)])

            # --- Cycle 2: failed purge restores the cancelled row. ---
            victim2 = "$reg_v2"
            parent2 = "$reg_p2"
            queue_edge_write(ns, [(self.room_id, victim2, parent2, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            with mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                side_effect=RuntimeError("simulated delete failure"),
            ):
                with self.assertRaises(RuntimeError):
                    delete_event_edges_batch(ns, [victim2])

            # The cancelled row was restored; the timer was (re)scheduled.
            self.assertEqual(queued_edge_write_count(ns), 1)
            self.assertIsNotNone(coalescer._delayed_call)

            # Advance past the debounce — the coalescer drains the row.
            reactor.advance(FLUSH_DELAY_SECS + 0.1)
            self.assertEqual(queued_edge_write_count(ns), 0)

            back_v2 = get_event_edges_backward_batch(ns, [victim2])
            self.assertIn((parent2, False), back_v2.get(victim2) or [])
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()
            flush_edge_writes(ns)

    def test_two_namespaces_flush_independently(self) -> None:
        """Rows in namespace A are not flushed when namespace B is drained."""
        ns_a = "test-edges-ns-a"
        ns_b = "test-edges-ns-b"
        try:
            queue_edge_write(ns_a, [(self.room_id, "$a1", "$a0", False)])
            queue_edge_write(ns_b, [(self.room_id, "$b1", "$b0", False)])
            self.assertEqual(queued_edge_write_count(ns_a), 1)
            self.assertEqual(queued_edge_write_count(ns_b), 1)

            # Flush only ns_a.
            flush_edge_writes(ns_a)
            self.assertEqual(queued_edge_write_count(ns_a), 0)
            self.assertEqual(queued_edge_write_count(ns_b), 1)

            # ns_b's row is still there.
            back_a = get_event_edges_backward_batch(ns_a, ["$a1"])
            self.assertEqual(back_a["$a1"], [("$a0", False)])
            back_b = get_event_edges_backward_batch(ns_b, ["$b1"])
            self.assertIsNone(back_b["$b1"])
        finally:
            flush_edge_writes(ns_a)
            flush_edge_writes(ns_b)

    def test_edge_write_repairs_missing_legacy_locator(self) -> None:
        """Edges for events whose event_json locator was never mirrored are
        still resolvable: `event_edges_put` re-publishes locators."""
        ns = "test-edges-legacy-locator"
        put_event_edges_batch(ns, [(self.room_id, "$old", "$older", False)])
        back = get_event_edges_backward_batch(ns, ["$old"])
        self.assertEqual(back["$old"], [("$older", False)])
        fwd = get_event_edges_forward_batch(ns, ["$older"])
        self.assertEqual(fwd["$older"], ["$old"])


class EventEdgesStorageIntegrationTestCase(HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        # The coalescer window is auto-tuned to the store's backing disk
        # (2.0s when rotational) unless the config pinned it, so advance the
        # reactor by what this homeserver actually got instead of assuming
        # the module constant matches the runtime value.
        self._flush_delay_secs = (
            hs.config.database.embedded_db_flush_delay_secs or FLUSH_DELAY_SECS
        )
        # The process-global embedded FWD index is keyed by room_id, not by
        # `_embedded_db_namespace`, and each test here gets a distinct namespace
        # but deterministic create-event content. Register a unique creator so
        # the create event -- and therefore the room id -- differs per test and
        # the shared FWD meta/collections cannot leak between tests (see the
        # room_forward_meta key layout).
        nonce = uuid4().hex
        username = f"alice_{nonce}"
        password = f"test_{nonce}"

        self.user_id = self.register_user(username, password)
        self.tok = self.login(username, password)
        self.room_id = self.helper.create_room_as(
            room_creator=self.user_id, tok=self.tok
        )

        self.store._embedded_event_edges_enabled = True
        self.store._embedded_event_edges_writable = True
        self.store._embedded_db_engine = "mtxdb"
        if not getattr(self.store, "_embedded_db_namespace", None):
            self.store._embedded_db_namespace = hs.hostname

        self.persist_store = hs.get_datastores().persist_events
        if self.persist_store is not None:
            self.persist_store._embedded_event_edges_enabled = True
            self.persist_store._embedded_event_edges_writable = True
            self.persist_store._embedded_db_engine = "mtxdb"
            if not getattr(self.persist_store, "_embedded_db_namespace", None):
                self.persist_store._embedded_db_namespace = hs.hostname

    def tearDown(self) -> None:
        # The embedded edge engine is process-global (one mtxdb store per
        # process, one active flush coalescer). Drain the coalesced edge-write
        # queues and shut down this test's coalescer so queued rows, tombstones,
        # pending delayed flush calls, and dirty EVENT_DAG/EJSON pools cannot
        # leak into the next test and perturb its FWD source-version match.
        try:
            flush_edge_writes()
            embedded_common.close_coalescer()
        finally:
            super().tearDown()

    def _advance_past_flush_window(self) -> None:
        """Advance the reactor past the coalescer's actual debounce window."""
        self.reactor.advance(self._flush_delay_secs + 0.1)

    def test_event_edges_mirrored_on_persistence(self) -> None:
        """When events are persisted, event_edges are dual-written to mtxdb."""
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # Edge writes are coalesced in-process; drain the queue so the mirror
        # reflects them (in production the next flush / threshold / shutdown
        # would do this, but the test asserts immediately).
        flush_edge_writes(self.store._embedded_db_namespace)

        # e2 should have e1 in its backward edges in mtxdb
        backward = get_event_edges_backward_batch(
            self.store._embedded_db_namespace, [e2_id]
        )
        self.assertIsNotNone(backward[e2_id])
        prev_ids = [p for p, _ in backward[e2_id] or []]
        self.assertIn(e1_id, prev_ids)

        # e1 should have e2 in its forward successors in mtxdb
        successors = self.get_success(self.store.get_successor_events(e1_id))
        self.assertIn(e2_id, successors)

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_below_threshold_edge_committed_synchronously(self) -> None:
        """In authoritative MTXDB mode a persisted edge is committed to mtxdb
        synchronously in the persistence transaction's post-commit callback --
        it is already visible on the next read and is never left waiting in
        the coalescer, even below the batch threshold and with fsync disabled
        (`no_sync`)."""
        ns = self.store._embedded_db_namespace
        # Force the no-sync path so the test proves the edge commit is not
        # coupled to whether the sync coalescer actually fsyncs.
        previous_no_sync = embedded_common._sync_disabled
        embedded_common.configure_sync(no_sync=True)
        try:
            # Drain anything earlier tests left queued so this test controls
            # queue content.
            flush_edge_writes(ns)
            with enable_ffi_counting():
                res = self.helper.send(self.room_id, "ring", tok=self.tok)
                e_id = res["event_id"]

                # Committed synchronously; nothing is left queued.
                self.assertGreater(get_ffi_count("event_edges_put_rows"), 0)
                self.assertEqual(
                    queued_edge_write_count(ns),
                    0,
                    "authoritative writes must not leave the edge queued",
                )

            backward = get_event_edges_backward_batch(ns, [e_id])
            self.assertIsNotNone(
                backward[e_id],
                "edge should already be visible in mtxdb",
            )
            prev_ids = [p for p, _ in backward[e_id] or []]
            self.assertTrue(prev_ids)
        finally:
            embedded_common.configure_sync(no_sync=previous_no_sync)

    def test_event_edges_purge_cleans_forward_edges(self) -> None:
        """Purging an event removes it from its parents' forward lists in mtxdb."""
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # Drain the coalesced edge writes so the mirror reflects both sends.
        flush_edge_writes(self.store._embedded_db_namespace)

        # Before deletion: e1 has e2 as forward successor
        fwd_before = get_event_edges_forward_batch(
            self.store._embedded_db_namespace, [e1_id]
        )
        self.assertIn(e2_id, fwd_before.get(e1_id) or [])

        # Purge e2
        delete_event_edges_batch(self.store._embedded_db_namespace, [e2_id])

        # After deletion: e2 is tombstoned in backward edges
        back_after = get_event_edges_backward_batch(
            self.store._embedded_db_namespace, [e2_id]
        )
        self.assertIsNone(back_after[e2_id])

        # And e1's forward edges no longer contain e2
        fwd_after = get_event_edges_forward_batch(
            self.store._embedded_db_namespace, [e1_id]
        )
        self.assertNotIn(e2_id, fwd_after.get(e1_id) or [])

    def test_rollback_leaves_mtxdb_unmodified(self) -> None:
        """A rolled back SQL transaction does not write orphaned edges to mtxdb."""
        from unittest.mock import Mock

        from synapse.storage.database import LoggingTransaction

        res1 = self.helper.send(self.room_id, "base_event", tok=self.tok)
        e1_id = res1["event_id"]

        fake_id = f"$aborted_{self.clock.time()}:test"
        mock_ev = Mock(
            room_id=self.room_id, event_id=fake_id, prev_event_ids=lambda: [e1_id]
        )

        persist_store = self.persist_store
        assert persist_store is not None

        # Commit the event row separately. The transaction under test should
        # roll back the edge insert, not fail its foreign-key check because
        # the fixture row was inserted in the same interaction.
        self.get_success(
            self.store.db_pool.simple_insert(
                table="events",
                values={
                    "instance_name": "master",
                    "stream_ordering": 999999,
                    "topological_ordering": 1,
                    "depth": 1,
                    "event_id": fake_id,
                    "room_id": self.room_id,
                    "type": "m.room.message",
                    "processed": True,
                    "outlier": False,
                    "origin_server_ts": int(self.clock.time_msec()),
                    "received_ts": int(self.clock.time_msec()),
                    "sender": self.user_id,
                    "contains_url": False,
                },
            )
        )

        def bad_txn(txn: LoggingTransaction) -> None:
            persist_store._handle_mult_prev_events(txn, [mock_ev])
            raise RuntimeError("simulated transaction failure")

        failure = self.get_failure(
            self.store.db_pool.runInteraction("test_abort", bad_txn),
            RuntimeError,
        )
        self.assertEqual(str(failure.value), "simulated transaction failure")

        # Check mtxdb: fake_id is NOT in mtxdb backward edges
        back = get_event_edges_backward_batch(
            self.store._embedded_db_namespace, [fake_id]
        )
        self.assertIsNone(back[fake_id])

        # And e1_id's forward edges do NOT contain fake_id
        fwd = get_event_edges_forward_batch(self.store._embedded_db_namespace, [e1_id])
        self.assertNotIn(fake_id, fwd.get(e1_id) or [])

    def test_read_only_worker_reads_edges(self) -> None:
        """A read-only worker can read edges from mtxdb without failing assert_writable."""
        res1 = self.helper.send(self.room_id, "msg1", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "msg2", tok=self.tok)
        e2_id = res2["event_id"]

        # Simulate read-only worker
        self.store._embedded_event_edges_writable = False
        self.store._embedded_event_edges_enabled = True

        successors = self.get_success(self.store.get_successor_events(e1_id))
        self.assertIn(e2_id, successors)

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_sql_fallback_and_repair_on_missing_mtxdb_edges(self) -> None:
        """SQL fallback returns edges missing from mtxdb and repairs the local store.

        This is a same-process test: it verifies that after the coalesced flush
        fires, the same in-process mtxdb handle reflects the repaired edge when
        read through the read-only (non-writable) code path.  It does NOT verify
        cross-worker or cross-process visibility; that requires a separate
        read-only engine handle or a genuine multi-process integration test.

        The metric assertions confirm that the final read-only query was served
        from the embedded store (hit counter up, fallback counter flat) rather
        than silently succeeding through the SQL fallback path.
        """
        res1 = self.helper.send(self.room_id, "parent", tok=self.tok)
        p_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "child", tok=self.tok)
        c_id = res2["event_id"]

        ns = self.store._embedded_db_namespace
        # Persist the coalesced edges, then simulate a post-commit mirror write
        # that never landed by deleting the edge directly at the FFI layer.
        # Deliberately NOT `delete_event_edges_batch`: that records a purge
        # tombstone and this scenario is a lost write, not a purge.  (The purge
        # tombstone + SQL-fallback repair case is `test_sql_fallback_repairs_preserved_row2_edge_despite_purge`.)
        flush_edge_writes(ns)
        mtxdb_engine.event_edges_delete(ns, [c_id])

        # Verify mtxdb has no forward edge for p_id
        fwd_miss = get_event_edges_forward_batch(ns, [p_id])
        self.assertIsNone(fwd_miss.get(p_id))

        # Querying successor events falls back to SQL, successfully finding c_id,
        # and queues a repair of the mtxdb forward edge.  The read is served
        # by SQL; the repair only lands in mtxdb on the next coalesced flush.
        successors = self.get_success(self.store.get_successor_events(p_id))
        self.assertIn(c_id, successors)

        # Advance the reactor past the flush window so the coalescer drains
        # the queued repair (and syncs EVENT_DAG).  Use FLUSH_DELAY_SECS plus
        # a small margin so the test is not fragile to floating-point
        # boundaries or minor changes to the delay value.
        self._advance_past_flush_window()

        # Now mtxdb has been repaired in-process.
        fwd_repaired = get_event_edges_forward_batch(ns, [p_id])
        self.assertIn(c_id, fwd_repaired.get(p_id) or [])

        # Exercise the read-only code path after the coalesced flush.
        # writable=False routes through the embedded read path (no SQL write).
        # enable_ffi_counting() activates counters only for this block so the
        # assertion is isolated from any other test activity and does not add
        # overhead to production deployments where diagnostics are disabled.
        self.store._embedded_event_edges_writable = False
        with enable_ffi_counting():
            reader_successors = self.get_success(self.store.get_successor_events(p_id))
            self.assertIn(c_id, reader_successors)

            # The query must have been an embedded hit, not a SQL fallback.
            self.assertEqual(
                get_ffi_count("event_edges_successor_hits"),
                1,
                "expected exactly one embedded hit for the read-only query",
            )
            self.assertEqual(
                get_ffi_count("event_edges_successor_fallbacks"),
                0,
                "expected no SQL fallback for the read-only query after repair",
            )

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_sql_fallback_repairs_preserved_row2_edge_despite_purge(self) -> None:
        """SQL keeps the live-child → purged-parent `event_edges` row (a
        backward extremity).  Even after a genuine purge tombstoned the parent,
        the SQL fallback still returns that edge and the repair enqueue is
        accepted: the tombstone is owner-based, so it only blocks rows whose
        own event_id is purged -- the child is not.  Without the owner-based
        filter this repair would be suppressed for the tombstone's TTL."""
        res1 = self.helper.send(self.room_id, "parent", tok=self.tok)
        p_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "child", tok=self.tok)
        c_id = res2["event_id"]

        ns = self.store._embedded_db_namespace
        flush_edge_writes(ns)

        # True purge of the parent: records an owner-based tombstone for p_id.
        delete_event_edges_batch(ns, [p_id])

        # Simulate the child's forward edge write never landing in mtxdb.
        mtxdb_engine.event_edges_delete(ns, [c_id])
        fwd_miss = get_event_edges_forward_batch(ns, [p_id])
        self.assertIsNone(fwd_miss.get(p_id))

        # get_successor_events falls back to the preserved SQL row and queues
        # the repair (child, purged parent).  The parent's tombstone must not
        # suppress it.
        successors = self.get_success(self.store.get_successor_events(p_id))
        self.assertIn(c_id, successors)

        self._advance_past_flush_window()

        # The preserved row[2] edge is repaired in mtxdb despite the tombstone.
        fwd_repaired = get_event_edges_forward_batch(ns, [p_id])
        self.assertIn(c_id, fwd_repaired.get(p_id) or [])

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_outbox_recovers_edge_publication_after_simulated_crash(self) -> None:
        """Crash between the SQL commit and the post-commit mtxdb edge write.

        The SQL transaction commits the event, its authoritative `event_edges`
        row, and a durable `edge_index_outbox` row. If the process dies before
        the post-commit mtxdb write lands, the publication worker must recover
        the forward index from the outbox, and it must acknowledge (delete) the
        outbox rows only after the mtxdb commit -- never before.
        """
        from synapse.storage.database import LoggingTransaction

        ns = self.store._embedded_db_namespace
        room_id = self.room_id

        def _sql_scalar(query: str, args: tuple[Any, ...]) -> Any:
            def _txn(txn: LoggingTransaction) -> Any:
                txn.execute(query, args)
                row = txn.fetchone()
                assert row is not None
                return row[0]

            return self.get_success(
                self.store.db_pool.runInteraction("crash_injection_sql", _txn)
            )

        outbox_count_query = "SELECT COUNT(*) FROM edge_index_outbox WHERE room_id = ?"

        with (
            # Model the crash: the post-commit mtxdb write is attempted, but it
            # never lands. The SQL transaction still commits the event and its
            # durable outbox row.
            mock.patch(
                "synapse.storage.databases.main.events.put_event_edges_batch"
            ) as direct_write,
            # Keep the background publication loop from draining the outbox
            # before the test has inspected it; the real drainer is invoked
            # explicitly below.
            mock.patch(
                "synapse.storage.databases.main.events_worker.drain_edge_index_outbox"
            ),
        ):
            res1 = self.helper.send(room_id, "parent", tok=self.tok)
            p_id = res1["event_id"]
            res2 = self.helper.send(room_id, "child", tok=self.tok)
            c_id = res2["event_id"]

            self.assertTrue(
                direct_write.called,
                "the post-commit mtxdb write should have been attempted",
            )

            # Step 1: SQL is authoritative and the outbox row is durable.
            self.assertEqual(
                _sql_scalar(
                    "SELECT COUNT(*) FROM event_edges "
                    "WHERE event_id = ? AND prev_event_id = ?",
                    (c_id, p_id),
                ),
                1,
                "the authoritative SQL edge must be committed",
            )
            self.assertGreater(
                _sql_scalar(outbox_count_query, (room_id,)),
                0,
                "the event transaction must commit a durable outbox row",
            )

            source_version = int(
                _sql_scalar(
                    "SELECT source_version FROM room_edge_source_version "
                    "WHERE room_id = ?",
                    (room_id,),
                )
            )

            # Step 2: the crash left mtxdb unpublished -- the gated read is a
            # version mismatch, not a (possibly partial) hit.
            self.assertEqual(
                get_event_edges_forward_batch(ns, room_id, source_version, [p_id]),
                {},
                "an unpublished forward index must not be served as a hit",
            )

            # Step 3: run the publication worker.
            self.assertTrue(
                self.get_success(
                    drain_edge_index_outbox(self.store, namespace=ns, room_id=room_id)
                ),
                "the drainer should have had outbox rows to apply",
            )

            # Step 4: the forward index is now complete and published, and only
            # now have the outbox rows been acknowledged.
            self.assertIn(
                c_id,
                (
                    get_event_edges_forward_batch(
                        ns, room_id, source_version, [p_id]
                    ).get(p_id)
                    or []
                ),
                "the drainer must publish the pending forward edge",
            )
            self.assertEqual(
                _sql_scalar(outbox_count_query, (room_id,)),
                0,
                "outbox rows must be acknowledged only after mtxdb publication",
            )

        # The gated production read path now serves a complete, published hit
        # rather than falling back to SQL.
        with enable_ffi_counting():
            successors = self.get_success(self.store.get_successor_events(p_id))
            self.assertIn(c_id, successors)
            self.assertEqual(
                get_ffi_count("event_edges_successor_hits"),
                1,
                "expected the recovered read to be an embedded hit",
            )
            self.assertEqual(
                get_ffi_count("event_edges_successor_fallbacks"),
                0,
                "expected no SQL fallback after the outbox drained",
            )

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_migrate_mtxdb_background_update_is_retired_noop(self) -> None:
        """The online `event_edges_migrate_mtxdb` background update is retired:
        FWD authority now requires a stopped-writer offline rebuild, so driving
        it to completion must NOT mirror legacy SQL-only rows into mtxdb. Those
        rows stay served from SQL until the offline rebuild runs."""
        ns = self.store._embedded_db_namespace
        assert self.persist_store is not None

        # SQL-only: disable the mirror before these events persist, so the
        # only place this edge exists afterward is the SQL event_edges row,
        # exactly like a pre-existing server's history.
        self.persist_store._embedded_event_edges_writable = False
        res1 = self.helper.send(self.room_id, "legacy-parent", tok=self.tok)
        p_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "legacy-child", tok=self.tok)
        c_id = res2["event_id"]
        self.persist_store._embedded_event_edges_writable = True

        flush_edge_writes(ns)
        self.assertIsNone(
            get_event_edges_backward_batch(ns, [c_id]).get(c_id),
            "the mirror-disabled write must not have reached mtxdb",
        )
        self.assertIsNone(
            get_event_edges_forward_batch(ns, [p_id]).get(p_id),
            "the mirror-disabled write must not have reached mtxdb",
        )

        # Re-register the background update (a fresh test DB already marks it
        # complete) and drive it to completion, the same pattern
        # test_events_bg_updates.py uses.
        self.get_success(
            self.store.db_pool.simple_insert(
                table="background_updates",
                values={
                    "update_name": "event_edges_migrate_mtxdb",
                    "progress_json": "{}",
                },
            )
        )
        # has_completed_background_updates() caches _all_done=True forever
        # once it's observed an empty table -- which it already has by now,
        # via reactor pumps inside the register_user/login/send calls above.
        # Reset it so wait_for_background_updates() re-checks the DB instead
        # of trusting the stale cache (test_events_bg_updates.py's
        # TestRedactionsRecheckBgUpdate does the same for the same reason).
        self.store.db_pool.updates._all_done = False
        self.wait_for_background_updates()

        # The legacy edge is still in SQL, and the SQL fallback keeps serving
        # it.
        sql_prev = self.get_success(
            self.store.db_pool.simple_select_onecol(
                table="event_edges",
                keyvalues={"event_id": c_id},
                retcol="prev_event_id",
                desc="legacy_event_edges_prev",
            )
        )
        self.assertIn(p_id, sql_prev, "legacy event_edges row must remain in SQL")

        # ...but the retired online migration must not publish it to mtxdb;
        # only the offline rebuild does that.
        self.assertIsNone(
            get_event_edges_backward_batch(ns, [c_id]).get(c_id),
            "the retired online migration must not mirror legacy rows",
        )
        self.assertIsNone(
            get_event_edges_forward_batch(ns, [p_id]).get(p_id),
            "the retired online migration must not mirror legacy rows",
        )

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_get_successor_events_gates_on_mtxdb_migration_completion(self) -> None:
        """A parent with two children -- one legacy (SQL-only, predates the
        mirror), one mirrored (a live post-enable write) -- gives mtxdb a
        forward list that is non-empty but *incomplete*: `[mirrored_child]`,
        missing `legacy_child`. `get_successor_events`'s fallback is
        miss-triggered (`successors is not None`), so it cannot detect a
        partial hit as anything other than a hit. Confirms it does not try:
        while `event_edges_migrate_mtxdb` is incomplete, both children must
        come back (the all-SQL path, gated shut); once complete, both must
        still come back (mtxdb now has both, backfill covered the legacy
        one)."""
        ns = self.store._embedded_db_namespace

        parent_res = self.helper.send(self.room_id, "gate-parent", tok=self.tok)
        parent_id = parent_res["event_id"]

        # Two children of the same parent, inserted directly (a real forked
        # DAG needs auth-event plumbing this test doesn't need): one SQL-only
        # (the legacy shape), one also mirrored (the live post-enable shape).
        legacy_child = "$gate-legacy-child:test"
        mirrored_child = "$gate-mirrored-child:test"
        for child_id in (legacy_child, mirrored_child):
            self.get_success(
                self.store.db_pool.simple_insert(
                    table="events",
                    values={
                        "event_id": child_id,
                        "room_id": self.room_id,
                        "topological_ordering": 1,
                        "depth": 1,
                        "type": "m.test",
                        "sender": self.user_id,
                        "processed": True,
                        "outlier": False,
                    },
                )
            )
            self.get_success(
                self.store.db_pool.simple_insert(
                    table="event_edges",
                    values={"event_id": child_id, "prev_event_id": parent_id},
                )
            )
        put_event_edges_batch(ns, [(self.room_id, mirrored_child, parent_id, False)])

        # Sanity: mtxdb's forward list for parent_id is now a real hit, but
        # an incomplete one -- exactly the shape the gate exists for.
        raw_forward = get_event_edges_forward_batch(ns, [parent_id])
        self.assertEqual(raw_forward.get(parent_id), [mirrored_child])

        # event_edges_migrate_mtxdb is not registered as pending in a fresh
        # test DB (it's marked complete, like every other background update),
        # so has_completed_background_update already reports True. Reinsert
        # it and reset the cache to actually exercise the gate being closed
        # (test_migrate_mtxdb_background_update_mirrors_legacy_rows's idiom).
        self.get_success(
            self.store.db_pool.simple_insert(
                table="background_updates",
                values={
                    "update_name": "event_edges_migrate_mtxdb",
                    "progress_json": "{}",
                },
            )
        )
        self.store.db_pool.updates._all_done = False
        self.store.db_pool.updates._completed_background_updates.discard(
            "event_edges_migrate_mtxdb"
        )

        # Gate closed: must take the all-SQL path and return both children,
        # not the incomplete mtxdb list.
        successors = self.get_success(self.store.get_successor_events(parent_id))
        self.assertEqual(
            set(successors),
            {legacy_child, mirrored_child},
            "gate closed: must not trust the incomplete mtxdb hit",
        )

        # Drive the background update to completion: it mirrors the legacy
        # child too, so mtxdb's forward list becomes complete.
        self.wait_for_background_updates()

        successors_after = self.get_success(self.store.get_successor_events(parent_id))
        self.assertEqual(
            set(successors_after),
            {legacy_child, mirrored_child},
            "gate open: mtxdb must now be complete (backfill mirrored the "
            "legacy child), so the hit is correct",
        )

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_is_event_next_to_forward_gap_gates_on_mtxdb_migration_completion(
        self,
    ) -> None:
        """Same partial-hit shape as
        `test_get_successor_events_gates_on_mtxdb_migration_completion`, for
        `is_event_next_to_forward_gap`'s independent gate
        (`embedded_edges_trustworthy`, resolved before `runInteraction` since
        its txn closure is sync). `parent` has a real (non-rejected) child,
        so it must never be reported as a forward gap, gate open or closed --
        the gate exists so an incomplete mtxdb hit can't override that by
        claiming `children == []` when the mirror simply hasn't caught up."""
        ns = self.store._embedded_db_namespace

        parent_res = self.helper.send(self.room_id, "fg-gate-parent", tok=self.tok)
        parent_id = parent_res["event_id"]
        parent_event = self.get_success(self.store.get_event(parent_id))

        legacy_child = "$fg-gate-legacy-child:test"
        mirrored_child = "$fg-gate-mirrored-child:test"
        for child_id in (legacy_child, mirrored_child):
            self.get_success(
                self.store.db_pool.simple_insert(
                    table="events",
                    values={
                        "event_id": child_id,
                        "room_id": self.room_id,
                        "topological_ordering": 1,
                        "depth": 1,
                        "type": "m.test",
                        "sender": self.user_id,
                        "processed": True,
                        "outlier": False,
                    },
                )
            )
            self.get_success(
                self.store.db_pool.simple_insert(
                    table="event_edges",
                    values={"event_id": child_id, "prev_event_id": parent_id},
                )
            )
        put_event_edges_batch(ns, [(self.room_id, mirrored_child, parent_id, False)])

        self.get_success(
            self.store.db_pool.simple_insert(
                table="background_updates",
                values={
                    "update_name": "event_edges_migrate_mtxdb",
                    "progress_json": "{}",
                },
            )
        )
        self.store.db_pool.updates._all_done = False
        self.store.db_pool.updates._completed_background_updates.discard(
            "event_edges_migrate_mtxdb"
        )

        self.assertFalse(
            self.get_success(self.store.is_event_next_to_forward_gap(parent_event)),
            "gate closed: a real child exists, must not be reported as a gap",
        )

        self.wait_for_background_updates()

        self.assertFalse(
            self.get_success(self.store.is_event_next_to_forward_gap(parent_event)),
            "gate open: still a real child, still not a gap",
        )


class EventEdgesMigrationGateTestCase(EventEdgesStorageIntegrationTestCase):
    """`check_event_edges_migration_complete`: Blocker 1's hard precondition
    for the (not-yet-shipped) release that removes the SQL `event_edges`
    insert -- `res/docs/2026-09-28-event-edges-sql-removal-plan.md`.

    Reuses `EventEdgesStorageIntegrationTestCase.prepare` for a homeserver
    with the embedded edges engine enabled and writable.
    """

    def _make_migration_incomplete(self) -> None:
        """Reinsert `event_edges_migrate_mtxdb` as pending, matching the
        idiom in `test_migrate_mtxdb_background_update_is_retired_noop`."""
        self.get_success(
            self.store.db_pool.simple_insert(
                table="background_updates",
                values={
                    "update_name": "event_edges_migrate_mtxdb",
                    "progress_json": "{}",
                },
            )
        )
        self.store.db_pool.updates._all_done = False
        self.store.db_pool.updates._completed_background_updates.discard(
            "event_edges_migrate_mtxdb"
        )

    def test_no_op_while_flag_false(self) -> None:
        """`EVENT_EDGES_SQL_INSERT_REMOVED` is False today: the check must
        never raise, regardless of migration state, since there is nothing
        to protect yet (the SQL fallback still exists)."""
        self._make_migration_incomplete()

        self.get_success(check_event_edges_migration_complete(self.hs))

    @mock.patch.object(
        embedded_event_edges_module, "EVENT_EDGES_SQL_INSERT_REMOVED", True
    )
    def test_no_raise_when_writer_and_migration_complete(self) -> None:
        """With the flag on and the migration already complete (the default
        state of a fresh test homeserver -- every background update is
        marked done), the check must be silent."""
        self.get_success(check_event_edges_migration_complete(self.hs))

    @mock.patch.object(
        embedded_event_edges_module, "EVENT_EDGES_SQL_INSERT_REMOVED", True
    )
    def test_no_op_for_non_writer_process(self) -> None:
        """With the flag on and the migration incomplete, a process that
        isn't the events-stream writer must not be blocked -- it never
        writes event_edges itself, so it has nothing to protect against.
        `embedded_event_edges_is_writable` is config-derived (not a store
        attribute), so it's mocked directly rather than mutated on the
        store."""
        self._make_migration_incomplete()

        with mock.patch.object(
            embedded_event_edges_module,
            "embedded_event_edges_is_writable",
            return_value=False,
        ):
            self.get_success(check_event_edges_migration_complete(self.hs))
