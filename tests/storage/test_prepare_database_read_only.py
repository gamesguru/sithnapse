#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
import io
import sqlite3
from unittest import SkipTest

from twisted.internet.testing import MemoryReactor

from synapse.server import HomeServer
from synapse.storage.database import LoggingDatabaseConnection
from synapse.storage.databases.main import check_retired_migrations_before_upgrade
from synapse.storage.engines import PostgresEngine
from synapse.storage.prepare_database import (
    PrepareDatabaseException,
    _apply_module_schema_files,
    prepare_database,
)
from synapse.util.clock import Clock

from tests.unittest import HomeserverTestCase

_DELTA = "97/06_server_key_response_blobs.sql"


class ReadOnlyPrepareDatabaseTests(HomeserverTestCase):
    """`read_only=True` must refuse to migrate rather than apply deltas."""

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.db_pool = hs.get_datastores().main.db_pool
        self.db_conn = LoggingDatabaseConnection(
            conn=self.db_pool._db_pool.connect(),
            engine=self.db_pool.engine,
            default_txn_name="tests",
            server_name="test_server",
        )

    def _forget_delta(self) -> None:
        cur = self.db_conn.cursor()
        cur.execute("DELETE FROM applied_schema_deltas WHERE file = ?", (_DELTA,))
        self.db_conn.commit()

    def _delta_applied(self) -> bool:
        cur = self.db_conn.cursor()
        cur.execute("SELECT 1 FROM applied_schema_deltas WHERE file = ?", (_DELTA,))
        found = cur.fetchone() is not None
        self.db_conn.commit()
        return found

    def test_current_schema_passes(self) -> None:
        prepare_database(
            self.db_conn, self.db_pool.engine, self.hs.config, read_only=True
        )

    def test_pending_retired_embedded_migration_is_refused(self) -> None:
        cur = self.db_conn.cursor()
        cur.execute(
            "INSERT INTO background_updates (update_name, progress_json) VALUES (?, ?)",
            ("state_hamt_embedded_migration", "{}"),
        )
        self.db_conn.commit()

        with self.assertRaisesRegex(
            RuntimeError, "embedded state migration is incomplete"
        ):
            prepare_database(
                self.db_conn, self.db_pool.engine, self.hs.config, read_only=True
            )

    def test_pending_delta_is_refused_not_applied(self) -> None:
        self._forget_delta()
        self.assertFalse(self._delta_applied())

        with self.assertRaises(PrepareDatabaseException):
            prepare_database(
                self.db_conn, self.db_pool.engine, self.hs.config, read_only=True
            )

        # It was neither applied nor recorded as applied.
        self.assertFalse(self._delta_applied())

    def test_pending_retired_migration_is_refused_for_the_state_database(self) -> None:
        """The state database has its own queue, so the guard must also run when
        this physical database holds only the state schema."""
        cur = self.db_conn.cursor()
        cur.execute(
            "INSERT INTO background_updates (update_name, progress_json) VALUES (?, ?)",
            ("state_hamt_embedded_migration", "{}"),
        )
        self.db_conn.commit()

        with self.assertRaisesRegex(
            RuntimeError, "embedded state migration is incomplete"
        ):
            prepare_database(
                self.db_conn,
                self.db_pool.engine,
                self.hs.config,
                databases=["state"],
                read_only=True,
            )

    def test_read_only_refuses_unapplied_module_schema(self) -> None:
        cur = self.db_conn.cursor()
        with self.assertRaises(PrepareDatabaseException):
            _apply_module_schema_files(
                cur,
                self.db_pool.engine,
                "tests.module",
                [("new_schema.sql", io.StringIO("CREATE TABLE should_not_exist(x);"))],
                read_only=True,
            )

        # Refused before executing anything.
        cur.execute(
            "SELECT 1 FROM applied_module_schemas WHERE module_name = ?",
            ("tests.module",),
        )
        self.assertIsNone(cur.fetchone())
        self.db_conn.commit()

    def test_read_only_allows_already_applied_module_schema(self) -> None:
        cur = self.db_conn.cursor()
        cur.execute(
            "INSERT INTO applied_module_schemas (module_name, file) VALUES (?, ?)",
            ("tests.module", "old_schema.sql"),
        )
        _apply_module_schema_files(
            cur,
            self.db_pool.engine,
            "tests.module",
            [("old_schema.sql", io.StringIO("CREATE TABLE should_not_exist(x);"))],
            read_only=True,
        )
        self.db_conn.commit()

    def test_retired_migration_check_tolerates_a_missing_queue_table(self) -> None:
        """A database that predates `background_updates` cannot have a pending
        retired migration, so the guard must not fail on the missing table."""
        if isinstance(self.db_pool.engine, PostgresEngine):
            raise SkipTest("uses a bare sqlite database")

        conn = sqlite3.connect(":memory:")
        try:
            check_retired_migrations_before_upgrade(conn.cursor(), self.db_pool.engine)
        finally:
            conn.close()
