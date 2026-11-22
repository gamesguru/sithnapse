#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
from twisted.internet.testing import MemoryReactor

from synapse.server import HomeServer
from synapse.storage.database import LoggingDatabaseConnection
from synapse.storage.prepare_database import PrepareDatabaseException, prepare_database
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
