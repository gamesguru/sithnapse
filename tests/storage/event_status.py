"""Checking mtxdb event statuses against the SQL rows they stand in for."""

from synapse.server import HomeServer
from synapse.storage.database import LoggingTransaction
from synapse.storage.databases.main.embedded_event_status import get_event_statuses

from tests.unittest import HomeserverTestCase


def assert_statuses_match_sql(
    test: HomeserverTestCase, hs: HomeServer, room_id: str
) -> None:
    """Every event stored for `room_id` has a status agreeing with SQL on
    whether it is an outlier and whether it is rejected."""

    def get_rows_txn(txn: LoggingTransaction) -> dict[str, tuple[bool, bool]]:
        txn.execute(
            "SELECT e.event_id, e.outlier, r.event_id IS NOT NULL FROM events AS e"
            " LEFT JOIN rejections AS r USING (event_id) WHERE e.room_id = ?",
            (room_id,),
        )
        return {
            event_id: (bool(outlier), bool(rejected))
            for event_id, outlier, rejected in txn
        }

    expected = test.get_success(
        hs.get_datastores().main.db_pool.runInteraction("get_status_rows", get_rows_txn)
    )
    test.assertTrue(expected)
    actual = {
        event_id: (status.outlier, status.rejected)
        for event_id, status in get_event_statuses(list(expected)).items()
    }
    test.assertEqual(actual, expected)
