#!/usr/bin/env python3
"""Populate the export-data CI mtxdb with adjacency from the SQLite fixture."""

from __future__ import annotations

import argparse
import logging
from typing import cast

import yaml

from twisted.internet import defer, reactor as reactor_

from synapse.config.homeserver import HomeServerConfig
from synapse.server import HomeServer
from synapse.storage import DataStore
from synapse.storage.database import LoggingTransaction
from synapse.storage.databases.main.embedded_event_edges import put_event_edges_batch
from synapse.types import ISynapseReactor

reactor = cast(ISynapseReactor, reactor_)
logger = logging.getLogger("synapse_prepare_export_data_mtxdb")


class MigrationHomeserver(HomeServer):
    DATASTORE_CLASS = DataStore

    def __init__(self, config: HomeServerConfig):
        super().__init__(
            hostname=config.server.server_name, config=config, reactor=reactor
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Export config with mtxdb")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    with open(args.config, encoding="utf-8") as config_file:
        config = HomeServerConfig()
        config.parse_config_dict(yaml.safe_load(config_file), "", "")

    hs = MigrationHomeserver(config)
    hs.setup()
    hs.get_storage_controllers()
    store = hs.get_datastores().main
    exit_code = 0

    async def run() -> None:
        nonlocal exit_code
        try:

            def read_event_edges(
                txn: LoggingTransaction,
            ) -> list[tuple[str, str, str | None]]:
                # The fixture's SQL event_edges table is the source of truth for
                # the old database. Include childless events so mtxdb records
                # roots as well as events with predecessors.
                txn.execute(
                    """
                    SELECT events.room_id, events.event_id, event_edges.prev_event_id
                    FROM events
                    LEFT JOIN event_edges USING (event_id)
                    ORDER BY events.room_id, events.event_id
                    """
                )
                return cast(list[tuple[str, str, str | None]], txn.fetchall())

            rows = await store.db_pool.runInteraction(
                "prepare_export_data_mtxdb_read_edges", read_event_edges
            )
            put_event_edges_batch(rows, sync=True)
            logger.info("Imported mtxdb adjacency for %d edge rows", len(rows))
        except Exception:
            logger.exception("Could not prepare the export-data mtxdb fixture")
            exit_code = 1
        finally:
            reactor.stop()

    hs.get_clock().call_when_running(lambda: defer.ensureDeferred(run()))
    reactor.run()
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
