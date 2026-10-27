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

"""Move event edges between the SQL table and mtxdb.

Stop Synapse before running this. The configured backend is the destination;
the `edge_backend` marker is the source. The marker is flipped only after the
copy has been verified, so a failed run leaves the previous backend
authoritative.
"""

import argparse
import json
import logging
from typing import cast

import yaml

from twisted.internet import defer, reactor as reactor_

from synapse.config.homeserver import HomeServerConfig
from synapse.server import HomeServer
from synapse.storage import DataStore
from synapse.storage.databases.main.embedded_event_edges import migrate_event_edges
from synapse.types import ISynapseReactor

reactor = cast(ISynapseReactor, reactor_)
logger = logging.getLogger("synapse_event_edges_migrate")


class MigrateHomeserver(HomeServer):
    DATASTORE_CLASS = DataStore

    def __init__(self, config: HomeServerConfig):
        super().__init__(
            hostname=config.server.server_name,
            config=config,
            reactor=reactor,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate event edges between the SQL event_edges table and mtxdb. "
            "Stop Synapse first."
        )
    )
    parser.add_argument("-v", action="store_true")
    parser.add_argument(
        "--config",
        type=argparse.FileType("r"),
        required=True,
        help="Synapse homeserver config, configured for the destination backend.",
    )
    parser.add_argument(
        "--to",
        choices=("sql", "mtxdb"),
        required=True,
        help="Backend that should hold the edges after migration.",
    )
    parser.add_argument(
        "--from",
        dest="source",
        choices=("sql", "mtxdb"),
        help=(
            "Backend that currently holds the edges. Defaults to the stored "
            "edge_backend marker; use this when the marker is stale."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow an empty source to overwrite a non-empty target.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Events read per batch (default: 1000).",
    )
    parser.add_argument(
        "--mtxdb-path",
        help=(
            "mtxdb directory, if it is not in the config. Required when "
            "migrating to SQL and the config no longer has embedded_db.path."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.v else logging.INFO,
        format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
    )

    hs_config = yaml.safe_load(args.config)
    config = HomeServerConfig()
    config.parse_config_dict(hs_config, "", "")

    hs = MigrateHomeserver(config)
    hs.setup()
    hs.get_storage_controllers()

    exit_code = 0

    async def run() -> None:
        nonlocal exit_code
        try:
            result = await migrate_event_edges(
                hs,
                target=args.to,
                source=args.source,
                force=args.force,
                batch_size=args.batch_size,
                mtxdb_path=args.mtxdb_path,
            )
            print(json.dumps(result, indent=2, sort_keys=True))
        except Exception:
            logger.exception("Event-edges migration failed")
            exit_code = 1
        finally:
            reactor.stop()

    hs.get_clock().call_when_running(lambda: defer.ensureDeferred(run()))
    reactor.run()
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
