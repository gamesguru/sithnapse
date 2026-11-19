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

"""Offline migration of auth edges from SQL to mtxdb. Stop Synapse first."""

import argparse
import json
import logging
from typing import cast

import yaml

from twisted.internet import defer, reactor as reactor_

from synapse.config.homeserver import HomeServerConfig
from synapse.server import HomeServer
from synapse.storage import DataStore
from synapse.storage.databases.main.embedded_event_auth_chains import (
    migrate_auth_chain_edges,
)
from synapse.types import ISynapseReactor

reactor = cast(ISynapseReactor, reactor_)
logger = logging.getLogger("synapse_auth_edges_migrate")


class MigrateHomeserver(HomeServer):
    DATASTORE_CLASS = DataStore

    def __init__(self, config: HomeServerConfig):
        super().__init__(
            hostname=config.server.server_name, config=config, reactor=reactor
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate SQL event_auth edges to mtxdb. Stop Synapse first."
    )
    parser.add_argument("-v", action="store_true")
    parser.add_argument(
        "--config",
        type=argparse.FileType("r"),
        required=True,
        help="Synapse config with embedded_db.engine and embedded_db.path set.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="State events per batch."
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.v else logging.INFO,
        format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
    )

    config = HomeServerConfig()
    config.parse_config_dict(yaml.safe_load(args.config), "", "")
    hs = MigrateHomeserver(config)
    hs.setup()
    hs.get_storage_controllers()
    exit_code = 0

    async def run() -> None:
        nonlocal exit_code
        try:
            result = await migrate_auth_chain_edges(hs, batch_size=args.batch_size)
            print(json.dumps(result, indent=2, sort_keys=True))
        except Exception:
            logger.exception("Auth-edge migration failed")
            exit_code = 1
        finally:
            reactor.stop()

    hs.get_clock().call_when_running(lambda: defer.ensureDeferred(run()))
    reactor.run()
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
