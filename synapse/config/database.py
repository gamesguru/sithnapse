#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2020-2021 The Matrix.org Foundation C.I.C.
# Copyright 2014-2016 OpenMarket Ltd
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
import argparse
import logging
import os
from typing import Any

from synapse.config._base import Config, ConfigError
from synapse.types import JsonDict

logger = logging.getLogger(__name__)

NON_SQLITE_DATABASE_PATH_WARNING = """\
Ignoring 'database_path' setting: not using a sqlite3 database.
--------------------------------------------------------------------------------
"""

DEFAULT_CONFIG = """\
database:
  name: sqlite3
  args:
    database: %(database_path)s
"""


class DatabaseConnectionConfig:
    """Contains the connection config for a particular database.

    Args:
        name: A label for the database, used for logging.
        db_config: The config for a particular database, as per `database`
            section of main config. Has three fields: `name` for database
            module name, `args` for the args to give to the database
            connector, and optional `data_stores` that is a list of stores to
            provision on this database (defaulting to all).
    """

    def __init__(self, name: str, db_config: dict):
        db_engine = db_config.get("name", "sqlite3")

        if db_engine not in ("sqlite3", "psycopg2"):
            raise ConfigError("Unsupported database type %r" % (db_engine,))

        if db_engine == "sqlite3":
            db_config.setdefault("args", {}).update(
                {"cp_min": 1, "cp_max": 1, "check_same_thread": False}
            )

        data_stores = db_config.get("data_stores")
        if data_stores is None:
            data_stores = ["main", "state"]

        self.name = name
        self.config = db_config

        # The `data_stores` config is actually talking about `databases` (we
        # changed the name).
        self.databases = data_stores


class DatabaseConfig(Config):
    section = "database"

    def __init__(self, *args: Any):
        super().__init__(*args)

        self.databases: list[DatabaseConnectionConfig] = []
        self.embedded_hamt_engine: str | None = None
        self.embedded_hamt_path: str | None = None
        # Optional explicit namespace for HAMT keys in the embedded engine
        # (see StateGroupDataStore.hamt_namespace) -- defaults to the server
        # name when unset. Only useful for isolating multiple homeservers
        # that share one embedded-engine file (e.g. many trial test
        # processes reusing one mdbx path), not a normal deployment concern.
        self.embedded_hamt_namespace: str | None = None

    def read_config(self, config: JsonDict, **kwargs: Any) -> None:
        # We *experimentally* support specifying multiple databases via the
        # `databases` key. This is a map from a label to database config in the
        # same format as the `database` config option, plus an extra
        # `data_stores` key to specify which data store goes where. For example:
        #
        #   databases:
        #       master:
        #           name: psycopg2
        #           data_stores: ["main"]
        #           args: {}
        #       state:
        #           name: psycopg2
        #           data_stores: ["state"]
        #           args: {}

        multi_database_config = config.get("databases")
        database_config = config.get("database")
        database_path = config.get("database_path")

        embedded_config = config.get("embedded_hamt")
        if embedded_config:
            self.embedded_hamt_engine = embedded_config.get("engine")
            self.embedded_hamt_path = embedded_config.get("path")
            self.embedded_hamt_namespace = embedded_config.get("namespace")

        env_engine = os.environ.get("SYNAPSE_EMBEDDED_HAMT_ENGINE")
        if env_engine:
            self.embedded_hamt_engine = env_engine
        env_path = os.environ.get("SYNAPSE_EMBEDDED_HAMT_PATH")
        if env_path:
            self.embedded_hamt_path = env_path

        # A concise production switch. The path is deliberately still
        # required: unlike tests, a production server must never silently put
        # persistent state into a temporary directory.
        if os.environ.get("SYNAPSE_MDBX"):
            self.embedded_hamt_engine = "mdbx"
            self.embedded_hamt_path = os.environ.get(
                "SYNAPSE_MDBX_PATH", self.embedded_hamt_path
            )
            if not self.embedded_hamt_path:
                raise ConfigError(
                    "SYNAPSE_MDBX requires SYNAPSE_MDBX_PATH or embedded_hamt.path"
                )

        # Validate embedded_hamt engine/path consistency.
        # A half-set config (engine without path) boots fine but crashes on
        # first state write with "RuntimeError: mdbx not opened".
        if self.embedded_hamt_engine and not self.embedded_hamt_path:
            raise ConfigError(
                f"embedded_hamt.engine is set to {self.embedded_hamt_engine!r} "
                "but embedded_hamt.path is not set. "
                "Set embedded_hamt.path (or SYNAPSE_EMBEDDED_HAMT_PATH) to "
                "a file path, or remove the engine setting."
            )
        if self.embedded_hamt_path and not self.embedded_hamt_engine:
            raise ConfigError(
                "embedded_hamt.path is set but embedded_hamt.engine is not. "
                "Set embedded_hamt.engine (or SYNAPSE_EMBEDDED_HAMT_ENGINE) to "
                "'mdbx', or remove the path setting."
            )
        if self.embedded_hamt_engine and self.embedded_hamt_engine != "mdbx":
            raise ConfigError(
                f"embedded_hamt.engine is {self.embedded_hamt_engine!r}, "
                "but only 'mdbx' is supported."
            )

        if multi_database_config and database_config:
            raise ConfigError("Can't specify both 'database' and 'databases' in config")

        if multi_database_config:
            if database_path:
                raise ConfigError("Can't specify 'database_path' with 'databases'")

            self.databases = [
                DatabaseConnectionConfig(name, db_conf)
                for name, db_conf in multi_database_config.items()
            ]

        if database_config:
            self.databases = [DatabaseConnectionConfig("master", database_config)]

        if database_path:
            if self.databases and self.databases[0].name != "sqlite3":
                logger.warning(NON_SQLITE_DATABASE_PATH_WARNING)
                return

            database_config = {"name": "sqlite3", "args": {}}
            self.databases = [DatabaseConnectionConfig("master", database_config)]
            self.set_databasepath(database_path)

    def generate_config_section(self, data_dir_path: str, **kwargs: Any) -> str:
        return DEFAULT_CONFIG % {
            "database_path": os.path.join(data_dir_path, "homeserver.db")
        }

    def read_arguments(self, args: argparse.Namespace) -> None:
        """
        Cases for the cli input:
          - If no databases are configured and no database_path is set, raise.
          - No databases and only database_path available ==> sqlite3 db.
          - If there are multiple databases and a database_path raise an error.
          - If the database set in the config file is sqlite then
            overwrite with the command line argument.
        """

        if args.database_path is None:
            if not self.databases:
                raise ConfigError("No database config provided")
            return

        if len(self.databases) == 0:
            database_config = {"name": "sqlite3", "args": {}}
            self.databases = [DatabaseConnectionConfig("master", database_config)]
            self.set_databasepath(args.database_path)
            return

        if self.get_single_database().name == "sqlite3":
            self.set_databasepath(args.database_path)
        else:
            logger.warning(NON_SQLITE_DATABASE_PATH_WARNING)

    def set_databasepath(self, database_path: str) -> None:
        if database_path != ":memory:":
            database_path = self.abspath(database_path)

        self.databases[0].config["args"]["database"] = database_path

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        db_group = parser.add_argument_group("database")
        db_group.add_argument(
            "-d",
            "--database-path",
            metavar="SQLITE_DATABASE_PATH",
            help="The path to a sqlite database to use.",
        )

    def get_single_database(self) -> DatabaseConnectionConfig:
        """Returns the database if there is only one, useful for e.g. tests"""
        if not self.databases:
            raise Exception("More than one database exists")

        return self.databases[0]
