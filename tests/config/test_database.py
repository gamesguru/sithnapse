#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
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

import os

import yaml

from synapse.config._base import RootConfig
from synapse.config.database import DatabaseConfig

from tests import unittest


class DatabaseConfigTestCase(unittest.TestCase):
    def test_database_configured_correctly(self) -> None:
        conf = yaml.safe_load(
            DatabaseConfig(RootConfig()).generate_config_section(
                data_dir_path="/data_dir_path"
            )
        )

        expected_database_conf = {
            "name": "sqlite3",
            "args": {"database": "/data_dir_path/homeserver.db"},
        }

        self.assertEqual(conf["database"], expected_database_conf)

    def _read_config(
        self, embedded_hamt: dict | None = None, env: dict[str, str] | None = None
    ) -> DatabaseConfig:
        """Helper: build a minimal config dict and parse it via DatabaseConfig."""
        config: dict = {"database": {"name": "sqlite3", "args": {}}}
        if embedded_hamt is not None:
            config["embedded_hamt"] = embedded_hamt

        old_env = os.environ.copy()
        if env:
            os.environ.update(env)
        try:
            dc = DatabaseConfig(RootConfig())
            dc.read_config(config)
            return dc
        finally:
            os.environ.clear()
            os.environ.update(old_env)

    def test_engine_without_path_raises(self) -> None:
        """engine set + path missing → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._read_config(
                embedded_hamt={"engine": "mdbx"},
            )

    def test_engine_without_path_env_raises(self) -> None:
        """SYNAPSE_EMBEDDED_HAMT_ENGINE set + path missing → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._read_config(
                env={"SYNAPSE_EMBEDDED_HAMT_ENGINE": "mdbx"},
            )

    def test_path_without_engine_raises(self) -> None:
        """path set + engine missing → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._read_config(
                embedded_hamt={"path": "/tmp/test.mdbx"},
            )

    def test_engine_not_mdbx_raises(self) -> None:
        """engine set to a non-mdbx value → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._read_config(
                embedded_hamt={"engine": "unknown_engine", "path": "/tmp/test"},
            )

    def test_both_set_ok(self) -> None:
        """engine + path both set → no error."""
        dc = self._read_config(
            embedded_hamt={"engine": "mdbx", "path": "/tmp/test.mdbx"},
        )
        self.assertEqual(dc.embedded_hamt_engine, "mdbx")
        self.assertEqual(dc.embedded_hamt_path, "/tmp/test.mdbx")

    def test_neither_set_ok(self) -> None:
        """engine + path both unset → no error."""
        dc = self._read_config()
        self.assertIsNone(dc.embedded_hamt_engine)
        self.assertIsNone(dc.embedded_hamt_path)
