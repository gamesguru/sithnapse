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
                embedded_hamt={"engine": "mtxdb"},
            )

    def test_engine_without_path_env_raises(self) -> None:
        """SYNAPSE_EMBEDDED_HAMT_ENGINE set + path missing → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._read_config(
                env={"SYNAPSE_EMBEDDED_HAMT_ENGINE": "mtxdb"},
            )

    def test_path_without_engine_raises(self) -> None:
        """path set + engine missing → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._read_config(
                embedded_hamt={"path": "/tmp/test.mtxdb"},
            )

    def test_engine_unsupported_raises(self) -> None:
        """engine set to an unsupported value → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._read_config(
                embedded_hamt={"engine": "unknown_engine", "path": "/tmp/test"},
            )

    def test_engine_mtxdb_ok(self) -> None:
        """engine set to 'mtxdb' with a path → no error."""
        dc = self._read_config(
            embedded_hamt={"engine": "mtxdb", "path": "/tmp/test"},
        )
        self.assertEqual(dc.embedded_hamt_engine, "mtxdb")

    def test_neither_set_ok(self) -> None:
        """engine + path both unset → no error."""
        dc = self._read_config()
        self.assertIsNone(dc.embedded_hamt_engine)
        self.assertIsNone(dc.embedded_hamt_path)


class EmbeddedHamtWorkerGuardTestCase(unittest.TestCase):
    """Test that embedded_hamt.engine is rejected in multi-worker configs."""

    def _make_worker_config(
        self,
        worker_app: str | None = None,
        instance_map: dict | None = None,
        embedded_hamt_engine: str | None = "mtxdb",
    ) -> None:
        """Build a WorkerConfig and call read_config, triggering the guard."""
        from unittest.mock import Mock

        from synapse.config.workers import WorkerConfig

        root = Mock()
        root.database.embedded_hamt_engine = embedded_hamt_engine

        worker_config = WorkerConfig(root)
        config: dict = {}
        if worker_app is not None:
            config["worker_app"] = worker_app
        if instance_map is not None:
            config["instance_map"] = instance_map
        worker_config.read_config(config, allow_secrets_in_config=True)

    def test_worker_app_raises(self) -> None:
        """embedded_hamt + worker_app → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._make_worker_config(
                worker_app="synapse.app.generic_worker",
                instance_map={"main": {"host": "127.0.0.1", "port": 8008}},
            )

    def test_instance_map_raises(self) -> None:
        """embedded_hamt + non-empty instance_map (no worker_app) → ConfigError."""
        from synapse.config._base import ConfigError

        with self.assertRaises(ConfigError):
            self._make_worker_config(
                instance_map={"main": {"host": "127.0.0.1", "port": 8008}},
            )

    def test_single_process_ok(self) -> None:
        """embedded_hamt alone (no worker_app, no instance_map) → no error."""
        self._make_worker_config()

    def test_no_embedded_hamt_with_workers_ok(self) -> None:
        """worker_app + instance_map without embedded_hamt → no error."""
        self._make_worker_config(
            worker_app="synapse.app.generic_worker",
            instance_map={"main": {"host": "127.0.0.1", "port": 8008}},
            embedded_hamt_engine=None,
        )
