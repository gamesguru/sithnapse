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

from synapse.config._base import ConfigError, RootConfig
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
        self.assertEqual(dc.embedded_hamt_path, "/tmp/test")

    def test_neither_set_ok(self) -> None:
        """engine + path both unset → no error."""
        dc = self._read_config()
        self.assertIsNone(dc.embedded_hamt_engine)
        self.assertIsNone(dc.embedded_hamt_path)


class EmbeddedHamtWorkerGuardTestCase(unittest.TestCase):
    """Test that embedded_hamt.engine is rejected only for *sharded-events*
    multi-worker configs (more than one instance in `writers.events`), not
    multi-worker configs in general.

    A single events writer opens the embedded engine writable; every other
    process (including any number of non-events workers) opens read-only
    and self-heals a stale read via
    `refresh_state_hamt_collections_for_groups` -- see
    `StateGroupDataStore.__init__`. That safety argument breaks down with
    more than one events writer, since each would independently decide
    it's the writer and race for mtxdb's exclusive lock -- see the guard
    in synapse/config/workers.py for the full explanation.
    """

    def _make_worker_config(
        self,
        worker_app: str | None = None,
        instance_map: dict | None = None,
        stream_writers: dict | None = None,
        run_background_tasks_on: str | None = None,
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
        if stream_writers is not None:
            config["stream_writers"] = stream_writers
        if run_background_tasks_on is not None:
            config["run_background_tasks_on"] = run_background_tasks_on
        worker_config.read_config(config, allow_secrets_in_config=True)

    def test_worker_app_with_single_events_writer_ok(self) -> None:
        """embedded_hamt + worker_app, default (single) events writer →
        no error: this is the supported single-writer, N-read-only-worker
        topology."""
        self._make_worker_config(
            worker_app="synapse.app.generic_worker",
            instance_map={"main": {"host": "127.0.0.1", "port": 8008}},
        )

    def test_instance_map_with_single_events_writer_ok(self) -> None:
        """embedded_hamt + non-empty instance_map (no worker_app), default
        (single) events writer → no error."""
        self._make_worker_config(
            instance_map={"main": {"host": "127.0.0.1", "port": 8008}},
        )

    def test_sharded_events_writers_raises(self) -> None:
        """embedded_hamt + more than one events writer → ConfigError: each
        would independently open the store writable and race for mtxdb's
        exclusive lock."""
        with self.assertRaises(ConfigError):
            self._make_worker_config(
                worker_app="synapse.app.generic_worker",
                instance_map={
                    "main": {"host": "127.0.0.1", "port": 8008},
                    "event_persister1": {"host": "127.0.0.1", "port": 8009},
                    "event_persister2": {"host": "127.0.0.1", "port": 8010},
                },
                stream_writers={"events": ["event_persister1", "event_persister2"]},
            )

    def test_single_explicit_events_writer_not_master_raises(self) -> None:
        """embedded_hamt + exactly one events writer, but it isn't the main
        process → ConfigError: the embedded-HAMT background migration's
        poll loop only ever runs on main (see synapse/app/homeserver.py),
        so main must also be the mtxdb writer or that migration crashes
        writing through main's read-only-opened store."""
        with self.assertRaises(ConfigError):
            self._make_worker_config(
                worker_app="synapse.app.generic_worker",
                instance_map={
                    "main": {"host": "127.0.0.1", "port": 8008},
                    "event_persister1": {"host": "127.0.0.1", "port": 8009},
                },
                stream_writers={"events": "event_persister1"},
            )

    def test_single_explicit_events_writer_is_master_ok(self) -> None:
        """embedded_hamt + exactly one events writer, and it's explicitly
        the main process → no error."""
        self._make_worker_config(
            worker_app="synapse.app.generic_worker",
            instance_map={"main": {"host": "127.0.0.1", "port": 8008}},
            stream_writers={"events": "master"},
        )

    def test_run_background_tasks_on_other_worker_raises(self) -> None:
        """embedded_hamt + a single events writer that is main, but
        run_background_tasks_on names a different instance → ConfigError:
        that instance would run its own independent background-updates
        poll loop, concurrently with main's own unconditional one, with no
        cross-instance coordination on the embedded-HAMT-writing rows."""
        with self.assertRaises(ConfigError):
            self._make_worker_config(
                worker_app="synapse.app.generic_worker",
                instance_map={
                    "main": {"host": "127.0.0.1", "port": 8008},
                    "background_worker1": {"host": "127.0.0.1", "port": 8009},
                },
                run_background_tasks_on="background_worker1",
            )

    def test_run_background_tasks_on_master_explicit_ok(self) -> None:
        """embedded_hamt + single events writer (main) + run_background_tasks_on
        explicitly set to main → no error."""
        self._make_worker_config(
            worker_app="synapse.app.generic_worker",
            instance_map={"main": {"host": "127.0.0.1", "port": 8008}},
            run_background_tasks_on="master",
        )

    def test_single_process_ok(self) -> None:
        """embedded_hamt alone (no worker_app, no instance_map) → no error."""
        self._make_worker_config()

    def test_no_embedded_hamt_with_sharded_writers_ok(self) -> None:
        """Sharded events writers without embedded_hamt → no error."""
        self._make_worker_config(
            worker_app="synapse.app.generic_worker",
            instance_map={
                "main": {"host": "127.0.0.1", "port": 8008},
                "event_persister1": {"host": "127.0.0.1", "port": 8009},
                "event_persister2": {"host": "127.0.0.1", "port": 8010},
            },
            stream_writers={"events": ["event_persister1", "event_persister2"]},
            embedded_hamt_engine=None,
        )
