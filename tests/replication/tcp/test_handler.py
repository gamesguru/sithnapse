#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2022 The Matrix.org Foundation C.I.C.
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

from typing import cast

from twisted.internet import defer

from synapse.api.constants import ReceiptTypes
from synapse.replication.tcp.commands import PositionCommand
from synapse.replication.tcp.protocol import IReplicationConnection
from synapse.util.caches.stream_change_cache import StreamChangeCache

from tests.replication._base import BaseMultiWorkerStreamTestCase


class ChannelsTestCase(BaseMultiWorkerStreamTestCase):
    def test_subscribed_to_enough_redis_channels(self) -> None:
        # The default main process is subscribed to the USER_IP channel.
        self.assertCountEqual(
            self.hs.get_replication_command_handler()._channels_to_subscribe_to,
            ["USER_IP"],
        )

    def test_background_worker_subscribed_to_user_ip(self) -> None:
        # The default main process is subscribed to the USER_IP channel.
        worker1 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker1",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )
        self.assertIn(
            "USER_IP",
            worker1.get_replication_command_handler()._channels_to_subscribe_to,
        )

        # Advance so the Redis subscription gets processed
        self.pump(0.1)

        # The counts are 2 because both the main process and the worker are subscribed.
        self.assertEqual(len(self._redis_server._subscribers_by_channel[b"test"]), 2)
        self.assertEqual(
            len(self._redis_server._subscribers_by_channel[b"test/USER_IP"]), 2
        )

    def test_non_background_worker_not_subscribed_to_user_ip(self) -> None:
        # The default main process is subscribed to the USER_IP channel.
        worker2 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker2",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )
        self.assertNotIn(
            "USER_IP",
            worker2.get_replication_command_handler()._channels_to_subscribe_to,
        )

        # Advance so the Redis subscription gets processed
        self.pump(0.1)

        # The count is 2 because both the main process and the worker are subscribed.
        self.assertEqual(len(self._redis_server._subscribers_by_channel[b"test"]), 2)
        # For USER_IP, the count is 1 because only the main process is subscribed.
        self.assertEqual(
            len(self._redis_server._subscribers_by_channel[b"test/USER_IP"]), 1
        )

    def test_wait_for_stream_position(self) -> None:
        """Check that wait for stream position correctly waits for an update from the
        correct instance.
        """
        store = self.hs.get_datastores().main
        cmd_handler = self.hs.get_replication_command_handler()
        data_handler = self.hs.get_replication_data_handler()

        worker1 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker1",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )

        cache_id_gen = worker1.get_datastores().main._cache_id_gen
        assert cache_id_gen is not None

        self.replicate()

        # First, make sure the master knows that `worker1` exists.
        initial_token = cache_id_gen.get_current_token()
        cmd_handler.send_command(
            PositionCommand("caches", "worker1", initial_token, initial_token)
        )
        self.replicate()

        # Next send out a normal RDATA, and check that waiting for that stream
        # ID returns immediately.
        ctx = cache_id_gen.get_next()
        next_token = self.get_success(ctx.__aenter__())
        self.get_success(ctx.__aexit__(None, None, None))

        self.get_success(
            data_handler.wait_for_stream_position("worker1", "caches", next_token)
        )

        # `wait_for_stream_position` should only return once master receives a
        # notification that `next_token` has persisted.
        ctx_worker1 = cache_id_gen.get_next()
        next_token = self.get_success(ctx_worker1.__aenter__())

        d = defer.ensureDeferred(
            data_handler.wait_for_stream_position("worker1", "caches", next_token)
        )
        self.assertFalse(d.called)

        # ... updating the cache ID gen on the master still shouldn't cause the
        # deferred to wake up.
        assert store._cache_id_gen is not None
        ctx = store._cache_id_gen.get_next()
        self.get_success(ctx.__aenter__())
        self.get_success(ctx.__aexit__(None, None, None))

        d = defer.ensureDeferred(
            data_handler.wait_for_stream_position("worker1", "caches", next_token)
        )
        self.assertFalse(d.called)

        # ... but worker1 finishing (and so sending an update) should.
        self.get_success(ctx_worker1.__aexit__(None, None, None))

        # Wait for the stream position to be replicated to the master process
        #
        # Replication travels over `FakeTransport` and we're specifically flushing the
        # write
        self.reactor.advance(0)

        self.assertTrue(d.called)

    def test_wait_for_stream_position_rdata(self) -> None:
        """Check that wait for stream position correctly waits for an update
        from the correct instance, when RDATA is sent.
        """
        store = self.hs.get_datastores().main
        cmd_handler = self.hs.get_replication_command_handler()
        data_handler = self.hs.get_replication_data_handler()

        worker1 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker1",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )

        cache_id_gen = worker1.get_datastores().main._cache_id_gen
        assert cache_id_gen is not None

        self.replicate()

        # First, make sure the master knows that `worker1` exists.
        initial_token = cache_id_gen.get_current_token()
        cmd_handler.send_command(
            PositionCommand("caches", "worker1", initial_token, initial_token)
        )
        self.replicate()

        # `wait_for_stream_position` should only return once master receives a
        # notification that `next_token2` has persisted.
        ctx_worker1 = cache_id_gen.get_next_mult(2)
        next_token1, next_token2 = self.get_success(ctx_worker1.__aenter__())

        d = defer.ensureDeferred(
            data_handler.wait_for_stream_position("worker1", "caches", next_token2)
        )
        self.assertFalse(d.called)

        # Insert an entry into the cache stream with token `next_token1`, but
        # not `next_token2`.
        self.get_success(
            store.db_pool.simple_insert(
                table="cache_invalidation_stream_by_instance",
                values={
                    "stream_id": next_token1,
                    "instance_name": "worker1",
                    "cache_func": "foo",
                    "keys": [],
                    "invalidation_ts": 0,
                },
            )
        )

        # Finish the context manager, triggering the data to be sent to master.
        self.get_success(ctx_worker1.__aexit__(None, None, None))

        # Wait for the stream position to be replicated to the master process
        #
        # Replication travels over `FakeTransport` and we're specifically flushing the
        # write
        self.reactor.advance(0)

        # Master should get told about `next_token2`, so the deferred should
        # resolve.
        self.assertTrue(d.called)

    def test_position_recovers_dropped_rdata_for_stream_change_cache(self) -> None:
        """A POSITION whose `prev_token` equals our last-known position for
        that writer must still trigger a catch-up fetch, not be treated as
        "nothing missing".

        This simulates an RDATA broadcast being dropped (e.g. a missed Redis
        pub/sub message): the row is persisted and the writer's id gen has
        moved on, but this instance never received the corresponding RDATA,
        so its own copy of the writer's position is still stuck at
        `prev_token`. If the POSITION for that gap is (wrongly) treated as a
        no-op, the id gen still advances -- via `on_position`'s `on_rdata`
        call with an empty row list -- but `_receipts_stream_cache`, which is
        only updated from actual rows, never learns the room changed.
        """
        worker1 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker1",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )
        worker1_store = worker1.get_datastores().main
        main_store = self.hs.get_datastores().main

        room_id = "!test:test"
        user_id = "@bob:test"

        # Establish a baseline: worker1 knows the main process's current
        # receipts position.
        self.replicate()
        prev_token = worker1_store._receipts_id_gen.get_current_token_for_writer(
            "master"
        )
        self.assertEqual(
            prev_token,
            main_store._receipts_id_gen.get_current_token_for_writer("master"),
        )

        # Write a receipt on the main process and let it replicate normally.
        self.get_success(
            main_store.insert_receipt(
                room_id, ReceiptTypes.READ, user_id, ["$fake:test"], None, {}
            )
        )
        self.replicate()

        new_token = main_store._receipts_id_gen.get_current_token_for_writer("master")
        self.assertGreater(new_token, prev_token)
        # Sanity check: worker1 really did catch up for real.
        self.assertEqual(
            worker1_store._receipts_id_gen.get_current_token_for_writer("master"),
            new_token,
        )

        # Now simulate the RDATA for that write having been dropped (e.g. a
        # missed Redis pub/sub message): roll back worker1's tracked position
        # for the "master" writer to `prev_token`, as if it had never seen
        # the update. `get_current_token_for_writer` returns
        # `max(_current_positions[instance], _persisted_upto_position)`, so
        # both need rolling back -- otherwise the persisted-upto watermark
        # (already advanced by the real replication above) masks the
        # rollback and this "faithfully reproduces a dropped RDATA" premise
        # would silently not hold.
        worker1_store._receipts_id_gen._current_positions["master"] = prev_token
        worker1_store._receipts_id_gen._persisted_upto_position = prev_token
        self.assertEqual(
            worker1_store._receipts_id_gen.get_current_token_for_writer("master"),
            prev_token,
        )

        # Also reset worker1's stream-change cache back to "nothing known
        # past prev_token" -- the earlier real replication already
        # (correctly) populated it, so without this reset the upcoming
        # assertion couldn't distinguish "the fix's catch-up repopulated it"
        # from "it was already set from before".
        worker1_store._receipts_stream_cache = StreamChangeCache(
            name="ReceiptsRoomChangeCache",
            server_name=worker1_store.server_name,
            current_stream_pos=prev_token,
        )
        self.assertFalse(
            worker1_store._receipts_stream_cache.has_entity_changed(room_id, prev_token)
        )

        # Deliver *only* the POSITION for that write directly to worker1's
        # command handler, as if it arrived over replication without a
        # preceding RDATA -- `prev_token` here equals what worker1 now
        # (again) believes the position to be.
        position_cmd = PositionCommand("receipts", "master", prev_token, new_token)
        worker1_cmd_handler = worker1.get_replication_command_handler()
        fake_conn = cast(IReplicationConnection, object())
        self.get_success(
            worker1_cmd_handler._process_position("receipts", fake_conn, position_cmd)
        )

        # The whole point of the fix: a POSITION whose `prev_token` matches
        # our current position must still trigger a catch-up fetch for the
        # gap, repopulating the stream-change cache -- not be treated as a
        # no-op that leaves it stale.
        self.assertTrue(
            worker1_store._receipts_stream_cache.has_entity_changed(room_id, prev_token)
        )

        # And that catch-up must have brought worker1's id gen position back
        # in sync -- proving the fetched row didn't just silently vanish.
        self.assertEqual(
            worker1_store._receipts_id_gen.get_current_token_for_writer("master"),
            new_token,
        )

    def test_all_equal_position_does_not_call_get_updates_since(self) -> None:
        """A POSITION where prev_token == new_token == current_token is a zero-width
        interval. _process_position must short-circuit and never call
        stream.get_updates_since.
        """
        from unittest.mock import AsyncMock, patch

        worker1 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker1",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )
        self.replicate()

        worker1_cmd_handler = worker1.get_replication_command_handler()
        fake_conn = cast(IReplicationConnection, object())

        # The receipts stream token on worker1 as seen from the master writer.
        receipts_stream = worker1_cmd_handler._streams["receipts"]
        token = receipts_stream.current_token("master")

        # Construct an all-equal POSITION: prev == new == current.
        position_cmd = PositionCommand("receipts", "master", token, token)

        spy = AsyncMock(return_value=([], token, False))
        with patch.object(receipts_stream, "get_updates_since", spy):
            self.get_success(
                worker1_cmd_handler._process_position(
                    "receipts", fake_conn, position_cmd
                )
            )

        spy.assert_not_called()

    def test_dropped_rdata_position_still_calls_get_updates_since(self) -> None:
        """A POSITION where current_token == prev_token but prev_token < new_token
        (the dropped-RDATA scenario) must still call stream.get_updates_since to
        recover the gap.
        """
        from unittest.mock import AsyncMock, patch

        worker1 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker1",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )
        self.replicate()

        worker1_store = worker1.get_datastores().main
        worker1_cmd_handler = worker1.get_replication_command_handler()
        fake_conn = cast(IReplicationConnection, object())

        receipts_stream = worker1_cmd_handler._streams["receipts"]
        prev_token = receipts_stream.current_token("master")
        new_token = prev_token + 1

        # Simulate the dropped-RDATA scenario: roll worker1's position back so
        # that current_token == prev_token while the writer has advanced to new_token.
        worker1_store._receipts_id_gen._current_positions["master"] = prev_token
        worker1_store._receipts_id_gen._persisted_upto_position = prev_token

        position_cmd = PositionCommand("receipts", "master", prev_token, new_token)

        # get_updates_since will return an empty list (no real rows were written
        # for this synthetic token gap), but it must still be called.
        spy = AsyncMock(return_value=([], new_token, False))
        with patch.object(receipts_stream, "get_updates_since", spy):
            self.get_success(
                worker1_cmd_handler._process_position(
                    "receipts", fake_conn, position_cmd
                )
            )

        spy.assert_called_once_with("master", prev_token, new_token)

    def test_stream_reset_position_forces_catchup(self) -> None:
        """A POSITION where current_token > cmd.new_token describes a stream
        that has been "reset" (e.g. `caches` or `typing` after the writer
        restarts and its id gen starts again from a low value). This must
        still be treated as `missing_updates`, not silently ignored --
        otherwise this instance stays stuck at its old (now invalid) position
        forever, since nothing else will ever tell it to move backwards.
        """
        from unittest.mock import AsyncMock, patch

        worker1 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker1",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )
        self.replicate()

        worker1_cmd_handler = worker1.get_replication_command_handler()
        fake_conn = cast(IReplicationConnection, object())

        receipts_stream = worker1_cmd_handler._streams["receipts"]
        current_token = receipts_stream.current_token("master")

        # Simulate a writer restart: the incoming POSITION's new_token is
        # lower than what we're currently sitting at.
        prev_token = 0
        new_token = 0
        assert current_token > new_token

        position_cmd = PositionCommand("receipts", "master", prev_token, new_token)

        spy = AsyncMock(return_value=([], new_token, False))
        with patch.object(receipts_stream, "get_updates_since", spy):
            self.get_success(
                worker1_cmd_handler._process_position(
                    "receipts", fake_conn, position_cmd
                )
            )

        spy.assert_called_once_with("master", current_token, new_token)

    def test_partial_progress_position_does_not_force_catchup(self) -> None:
        """A POSITION where cmd.prev_token < current_token < cmd.new_token
        describes an instance that is already mid-stream, actively catching
        up via ordinary RDATA delivery. _process_position must not force a
        redundant `get_updates_since` fetch in this case -- the remaining gap
        will be closed by normal RDATA processing, and forcing a fetch on
        every such POSITION would defeat the purpose of streaming updates
        over pub/sub.
        """
        from unittest.mock import AsyncMock, patch

        worker1 = self.make_worker_hs(
            "synapse.app.generic_worker",
            extra_config={
                "worker_name": "worker1",
                "run_background_tasks_on": "worker1",
                "redis": {"enabled": True},
            },
        )
        self.replicate()

        worker1_store = worker1.get_datastores().main
        worker1_cmd_handler = worker1.get_replication_command_handler()
        fake_conn = cast(IReplicationConnection, object())

        receipts_stream = worker1_cmd_handler._streams["receipts"]
        current_token = receipts_stream.current_token("master")

        # Simulate normal partial progress: our current position is strictly
        # between prev_token and new_token, i.e. we've already moved on from
        # prev_token via ordinary RDATA delivery, but haven't reached
        # new_token yet.
        prev_token = current_token - 1
        new_token = current_token + 1
        worker1_store._receipts_id_gen._current_positions["master"] = current_token
        worker1_store._receipts_id_gen._persisted_upto_position = current_token

        position_cmd = PositionCommand("receipts", "master", prev_token, new_token)

        spy = AsyncMock(return_value=([], new_token, False))
        with patch.object(receipts_stream, "get_updates_since", spy):
            self.get_success(
                worker1_cmd_handler._process_position(
                    "receipts", fake_conn, position_cmd
                )
            )

        spy.assert_not_called()
