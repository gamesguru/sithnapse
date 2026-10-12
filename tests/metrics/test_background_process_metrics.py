from unittest.mock import Mock

from twisted.internet import defer
from twisted.trial import unittest

from synapse.logging.context import ContextResourceUsage, LoggingContext
from synapse.metrics.background_process_metrics import (
    _background_process_in_flight_count,
    _BackgroundProcess,
    run_as_background_process,
)


class TestBackgroundProcessMetrics(unittest.TestCase):
    def test_update_metrics_with_negative_time_diff(self) -> None:
        """We should ignore negative reported utime and stime differences"""
        usage = ContextResourceUsage()
        usage.ru_stime = usage.ru_utime = -1.0

        mock_logging_context = Mock(spec=LoggingContext)
        mock_logging_context.get_resource_usage.return_value = usage

        process = _BackgroundProcess(
            desc="test process", server_name="test_server", ctx=mock_logging_context
        )
        # Should not raise
        process.update_metrics()

    def test_run_as_background_process_cancellation(self) -> None:
        """Cancellation should return None without raising."""

        pending: defer.Deferred[None] = defer.Deferred()

        async def _cancellable() -> None:
            await pending

        d = run_as_background_process(  # type: ignore[untracked-background-process]
            "test cancellation",
            "test_server",
            _cancellable,
        )
        gauge = _background_process_in_flight_count.labels(
            name="test cancellation", server_name="test_server"
        )
        self.assertEqual(gauge._value.get(), 1.0)
        d.cancel()
        self.successResultOf(d)
        self.assertIsNone(d.result)
        self.assertEqual(gauge._value.get(), 0.0)
