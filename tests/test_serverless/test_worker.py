"""Tests for the worker module."""

import unittest
from argparse import Namespace
from unittest.mock import MagicMock, patch

from wavespeed.serverless.worker import _is_local, run_worker


class TestIsLocal(unittest.TestCase):
    """Tests for the _is_local function."""

    def test_is_local_with_test_input(self):
        """Test _is_local returns True when test_input is provided."""
        args = Namespace(test_input='{"input": {}}')
        config = {"_args": args}

        self.assertTrue(_is_local(config))

    def test_is_local_without_job_endpoint(self):
        """Test _is_local returns True when no job endpoint is configured."""
        config = {"_args": None}

        with patch("wavespeed.serverless.worker.serverless") as mock_serverless:
            mock_serverless.webhook_get_job = None

            self.assertTrue(_is_local(config))

    def test_is_local_with_job_endpoint(self):
        """Test _is_local returns False when job endpoint is configured."""
        config = {"_args": None}

        with patch("wavespeed.serverless.worker.serverless") as mock_serverless:
            mock_serverless.webhook_get_job = "http://test.endpoint/jobs"

            self.assertFalse(_is_local(config))

    def test_is_local_test_input_overrides_endpoint(self):
        """Test that test_input takes precedence over job endpoint."""
        args = Namespace(test_input='{"input": {}}')
        config = {"_args": args}

        with patch("wavespeed.serverless.worker.serverless") as mock_serverless:
            mock_serverless.webhook_get_job = "http://test.endpoint/jobs"

            # Even with endpoint set, test_input should make it local
            self.assertTrue(_is_local(config))


class TestRunWorker(unittest.TestCase):
    """Tests for the run_worker function."""

    def test_run_worker_local_mode(self):
        """Test run_worker runs in local mode when appropriate."""

        def handler(job):
            return {"output": "test"}

        config = {
            "handler": handler,
            "_args": Namespace(test_input='{"input": {}}'),
        }

        with patch("wavespeed.serverless.worker.run_local") as mock_run_local, patch(
            "wavespeed.serverless.worker.log"
        ):
            run_worker(config)

            mock_run_local.assert_called_once_with(config)

    def test_run_worker_deployed_mode(self):
        """Test run_worker runs in deployed mode with job endpoint."""

        def handler(job):
            return {"output": "test"}

        config = {
            "handler": handler,
            "_args": None,
        }

        with patch("wavespeed.serverless.worker.serverless") as mock_serverless, patch(
            "wavespeed.serverless.worker.Heartbeat"
        ) as mock_heartbeat, patch(
            "wavespeed.serverless.worker.JobScaler"
        ) as mock_scaler, patch(
            "wavespeed.serverless.worker.log"
        ):
            mock_serverless.webhook_get_job = "http://test.endpoint/jobs"
            mock_serverless.pod_id = "worker_123"

            mock_heartbeat_instance = MagicMock()
            mock_heartbeat.return_value = mock_heartbeat_instance

            mock_scaler_instance = MagicMock()
            mock_scaler.return_value = mock_scaler_instance

            run_worker(config)

            mock_heartbeat_instance.start.assert_called_once()
            mock_scaler.assert_called_once_with(config)
            mock_scaler_instance.start.assert_called_once()
            mock_heartbeat_instance.stop.assert_called_once()

    def test_run_worker_keyboard_interrupt(self):
        """Test run_worker handles KeyboardInterrupt gracefully."""

        def handler(job):
            return {"output": "test"}

        config = {
            "handler": handler,
            "_args": None,
        }

        with patch("wavespeed.serverless.worker.serverless") as mock_serverless, patch(
            "wavespeed.serverless.worker.Heartbeat"
        ) as mock_heartbeat, patch(
            "wavespeed.serverless.worker.JobScaler"
        ) as mock_scaler, patch(
            "wavespeed.serverless.worker.log"
        ):
            mock_serverless.webhook_get_job = "http://test.endpoint/jobs"
            mock_serverless.pod_id = "worker_123"

            mock_heartbeat_instance = MagicMock()
            mock_heartbeat.return_value = mock_heartbeat_instance

            mock_scaler_instance = MagicMock()
            mock_scaler_instance.start.side_effect = KeyboardInterrupt()
            mock_scaler.return_value = mock_scaler_instance

            # Should not raise
            run_worker(config)

            # Heartbeat should still be stopped
            mock_heartbeat_instance.stop.assert_called_once()

    def test_run_worker_exception(self):
        """Test run_worker handles exceptions and re-raises."""

        def handler(job):
            return {"output": "test"}

        config = {
            "handler": handler,
            "_args": None,
        }

        with patch("wavespeed.serverless.worker.serverless") as mock_serverless, patch(
            "wavespeed.serverless.worker.Heartbeat"
        ) as mock_heartbeat, patch(
            "wavespeed.serverless.worker.JobScaler"
        ) as mock_scaler, patch(
            "wavespeed.serverless.worker.log"
        ):
            mock_serverless.webhook_get_job = "http://test.endpoint/jobs"
            mock_serverless.pod_id = "worker_123"

            mock_heartbeat_instance = MagicMock()
            mock_heartbeat.return_value = mock_heartbeat_instance

            mock_scaler_instance = MagicMock()
            mock_scaler_instance.start.side_effect = RuntimeError("Scaler failed")
            mock_scaler.return_value = mock_scaler_instance

            with self.assertRaises(RuntimeError):
                run_worker(config)

            # Heartbeat should still be stopped
            mock_heartbeat_instance.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
