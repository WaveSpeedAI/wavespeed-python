"""Tests for the progress module."""

import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from wavespeed.serverless.modules.progress import async_progress_update, progress_update
from wavespeed.serverless.modules.state import Job


class TestProgressUpdate(unittest.TestCase):
    """Tests for the progress_update function."""

    def test_progress_update_with_job_object(self):
        """Test progress update with Job object."""
        job = Job(id="job_123", input={})

        with patch(
            "wavespeed.serverless.modules.progress.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.progress.threading.Thread"
        ) as mock_thread:
            mock_serverless.webhook_post_output = "http://test.endpoint/output"
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance

            result = progress_update(job, {"progress": 50})

            self.assertTrue(result)
            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

    def test_progress_update_with_job_dict(self):
        """Test progress update with job dict."""
        job = {"id": "job_123", "input": {}}

        with patch(
            "wavespeed.serverless.modules.progress.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.progress.threading.Thread"
        ) as mock_thread:
            mock_serverless.webhook_post_output = "http://test.endpoint/output"
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance

            result = progress_update(job, {"progress": 75})

            self.assertTrue(result)

    def test_progress_update_invalid_job_type(self):
        """Test progress update with invalid job type."""
        with patch("wavespeed.serverless.modules.progress.log") as mock_log:
            result = progress_update("invalid", {"progress": 50})

            self.assertFalse(result)
            mock_log.warn.assert_called()

    def test_progress_update_no_endpoint(self):
        """Test progress update when no endpoint configured."""
        job = Job(id="job_123", input={})

        with patch(
            "wavespeed.serverless.modules.progress.serverless"
        ) as mock_serverless, patch("wavespeed.serverless.modules.progress.log"):
            mock_serverless.webhook_post_output = None

            result = progress_update(job, {"progress": 50})

            self.assertFalse(result)


class TestAsyncProgressUpdate(IsolatedAsyncioTestCase):
    """Tests for the async_progress_update function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.job = Job(id="job_123", input={})
        self.mock_session = AsyncMock(spec=aiohttp.ClientSession)

    async def test_async_progress_update_success(self):
        """Test successful async progress update."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.post.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.progress.serverless"
        ) as mock_serverless, patch("wavespeed.serverless.modules.progress.log"):
            mock_serverless.webhook_post_output = "http://test.endpoint/output"
            mock_serverless.api_key = "test_key"

            result = await async_progress_update(
                self.mock_session, self.job, {"progress": 50}
            )

            self.assertTrue(result)
            self.mock_session.post.assert_called_once()

    async def test_async_progress_update_payload(self):
        """Test async progress update payload format."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.post.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.progress.serverless"
        ) as mock_serverless, patch("wavespeed.serverless.modules.progress.log"):
            mock_serverless.webhook_post_output = "http://test.endpoint/output"
            mock_serverless.api_key = "test_key"

            await async_progress_update(
                self.mock_session, self.job, {"current": 5, "total": 10}
            )

            call_args = self.mock_session.post.call_args
            payload = call_args[1]["json"]
            self.assertEqual(payload["id"], "job_123")
            self.assertEqual(payload["status"], "IN_PROGRESS")
            self.assertEqual(payload["output"], {"current": 5, "total": 10})

    async def test_async_progress_update_no_endpoint(self):
        """Test async progress update when no endpoint configured."""
        with patch(
            "wavespeed.serverless.modules.progress.serverless"
        ) as mock_serverless, patch("wavespeed.serverless.modules.progress.log"):
            mock_serverless.webhook_post_output = None

            result = await async_progress_update(
                self.mock_session, self.job, {"progress": 50}
            )

            self.assertFalse(result)

    async def test_async_progress_update_error_status(self):
        """Test async progress update with error response."""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.post.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.progress.serverless"
        ) as mock_serverless, patch("wavespeed.serverless.modules.progress.log"):
            mock_serverless.webhook_post_output = "http://test.endpoint/output"
            mock_serverless.api_key = "test_key"

            result = await async_progress_update(
                self.mock_session, self.job, {"progress": 50}
            )

            self.assertFalse(result)

    async def test_async_progress_update_client_error(self):
        """Test async progress update handles client errors."""
        self.mock_session.post.side_effect = aiohttp.ClientError("Connection failed")

        with patch(
            "wavespeed.serverless.modules.progress.serverless"
        ) as mock_serverless, patch("wavespeed.serverless.modules.progress.log"):
            mock_serverless.webhook_post_output = "http://test.endpoint/output"
            mock_serverless.api_key = "test_key"

            result = await async_progress_update(
                self.mock_session, self.job, {"progress": 50}
            )

            self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
