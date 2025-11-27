"""Tests for the http module."""

import json
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch, MagicMock

import aiohttp

from wavespeed.serverless.modules.http import (
    send_result,
    stream_result,
    fetch_jobs,
)
from wavespeed.serverless.modules.state import Job


class TestSendResult(IsolatedAsyncioTestCase):
    """Tests for the send_result function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.job = Job(id="test_job_123", input={"data": "test"})
        self.mock_session = AsyncMock(spec=aiohttp.ClientSession)

    async def test_send_result_success(self):
        """Test successful result sending."""
        result = {"output": "test_output"}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.post.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "OUTPUT_ENDPOINT": "http://test.endpoint/output",
                "API_KEY": "test_api_key",
            }.get(key, default)

            success = await send_result(self.mock_session, result, self.job)

            self.assertTrue(success)
            self.mock_session.post.assert_called_once()

    async def test_send_result_with_webhook(self):
        """Test result sending uses webhook when available."""
        job_with_webhook = Job(
            id="test_job",
            input={},
            webhook="http://custom.webhook/callback",
        )
        result = {"output": "test"}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.post.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "API_KEY": "test_key",
            }.get(key, default)

            await send_result(self.mock_session, result, job_with_webhook)

            # Check that webhook URL was used
            call_args = self.mock_session.post.call_args
            self.assertEqual(call_args[0][0], "http://custom.webhook/callback")

    async def test_send_result_error_status(self):
        """Test result includes error status when error present."""
        result = {"error": "Something went wrong"}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.post.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "OUTPUT_ENDPOINT": "http://test.endpoint/output",
                "API_KEY": "test_key",
            }.get(key, default)

            await send_result(self.mock_session, result, self.job)

            # Check payload contains error
            call_args = self.mock_session.post.call_args
            payload = call_args[1]["json"]
            self.assertEqual(payload["status"], "FAILED")
            self.assertEqual(payload["error"], "Something went wrong")

    async def test_send_result_no_endpoint(self):
        """Test send_result returns False when no endpoint configured."""
        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.return_value = None

            job_no_webhook = Job(id="test", input={}, webhook=None)
            success = await send_result(
                self.mock_session, {"output": "test"}, job_no_webhook
            )

            self.assertFalse(success)

    async def test_send_result_http_error(self):
        """Test send_result handles HTTP errors."""
        self.mock_session.post.side_effect = aiohttp.ClientError("Connection failed")

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "OUTPUT_ENDPOINT": "http://test.endpoint/output",
                "API_KEY": "test_key",
            }.get(key, default)

            success = await send_result(
                self.mock_session, {"output": "test"}, self.job
            )

            self.assertFalse(success)


class TestStreamResult(IsolatedAsyncioTestCase):
    """Tests for the stream_result function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.job = Job(id="stream_job_123", input={})
        self.mock_session = AsyncMock(spec=aiohttp.ClientSession)

    async def test_stream_result_success(self):
        """Test successful stream result."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.post.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "STREAM_ENDPOINT": "http://test.endpoint/stream",
                "API_KEY": "test_key",
            }.get(key, default)

            success = await stream_result(
                self.mock_session, "partial_output", self.job
            )

            self.assertTrue(success)

    async def test_stream_result_no_endpoint(self):
        """Test stream_result when no endpoint configured."""
        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.return_value = None

            success = await stream_result(
                self.mock_session, "partial_output", self.job
            )

            self.assertFalse(success)

    async def test_stream_result_payload_format(self):
        """Test stream result payload format."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.post.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "STREAM_ENDPOINT": "http://test.endpoint/stream",
                "API_KEY": "test_key",
            }.get(key, default)

            await stream_result(self.mock_session, {"partial": "data"}, self.job)

            call_args = self.mock_session.post.call_args
            payload = call_args[1]["json"]
            self.assertEqual(payload["id"], "stream_job_123")
            self.assertEqual(payload["status"], "IN_PROGRESS")
            self.assertEqual(payload["stream"], {"partial": "data"})


class TestFetchJobs(IsolatedAsyncioTestCase):
    """Tests for the fetch_jobs function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.mock_session = AsyncMock(spec=aiohttp.ClientSession)

    async def test_fetch_jobs_success(self):
        """Test successful job fetching."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "jobs": [
                    {"id": "job_1", "input": {"n": 1}},
                    {"id": "job_2", "input": {"n": 2}},
                ]
            }
        )
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "JOB_ENDPOINT": "http://test.endpoint/jobs",
                "API_KEY": "test_key",
            }.get(key, default)

            jobs = await fetch_jobs(self.mock_session, num_jobs=2)

            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[0]["id"], "job_1")
            self.assertEqual(jobs[1]["id"], "job_2")

    async def test_fetch_jobs_204_no_content(self):
        """Test fetch_jobs with 204 No Content response."""
        mock_response = AsyncMock()
        mock_response.status = 204
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "JOB_ENDPOINT": "http://test.endpoint/jobs",
                "API_KEY": "test_key",
            }.get(key, default)

            jobs = await fetch_jobs(self.mock_session)

            self.assertEqual(jobs, [])

    async def test_fetch_jobs_no_endpoint(self):
        """Test fetch_jobs when no endpoint configured."""
        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.return_value = None

            jobs = await fetch_jobs(self.mock_session)

            self.assertEqual(jobs, [])

    async def test_fetch_jobs_error_status(self):
        """Test fetch_jobs with error status code."""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "JOB_ENDPOINT": "http://test.endpoint/jobs",
                "API_KEY": "test_key",
            }.get(key, default)

            jobs = await fetch_jobs(self.mock_session)

            self.assertEqual(jobs, [])

    async def test_fetch_jobs_client_error(self):
        """Test fetch_jobs handles client errors."""
        self.mock_session.get.side_effect = aiohttp.ClientError("Connection failed")

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "JOB_ENDPOINT": "http://test.endpoint/jobs",
                "API_KEY": "test_key",
            }.get(key, default)

            jobs = await fetch_jobs(self.mock_session)

            self.assertEqual(jobs, [])

    async def test_fetch_jobs_batch_size_query_param(self):
        """Test that batch_size is passed as query parameter."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"jobs": []})
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.get_serverless_env"
        ) as mock_env, patch("wavespeed.serverless.modules.http.log"):
            mock_env.side_effect = lambda key, default="": {
                "JOB_ENDPOINT": "http://test.endpoint/jobs",
                "API_KEY": "test_key",
            }.get(key, default)

            await fetch_jobs(self.mock_session, num_jobs=5)

            call_args = self.mock_session.get.call_args
            self.assertIn("batch_size=5", call_args[0][0])


if __name__ == "__main__":
    unittest.main()
