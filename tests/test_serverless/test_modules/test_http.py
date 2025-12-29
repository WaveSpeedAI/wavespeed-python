"""Tests for the http module."""

import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from wavespeed.serverless.modules.http import fetch_jobs, send_result, stream_result


class TestSendResult(IsolatedAsyncioTestCase):
    """Tests for the send_result function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.job = {"id": "test_job_123", "input": {"data": "test"}}
        self.mock_session = MagicMock(spec=aiohttp.ClientSession)
        self.mock_session.headers = {}

    async def test_send_result_success(self):
        """Test successful result sending."""
        result = {"output": "test_output"}

        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http._transmit"
        ) as mock_transmit, patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_done_url = "http://test.endpoint/$ID/output"
            mock_transmit.return_value = None

            await send_result(self.mock_session, result, self.job)

            mock_transmit.assert_called_once()
            call_args = mock_transmit.call_args
            # Check URL has job ID and isStream param
            self.assertIn("test_job_123", call_args[0][1])
            self.assertIn("isStream=false", call_args[0][1])

    async def test_send_result_with_stream_flag(self):
        """Test result sending with is_stream=True."""
        result = {"output": []}

        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http._transmit"
        ) as mock_transmit, patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_done_url = "http://test.endpoint/$ID/output"
            mock_transmit.return_value = None

            await send_result(self.mock_session, result, self.job, is_stream=True)

            call_args = mock_transmit.call_args
            self.assertIn("isStream=true", call_args[0][1])

    async def test_send_result_error_handling(self):
        """Test send_result handles errors gracefully."""
        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http._transmit"
        ) as mock_transmit, patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_done_url = "http://test.endpoint/$ID/output"
            mock_transmit.side_effect = aiohttp.ClientError("Connection failed")

            # Should not raise, just log error
            await send_result(self.mock_session, {"output": "test"}, self.job)


class TestStreamResult(IsolatedAsyncioTestCase):
    """Tests for the stream_result function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.job = {"id": "stream_job_123", "input": {}}
        self.mock_session = MagicMock(spec=aiohttp.ClientSession)
        self.mock_session.headers = {}

    async def test_stream_result_success(self):
        """Test successful stream result."""
        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http._transmit"
        ) as mock_transmit, patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_stream_url = "http://test.endpoint/$ID/stream"
            mock_transmit.return_value = None

            await stream_result(self.mock_session, {"output": "partial"}, self.job)

            mock_transmit.assert_called_once()

    async def test_stream_result_uses_stream_url(self):
        """Test stream result uses correct URL template."""
        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http._transmit"
        ) as mock_transmit, patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_stream_url = "http://stream.endpoint/$ID"
            mock_transmit.return_value = None

            await stream_result(self.mock_session, {"output": "data"}, self.job)

            call_args = mock_transmit.call_args
            self.assertIn("stream_job_123", call_args[0][1])


class TestFetchJobs(IsolatedAsyncioTestCase):
    """Tests for the fetch_jobs function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.mock_session = AsyncMock(spec=aiohttp.ClientSession)

    async def test_fetch_jobs_success_single_job(self):
        """Test successful job fetching with single job (dict response)."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content_type = "application/json"
        mock_response.content_length = 100
        mock_response.json = AsyncMock(return_value={"id": "job_1", "input": {"n": 1}})
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http.JobsProgress"
        ), patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_get_url = "http://test.endpoint/job-take/worker1"

            jobs = await fetch_jobs(self.mock_session, num_jobs=1)

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["id"], "job_1")

    async def test_fetch_jobs_success_batch(self):
        """Test successful job fetching with batch (list response)."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content_type = "application/json"
        mock_response.content_length = 100
        mock_response.json = AsyncMock(
            return_value=[
                {"id": "job_1", "input": {"n": 1}},
                {"id": "job_2", "input": {"n": 2}},
            ]
        )
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http.JobsProgress"
        ), patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_get_url = "http://test.endpoint/job-take/worker1"

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
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http.JobsProgress"
        ), patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_get_url = "http://test.endpoint/job-take/worker1"

            jobs = await fetch_jobs(self.mock_session)

            self.assertIsNone(jobs)

    async def test_fetch_jobs_400_flashboot(self):
        """Test fetch_jobs with 400 (FlashBoot enabled)."""
        mock_response = AsyncMock()
        mock_response.status = 400
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http.JobsProgress"
        ), patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_get_url = "http://test.endpoint/job-take/worker1"

            jobs = await fetch_jobs(self.mock_session)

            self.assertIsNone(jobs)

    async def test_fetch_jobs_429_raises(self):
        """Test fetch_jobs with 429 raises for special handling."""
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.request_info = MagicMock()
        mock_response.history = []
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http.JobsProgress"
        ), patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_get_url = "http://test.endpoint/job-take/worker1"

            with self.assertRaises(aiohttp.ClientResponseError) as context:
                await fetch_jobs(self.mock_session)

            self.assertEqual(context.exception.status, 429)

    async def test_fetch_jobs_no_endpoint(self):
        """Test fetch_jobs when no endpoint configured."""
        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch("wavespeed.serverless.modules.http.log"):
            mock_serverless.job_get_url = None

            jobs = await fetch_jobs(self.mock_session)

            self.assertIsNone(jobs)

    async def test_fetch_jobs_client_error(self):
        """Test fetch_jobs handles client errors."""
        self.mock_session.get.side_effect = aiohttp.ClientError("Connection failed")

        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http.JobsProgress"
        ), patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_get_url = "http://test.endpoint/job-take/worker1"

            jobs = await fetch_jobs(self.mock_session)

            self.assertIsNone(jobs)

    async def test_fetch_jobs_batch_url_modification(self):
        """Test that batch requests modify URL correctly."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content_type = "application/json"
        mock_response.content_length = 10
        mock_response.json = AsyncMock(return_value=[])
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        self.mock_session.get.return_value = mock_response

        with patch(
            "wavespeed.serverless.modules.http.serverless"
        ) as mock_serverless, patch(
            "wavespeed.serverless.modules.http.JobsProgress"
        ) as mock_progress, patch(
            "wavespeed.serverless.modules.http.log"
        ):
            mock_serverless.job_get_url = "http://test.endpoint/job-take/worker1"
            mock_progress_instance = MagicMock()
            mock_progress_instance.get_all.return_value = set()
            mock_progress.return_value = mock_progress_instance

            await fetch_jobs(self.mock_session, num_jobs=5)

            call_args = self.mock_session.get.call_args
            url = call_args[0][0]
            self.assertIn("job-take-batch", url)
            self.assertIn("batch_size=5", url)
            self.assertIn("job_in_progress=0", url)


if __name__ == "__main__":
    unittest.main()
