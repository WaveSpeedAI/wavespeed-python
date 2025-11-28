"""Tests for the job module."""

import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wavespeed.serverless.modules.job import (
    get_job,
    handle_job,
    run_job,
    run_job_generator,
)


class TestGetJob(IsolatedAsyncioTestCase):
    """Tests for the get_job function."""

    async def test_get_job_success(self):
        """Test successful job fetching."""
        mock_session = AsyncMock()

        with patch(
            "wavespeed.serverless.modules.job.fetch_jobs",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = [
                {"id": "job_123", "input": {"prompt": "test"}, "webhook": None}
            ]

            jobs = await get_job(mock_session, num_jobs=1)

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["id"], "job_123")
            self.assertEqual(jobs[0]["input"], {"prompt": "test"})
            mock_fetch.assert_called_once_with(mock_session, 1)

    async def test_get_job_empty(self):
        """Test when no jobs are available."""
        mock_session = AsyncMock()

        with patch(
            "wavespeed.serverless.modules.job.fetch_jobs",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = None

            jobs = await get_job(mock_session, num_jobs=1)

            self.assertIsNone(jobs)

    async def test_get_job_multiple(self):
        """Test fetching multiple jobs."""
        mock_session = AsyncMock()

        with patch(
            "wavespeed.serverless.modules.job.fetch_jobs",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = [
                {"id": "job_1", "input": {"n": 1}},
                {"id": "job_2", "input": {"n": 2}},
                {"id": "job_3", "input": {"n": 3}},
            ]

            jobs = await get_job(mock_session, num_jobs=3)

            self.assertEqual(len(jobs), 3)
            self.assertEqual(jobs[0]["id"], "job_1")
            self.assertEqual(jobs[1]["id"], "job_2")
            self.assertEqual(jobs[2]["id"], "job_3")


class TestRunJob(IsolatedAsyncioTestCase):
    """Tests for the run_job function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.sample_job = {
            "id": "test_job_123",
            "input": {"prompt": "hello"},
        }

    async def test_sync_handler_success(self):
        """Test running a sync handler successfully."""

        def sync_handler(job):
            return {"result": job["input"]["prompt"].upper()}

        with patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job(sync_handler, self.sample_job)

        self.assertEqual(result, {"output": {"result": "HELLO"}})

    async def test_async_handler_success(self):
        """Test running an async handler successfully."""

        async def async_handler(job):
            return {"result": job["input"]["prompt"].upper()}

        with patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job(async_handler, self.sample_job)

        self.assertEqual(result, {"output": {"result": "HELLO"}})

    async def test_handler_returns_output_dict(self):
        """Test handler that returns dict with output key."""

        def handler(job):
            return {"output": "already wrapped"}

        with patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job(handler, self.sample_job)

        self.assertEqual(result, {"output": {"output": "already wrapped"}})

    async def test_handler_returns_error_dict(self):
        """Test handler that returns dict with error key."""

        def handler(job):
            return {"error": "something went wrong"}

        with patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job(handler, self.sample_job)

        self.assertIn("error", result)
        self.assertEqual(result["error"], "something went wrong")

    async def test_handler_returns_none(self):
        """Test handler that returns None."""

        def handler(job):
            return None

        with patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job(handler, self.sample_job)

        self.assertEqual(result, {"output": None})

    async def test_handler_returns_primitive(self):
        """Test handler that returns a primitive value."""

        def handler(job):
            return 42

        with patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job(handler, self.sample_job)

        self.assertEqual(result, {"output": 42})

    async def test_handler_returns_bool(self):
        """Test handler that returns a boolean."""

        def handler(job):
            return True

        with patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job(handler, self.sample_job)

        self.assertEqual(result, {"output": True})

    async def test_handler_raises_exception(self):
        """Test handler that raises an exception."""

        def handler(job):
            raise ValueError("Test error")

        with patch("wavespeed.serverless.modules.job.log"), patch(
            "wavespeed.serverless.modules.job.serverless"
        ) as mock_serverless:
            mock_serverless.pod_hostname = ""
            result = await run_job(handler, self.sample_job)

        self.assertIn("error", result)
        self.assertIn("Test error", result["error"])


class TestRunJobGenerator(IsolatedAsyncioTestCase):
    """Tests for the run_job_generator function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.sample_job = {
            "id": "gen_job_123",
            "input": {"count": 3},
        }
        self.mock_session = AsyncMock()

    async def test_sync_generator_success(self):
        """Test running a generator handler.

        Note: Using async generator due to Python asyncio limitations with
        StopIteration in run_in_executor for sync generators.
        """

        async def gen_handler(job):
            for i in range(3):
                yield f"output_{i}"

        with patch(
            "wavespeed.serverless.modules.job.stream_result",
            new_callable=AsyncMock,
        ) as mock_stream, patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job_generator(
                gen_handler, self.sample_job, self.mock_session
            )

            self.assertIn("_is_stream", result)
            self.assertEqual(mock_stream.call_count, 3)

    async def test_async_generator_success(self):
        """Test running an async generator handler."""

        async def async_gen_handler(job):
            for i in range(3):
                yield f"output_{i}"

        with patch(
            "wavespeed.serverless.modules.job.stream_result",
            new_callable=AsyncMock,
        ) as mock_stream, patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job_generator(
                async_gen_handler, self.sample_job, self.mock_session
            )

            self.assertIn("_is_stream", result)
            self.assertEqual(mock_stream.call_count, 3)

    async def test_generator_with_aggregate(self):
        """Test generator with return_aggregate=True."""

        async def gen_handler(job):
            yield "part1"
            yield "part2"

        with patch(
            "wavespeed.serverless.modules.job.stream_result",
            new_callable=AsyncMock,
        ), patch("wavespeed.serverless.modules.job.serverless"):
            result = await run_job_generator(
                gen_handler,
                self.sample_job,
                self.mock_session,
                return_aggregate=True,
            )

            self.assertEqual(result["output"], ["part1", "part2"])

    async def test_generator_exception(self):
        """Test generator that raises an exception."""

        async def gen_handler(job):
            yield "first"
            raise ValueError("Generator error")

        with patch(
            "wavespeed.serverless.modules.job.stream_result",
            new_callable=AsyncMock,
        ), patch("wavespeed.serverless.modules.job.log"), patch(
            "wavespeed.serverless.modules.job.serverless"
        ) as mock_serverless:
            mock_serverless.pod_hostname = ""
            result = await run_job_generator(
                gen_handler, self.sample_job, self.mock_session
            )

            self.assertIn("error", result)
            self.assertIn("Generator error", result["error"])


class TestHandleJob(IsolatedAsyncioTestCase):
    """Tests for the handle_job function."""

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.sample_job = {
            "id": "handle_job_123",
            "input": {"data": "test"},
        }
        self.mock_session = AsyncMock()

    async def test_handle_job_success(self):
        """Test successful job handling."""

        def handler(job):
            return {"result": "success"}

        config = {"handler": handler, "_is_generator": False}

        with patch(
            "wavespeed.serverless.modules.job.send_result",
            new_callable=AsyncMock,
        ) as mock_send, patch(
            "wavespeed.serverless.modules.job.get_jobs_progress"
        ) as mock_progress, patch(
            "wavespeed.serverless.modules.job.log"
        ), patch(
            "wavespeed.serverless.modules.job.serverless"
        ):
            mock_jobs = MagicMock()
            mock_progress.return_value = mock_jobs

            await handle_job(self.mock_session, config, self.sample_job)

            mock_jobs.add.assert_called_once_with(self.sample_job)
            mock_jobs.remove.assert_called_once_with(self.sample_job)
            mock_send.assert_called_once()

    async def test_handle_job_generator(self):
        """Test handling a generator job."""

        async def gen_handler(job):
            yield "part1"
            yield "part2"

        config = {"handler": gen_handler, "_is_generator": True}

        with patch(
            "wavespeed.serverless.modules.job.send_result",
            new_callable=AsyncMock,
        ), patch(
            "wavespeed.serverless.modules.job.stream_result",
            new_callable=AsyncMock,
        ) as mock_stream, patch(
            "wavespeed.serverless.modules.job.get_jobs_progress"
        ) as mock_progress, patch(
            "wavespeed.serverless.modules.job.log"
        ), patch(
            "wavespeed.serverless.modules.job.serverless"
        ):
            mock_jobs = MagicMock()
            mock_progress.return_value = mock_jobs

            await handle_job(self.mock_session, config, self.sample_job)

            self.assertEqual(mock_stream.call_count, 2)

    async def test_handle_job_refresh_worker(self):
        """Test job that requests worker refresh."""

        def handler(job):
            # Must include output or error key to avoid wrapping, plus refresh_worker
            return {"output": "done", "refresh_worker": True}

        config = {"handler": handler, "_is_generator": False}

        with patch(
            "wavespeed.serverless.modules.job.send_result",
            new_callable=AsyncMock,
        ), patch(
            "wavespeed.serverless.modules.job.get_jobs_progress"
        ) as mock_progress, patch(
            "wavespeed.serverless.modules.job.log"
        ), patch(
            "wavespeed.serverless.modules.job.serverless"
        ):
            mock_jobs = MagicMock()
            mock_progress.return_value = mock_jobs

            await handle_job(self.mock_session, config, self.sample_job)

            self.assertTrue(config.get("refresh_worker"))

    async def test_handle_job_exception(self):
        """Test job handling when an exception occurs."""

        def handler(job):
            raise RuntimeError("Handler failed")

        config = {"handler": handler, "_is_generator": False}

        with patch(
            "wavespeed.serverless.modules.job.send_result",
            new_callable=AsyncMock,
        ) as mock_send, patch(
            "wavespeed.serverless.modules.job.get_jobs_progress"
        ) as mock_progress, patch(
            "wavespeed.serverless.modules.job.log"
        ), patch(
            "wavespeed.serverless.modules.job.serverless"
        ) as mock_serverless:
            mock_serverless.pod_hostname = ""
            mock_jobs = MagicMock()
            mock_progress.return_value = mock_jobs

            await handle_job(self.mock_session, config, self.sample_job)

            # Job should still be removed from progress
            mock_jobs.remove.assert_called_once_with(self.sample_job)
            # Error result should be sent
            mock_send.assert_called()


if __name__ == "__main__":
    unittest.main()
