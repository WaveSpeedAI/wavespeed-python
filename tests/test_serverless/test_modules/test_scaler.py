"""Tests for the scaler module."""

import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from wavespeed.serverless.modules.scaler import JobScaler
from wavespeed.serverless.modules.state import Job


class TestJobScaler(unittest.TestCase):
    """Tests for the JobScaler class initialization."""

    def test_initialization(self):
        """Test JobScaler initialization."""
        config = {"handler": lambda x: x}

        scaler = JobScaler(config)

        self.assertEqual(scaler.config, config)
        self.assertEqual(scaler.current_concurrency, 1)
        self.assertEqual(scaler.max_concurrency, 1)
        self.assertFalse(scaler._shutdown)
        self.assertEqual(scaler._active_jobs, 0)

    def test_initialization_with_concurrency_modifier(self):
        """Test JobScaler with concurrency modifier."""

        def modifier(current):
            return current * 2

        config = {"handler": lambda x: x, "concurrency_modifier": modifier}

        scaler = JobScaler(config)

        self.assertIsNotNone(scaler._concurrency_modifier)

    def test_set_scale(self):
        """Test set_scale method."""
        config = {"handler": lambda x: x}
        scaler = JobScaler(config)

        with patch("wavespeed.serverless.modules.scaler.log"):
            scaler.set_scale(5)

        self.assertEqual(scaler.current_concurrency, 5)
        self.assertEqual(scaler.max_concurrency, 5)

    def test_set_scale_minimum(self):
        """Test set_scale enforces minimum of 1."""
        config = {"handler": lambda x: x}
        scaler = JobScaler(config)

        with patch("wavespeed.serverless.modules.scaler.log"):
            scaler.set_scale(0)

        self.assertEqual(scaler.current_concurrency, 1)

    def test_kill_worker(self):
        """Test kill_worker method."""
        config = {"handler": lambda x: x}
        scaler = JobScaler(config)

        with patch("wavespeed.serverless.modules.scaler.log"):
            scaler.kill_worker()

        self.assertTrue(scaler._shutdown)
        self.assertTrue(config.get("_shutdown"))


class TestJobScalerAsync(IsolatedAsyncioTestCase):
    """Async tests for the JobScaler class."""

    async def test_update_concurrency(self):
        """Test _update_concurrency with modifier."""
        call_count = {"count": 0}

        def modifier(current):
            call_count["count"] += 1
            return current + 1

        config = {"handler": lambda x: x, "concurrency_modifier": modifier}
        scaler = JobScaler(config)

        with patch("wavespeed.serverless.modules.scaler.log"):
            scaler._update_concurrency()

        self.assertEqual(scaler.current_concurrency, 2)
        self.assertEqual(call_count["count"], 1)

    async def test_update_concurrency_no_modifier(self):
        """Test _update_concurrency without modifier."""
        config = {"handler": lambda x: x}
        scaler = JobScaler(config)

        scaler._update_concurrency()

        self.assertEqual(scaler.current_concurrency, 1)

    async def test_update_concurrency_modifier_exception(self):
        """Test _update_concurrency handles modifier exceptions."""

        def bad_modifier(current):
            raise ValueError("Modifier failed")

        config = {"handler": lambda x: x, "concurrency_modifier": bad_modifier}
        scaler = JobScaler(config)

        with patch("wavespeed.serverless.modules.scaler.log"):
            scaler._update_concurrency()

        # Concurrency should remain unchanged
        self.assertEqual(scaler.current_concurrency, 1)

    async def test_handle_job_wrapper(self):
        """Test _handle_job_wrapper method."""
        config = {"handler": lambda x: {"output": "test"}}
        scaler = JobScaler(config)
        mock_session = AsyncMock()
        job = Job(id="test_job", input={})

        with patch(
            "wavespeed.serverless.modules.scaler.handle_job", new_callable=AsyncMock
        ) as mock_handle:
            await scaler._handle_job_wrapper(mock_session, job)

            mock_handle.assert_called_once_with(mock_session, config, job)

    async def test_handle_job_wrapper_exception(self):
        """Test _handle_job_wrapper handles exceptions."""
        config = {"handler": lambda x: x}
        scaler = JobScaler(config)
        mock_session = AsyncMock()
        job = Job(id="test_job", input={})

        with patch(
            "wavespeed.serverless.modules.scaler.handle_job", new_callable=AsyncMock
        ) as mock_handle, patch("wavespeed.serverless.modules.scaler.log"):
            mock_handle.side_effect = RuntimeError("Job failed")

            # Should not raise
            await scaler._handle_job_wrapper(mock_session, job)

    async def test_fetch_jobs_loop_shutdown(self):
        """Test _fetch_jobs_loop stops on shutdown."""
        config = {"handler": lambda x: x}
        scaler = JobScaler(config)
        scaler._shutdown = True

        mock_session = AsyncMock()

        # Should return immediately without fetching
        await scaler._fetch_jobs_loop(mock_session)

    async def test_run_jobs_loop_shutdown(self):
        """Test _run_jobs_loop stops on shutdown."""
        config = {"handler": lambda x: x}
        scaler = JobScaler(config)
        scaler._shutdown = True

        mock_session = AsyncMock()

        # Should return immediately
        await scaler._run_jobs_loop(mock_session)

    async def test_jobs_queue(self):
        """Test jobs are properly queued."""
        config = {"handler": lambda x: x}
        scaler = JobScaler(config)

        job = Job(id="queue_test", input={})
        await scaler.jobs_queue.put(job)

        self.assertEqual(scaler.jobs_queue.qsize(), 1)
        retrieved = await scaler.jobs_queue.get()
        self.assertEqual(retrieved.id, "queue_test")


class TestJobScalerIntegration(IsolatedAsyncioTestCase):
    """Integration tests for JobScaler."""

    async def test_fetch_and_queue_jobs(self):
        """Test fetching jobs and queuing them."""
        config = {"handler": lambda x: {"output": "test"}}
        scaler = JobScaler(config)
        scaler.max_concurrency = 5

        mock_session = AsyncMock()

        with patch(
            "wavespeed.serverless.modules.scaler.get_job", new_callable=AsyncMock
        ) as mock_get_job, patch("wavespeed.serverless.modules.scaler.log"):
            # Return jobs once, then trigger shutdown
            call_count = {"count": 0}

            async def mock_get_job_impl(session, num_jobs):
                call_count["count"] += 1
                if call_count["count"] == 1:
                    return [
                        Job(id="job_1", input={}),
                        Job(id="job_2", input={}),
                    ]
                scaler._shutdown = True
                return []

            mock_get_job.side_effect = mock_get_job_impl

            # Run fetch loop (will exit after 2nd call due to shutdown)
            await scaler._fetch_jobs_loop(mock_session)

            self.assertEqual(scaler.jobs_queue.qsize(), 2)


if __name__ == "__main__":
    unittest.main()
