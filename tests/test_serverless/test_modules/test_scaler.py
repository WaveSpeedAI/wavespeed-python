"""Tests for the scaler module."""

import asyncio
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wavespeed.serverless.modules.scaler import JobScaler


class TestJobScaler(unittest.TestCase):
    """Tests for the JobScaler class initialization."""

    def test_initialization(self):
        """Test JobScaler initialization."""
        config = {"handler": lambda x: x}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress"):
            scaler = JobScaler(config)

            self.assertEqual(scaler.config, config)
            self.assertEqual(scaler.current_concurrency, 1)
            self.assertEqual(scaler.jobs_fetcher_timeout, 90)

    def test_initialization_with_concurrency_modifier(self):
        """Test JobScaler with concurrency modifier."""

        def modifier(current):
            return current * 2

        config = {"handler": lambda x: x, "concurrency_modifier": modifier}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress"):
            scaler = JobScaler(config)

            self.assertIsNotNone(scaler.concurrency_modifier)

    def test_is_alive(self):
        """Test is_alive method."""
        config = {"handler": lambda x: x}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress"):
            scaler = JobScaler(config)

            self.assertTrue(scaler.is_alive())

    def test_kill_worker(self):
        """Test kill_worker method."""
        config = {"handler": lambda x: x}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress"), patch(
            "wavespeed.serverless.modules.scaler.log"
        ):
            scaler = JobScaler(config)
            scaler.kill_worker()

            self.assertFalse(scaler.is_alive())


class TestJobScalerAsync(IsolatedAsyncioTestCase):
    """Async tests for the JobScaler class."""

    async def test_set_scale(self):
        """Test set_scale method."""
        config = {"handler": lambda x: x}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress") as mock_progress:
            mock_progress_instance = MagicMock()
            mock_progress_instance.get_job_count.return_value = 0
            mock_progress.return_value = mock_progress_instance

            scaler = JobScaler(config)
            scaler.concurrency_modifier = lambda x: 5

            with patch("wavespeed.serverless.modules.scaler.log"):
                await scaler.set_scale()

            self.assertEqual(scaler.current_concurrency, 5)

    async def test_current_occupancy(self):
        """Test current_occupancy method."""
        config = {"handler": lambda x: x}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress") as mock_progress:
            mock_progress_instance = MagicMock()
            mock_progress_instance.get_job_count.return_value = 2
            mock_progress.return_value = mock_progress_instance

            scaler = JobScaler(config)
            scaler.job_progress = mock_progress_instance

            with patch("wavespeed.serverless.modules.scaler.log"):
                occupancy = scaler.current_occupancy()

            # Queue is empty (0) + progress count (2) = 2
            self.assertEqual(occupancy, 2)

    async def test_handle_job(self):
        """Test handle_job method."""
        config = {"handler": lambda x: {"output": "test"}}

        mock_session = AsyncMock()
        job = {"id": "test_job", "input": {}}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress") as mock_progress:
            mock_progress_instance = MagicMock()
            mock_progress.return_value = mock_progress_instance

            scaler = JobScaler(config)
            scaler.job_progress = mock_progress_instance
            scaler.jobs_queue = MagicMock()

            # Override the jobs_handler directly since __init__ already captured handle_job
            mock_handle = AsyncMock()
            scaler.jobs_handler = mock_handle

            with patch("wavespeed.serverless.modules.scaler.log"):
                await scaler.handle_job(mock_session, job)

                mock_handle.assert_called_once_with(mock_session, config, job)
                mock_progress_instance.remove.assert_called_once_with(job)

    async def test_handle_job_exception(self):
        """Test handle_job handles exceptions."""
        config = {"handler": lambda x: x}
        mock_session = AsyncMock()
        job = {"id": "test_job", "input": {}}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress") as mock_progress:
            mock_progress_instance = MagicMock()
            mock_progress.return_value = mock_progress_instance

            scaler = JobScaler(config)
            scaler.job_progress = mock_progress_instance
            scaler.jobs_queue = MagicMock()

            # Override the jobs_handler directly since __init__ already captured handle_job
            mock_handle = AsyncMock()
            mock_handle.side_effect = RuntimeError("Job failed")
            scaler.jobs_handler = mock_handle

            with patch("wavespeed.serverless.modules.scaler.log"):
                with self.assertRaises(RuntimeError):
                    await scaler.handle_job(mock_session, job)

                # Job should still be removed from progress
                mock_progress_instance.remove.assert_called_once_with(job)

    async def test_get_jobs_shutdown(self):
        """Test get_jobs stops on shutdown."""
        config = {"handler": lambda x: x}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress") as mock_progress:
            mock_progress_instance = MagicMock()
            mock_progress.return_value = mock_progress_instance

            scaler = JobScaler(config)
            scaler.kill_worker()

            mock_session = AsyncMock()

            with patch("wavespeed.serverless.modules.scaler.log"):
                # Should return immediately without fetching
                await scaler.get_jobs(mock_session)

    async def test_run_jobs_shutdown(self):
        """Test run_jobs stops on shutdown."""
        config = {"handler": lambda x: x}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress"):
            scaler = JobScaler(config)
            scaler.kill_worker()

            mock_session = AsyncMock()

            with patch("wavespeed.serverless.modules.scaler.log"):
                # Should return immediately
                await scaler.run_jobs(mock_session)

    async def test_jobs_queue(self):
        """Test jobs are properly queued."""
        config = {"handler": lambda x: x}

        with patch("wavespeed.serverless.modules.scaler.JobsProgress"):
            scaler = JobScaler(config)

            job = {"id": "queue_test", "input": {}}
            await scaler.jobs_queue.put(job)

            self.assertEqual(scaler.jobs_queue.qsize(), 1)
            retrieved = await scaler.jobs_queue.get()
            self.assertEqual(retrieved["id"], "queue_test")


class TestJobScalerIntegration(IsolatedAsyncioTestCase):
    """Integration tests for JobScaler."""

    async def test_get_jobs_queues_jobs(self):
        """Test getting jobs and queuing them."""
        config = {"handler": lambda x: {"output": "test"}}

        mock_session = AsyncMock()

        with patch("wavespeed.serverless.modules.scaler.JobsProgress") as mock_progress:
            mock_progress_instance = MagicMock()
            mock_progress_instance.get_job_count.return_value = 0
            mock_progress.return_value = mock_progress_instance

            scaler = JobScaler(config)
            scaler.job_progress = mock_progress_instance

            # Increase concurrency to allow for 2 calls
            scaler.current_concurrency = 2
            scaler.jobs_queue = asyncio.Queue(maxsize=2)

            # Return 1 job first, then 1 more job and shutdown
            call_count = {"count": 0}

            async def mock_get_job_impl(session, num_jobs):
                call_count["count"] += 1
                if call_count["count"] == 1:
                    return [{"id": "job_1", "input": {}}]
                if call_count["count"] == 2:
                    scaler.kill_worker()
                    return [{"id": "job_2", "input": {}}]
                return None

            # Override the jobs_fetcher directly since __init__ already captured get_job
            scaler.jobs_fetcher = mock_get_job_impl

            with patch("wavespeed.serverless.modules.scaler.log"):
                # Run get_jobs loop (will exit after 2nd call due to shutdown)
                await scaler.get_jobs(mock_session)

                self.assertEqual(scaler.jobs_queue.qsize(), 2)


if __name__ == "__main__":
    unittest.main()
