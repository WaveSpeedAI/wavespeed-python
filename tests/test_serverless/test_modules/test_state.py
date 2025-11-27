"""Tests for the state module."""

import os
import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase

from wavespeed.serverless.modules.state import (
    Job,
    JobsProgress,
    get_worker_id,
    set_worker_id,
    get_jobs_progress,
)


class TestWorkerID(unittest.TestCase):
    """Tests for worker ID functions."""

    def setUp(self):
        """Reset worker ID before each test."""
        import wavespeed.serverless.modules.state as state

        state._worker_id = None

    def test_get_worker_id_generates_uuid(self):
        """Test that get_worker_id generates a UUID when not set."""
        # Clear any existing environment variable
        os.environ.pop("WAVERLESS_WORKER_ID", None)

        worker_id = get_worker_id()
        self.assertIsNotNone(worker_id)
        self.assertIsInstance(worker_id, str)
        self.assertTrue(len(worker_id) > 0)

    def test_get_worker_id_from_env(self):
        """Test that get_worker_id reads from environment."""
        import wavespeed.serverless.modules.state as state

        state._worker_id = None
        os.environ["WAVERLESS_WORKER_ID"] = "test-worker-123"

        worker_id = get_worker_id()
        self.assertEqual(worker_id, "test-worker-123")

        # Cleanup
        os.environ.pop("WAVERLESS_WORKER_ID", None)

    def test_set_worker_id(self):
        """Test that set_worker_id sets the worker ID."""
        set_worker_id("custom-worker-id")
        self.assertEqual(get_worker_id(), "custom-worker-id")


class TestJob(unittest.TestCase):
    """Tests for the Job dataclass."""

    def test_initialization_with_basic_attributes(self):
        """Test basic initialization of Job object."""
        job = Job(
            id="job_123",
            input={"task": "data_process"},
            webhook="http://example.com/webhook",
        )
        self.assertEqual(job.id, "job_123")
        self.assertEqual(job.input, {"task": "data_process"})
        self.assertEqual(job.webhook, "http://example.com/webhook")

    def test_initialization_without_webhook(self):
        """Test initialization without webhook."""
        job = Job(id="job_123", input={"task": "process"})
        self.assertEqual(job.id, "job_123")
        self.assertEqual(job.input, {"task": "process"})
        self.assertIsNone(job.webhook)

    def test_equality(self):
        """Test equality between two Job objects based on the job ID."""
        job1 = Job(id="job_123", input={})
        job2 = Job(id="job_123", input={"different": "input"})
        job3 = Job(id="job_456", input={})

        self.assertEqual(job1, job2)
        self.assertNotEqual(job1, job3)

    def test_hashing(self):
        """Test hashing of Job object based on the job ID."""
        job1 = Job(id="job_123", input={})
        job2 = Job(id="job_123", input={"different": "input"})
        job3 = Job(id="job_456", input={})

        self.assertEqual(hash(job1), hash(job2))
        self.assertNotEqual(hash(job1), hash(job3))

    def test_job_in_set(self):
        """Test that jobs can be used in sets."""
        job1 = Job(id="job_123", input={})
        job2 = Job(id="job_123", input={"different": "input"})
        job3 = Job(id="job_456", input={})

        job_set = {job1, job2, job3}
        self.assertEqual(len(job_set), 2)  # job1 and job2 are equal

    def test_equality_with_non_job(self):
        """Test equality with non-Job objects."""
        job = Job(id="job_123", input={})
        self.assertNotEqual(job, "job_123")
        self.assertNotEqual(job, {"id": "job_123"})
        self.assertNotEqual(job, None)


class TestJobsProgress(unittest.TestCase):
    """Tests for the JobsProgress class."""

    def setUp(self):
        """Clear jobs progress before each test."""
        # Reset singleton instance
        JobsProgress._instance = None
        # Remove pickle file if exists
        pickle_path = Path("/tmp/waverless_jobs_progress.pkl")
        if pickle_path.exists():
            pickle_path.unlink()

        self.jobs = JobsProgress()

    def tearDown(self):
        """Clean up after each test."""
        self.jobs.clear()
        JobsProgress._instance = None

    def test_singleton(self):
        """Test that JobsProgress is a singleton."""
        jobs2 = JobsProgress()
        self.assertIs(self.jobs, jobs2)

    def test_add_job(self):
        """Test adding jobs to progress tracker."""
        self.assertEqual(len(self.jobs), 0)

        self.jobs.add("job_123")
        self.assertEqual(len(self.jobs), 1)
        self.assertIn("job_123", self.jobs)

        self.jobs.add("job_456")
        self.assertEqual(len(self.jobs), 2)
        self.assertIn("job_456", self.jobs)

    def test_remove_job(self):
        """Test removing jobs from progress tracker."""
        self.jobs.add("job_123")
        self.assertEqual(len(self.jobs), 1)

        self.jobs.remove("job_123")
        self.assertEqual(len(self.jobs), 0)
        self.assertNotIn("job_123", self.jobs)

    def test_remove_nonexistent_job(self):
        """Test removing a job that doesn't exist doesn't raise."""
        self.jobs.remove("nonexistent")  # Should not raise

    def test_contains(self):
        """Test contains method."""
        self.jobs.add("job_123")
        self.assertTrue(self.jobs.contains("job_123"))
        self.assertFalse(self.jobs.contains("job_456"))

    def test_get_all(self):
        """Test get_all method."""
        self.jobs.add("job_123")
        self.jobs.add("job_456")

        all_jobs = self.jobs.get_all()
        self.assertEqual(all_jobs, {"job_123", "job_456"})

        # Verify it's a copy
        all_jobs.add("job_789")
        self.assertNotIn("job_789", self.jobs)

    def test_clear(self):
        """Test clearing all jobs."""
        self.jobs.add("job_123")
        self.jobs.add("job_456")
        self.assertEqual(len(self.jobs), 2)

        self.jobs.clear()
        self.assertEqual(len(self.jobs), 0)



class TestGetJobsProgress(unittest.TestCase):
    """Tests for the get_jobs_progress function."""

    def setUp(self):
        """Reset singleton before each test."""
        JobsProgress._instance = None
        pickle_path = Path("/tmp/waverless_jobs_progress.pkl")
        if pickle_path.exists():
            pickle_path.unlink()

    def tearDown(self):
        """Clean up after tests."""
        jobs = get_jobs_progress()
        jobs.clear()
        JobsProgress._instance = None

    def test_returns_singleton(self):
        """Test that get_jobs_progress returns the singleton."""
        jobs1 = get_jobs_progress()
        jobs2 = get_jobs_progress()
        self.assertIs(jobs1, jobs2)


if __name__ == "__main__":
    unittest.main()
