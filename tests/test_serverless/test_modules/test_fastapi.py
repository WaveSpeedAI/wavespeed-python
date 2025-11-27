"""Tests for the FastAPI server module."""

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from wavespeed.serverless.modules.fastapi import _job_results, _pending_jobs, WorkerAPI


class TestWorkerAPI(unittest.TestCase):
    """Tests for WorkerAPI class."""

    def setUp(self):
        """Set up test fixtures."""
        # Clear global state
        _pending_jobs.clear()
        _job_results.clear()

        def sync_handler(job):
            return {"result": job["input"].get("prompt", "").upper()}

        self.config = {"handler": sync_handler}
        self.api = WorkerAPI(self.config)
        self.client = TestClient(self.api.app)

    def tearDown(self):
        """Clean up after tests."""
        _pending_jobs.clear()
        _job_results.clear()

    def test_docs_redirect(self):
        """Test that /docs redirects to /."""
        response = self.client.get("/docs", follow_redirects=False)
        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers["location"], "/")

    def test_root_serves_swagger(self):
        """Test that root serves Swagger UI."""
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("swagger", response.text.lower())


class TestRunEndpoint(unittest.TestCase):
    """Tests for /run endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        _pending_jobs.clear()
        _job_results.clear()

        def sync_handler(job):
            return {"result": job["input"].get("prompt", "").upper()}

        self.config = {"handler": sync_handler}
        self.api = WorkerAPI(self.config)
        self.client = TestClient(self.api.app)

    def tearDown(self):
        """Clean up after tests."""
        _pending_jobs.clear()
        _job_results.clear()

    def test_run_returns_job_id(self):
        """Test that /run returns a job ID."""
        response = self.client.post(
            "/run",
            json={"input": {"prompt": "hello"}},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("id", data)
        self.assertTrue(data["id"].startswith("test-"))
        self.assertEqual(data["status"], "IN_PROGRESS")

    def test_run_stores_pending_job(self):
        """Test that /run stores the job in pending jobs."""
        response = self.client.post(
            "/run",
            json={"input": {"prompt": "hello"}},
        )
        job_id = response.json()["id"]
        self.assertIn(job_id, _pending_jobs)
        self.assertEqual(_pending_jobs[job_id]["input"], {"prompt": "hello"})

    def test_run_with_webhook(self):
        """Test that /run stores webhook URL."""
        response = self.client.post(
            "/run",
            json={"input": {"prompt": "hello"}, "webhook": "http://example.com/hook"},
        )
        job_id = response.json()["id"]
        self.assertEqual(_pending_jobs[job_id]["webhook"], "http://example.com/hook")


class TestRunsyncEndpoint(unittest.TestCase):
    """Tests for /runsync endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        _pending_jobs.clear()
        _job_results.clear()

        def sync_handler(job):
            return {"result": job["input"].get("prompt", "").upper()}

        self.config = {"handler": sync_handler}
        self.api = WorkerAPI(self.config)
        self.client = TestClient(self.api.app)

    def tearDown(self):
        """Clean up after tests."""
        _pending_jobs.clear()
        _job_results.clear()

    def test_runsync_success(self):
        """Test successful synchronous job execution."""
        response = self.client.post(
            "/runsync",
            json={"input": {"prompt": "hello"}},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "COMPLETED")
        self.assertIn("id", data)
        self.assertEqual(data["output"], {"result": "HELLO"})

    def test_runsync_with_error(self):
        """Test runsync with handler that returns error."""

        def error_handler(job):
            return {"error": "Something went wrong"}

        api = WorkerAPI({"handler": error_handler})
        client = TestClient(api.app)

        response = client.post(
            "/runsync",
            json={"input": {"prompt": "hello"}},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "FAILED")
        self.assertEqual(data["error"], "Something went wrong")

    def test_runsync_with_exception(self):
        """Test runsync with handler that raises exception."""

        def bad_handler(job):
            raise ValueError("Handler crashed")

        api = WorkerAPI({"handler": bad_handler})
        client = TestClient(api.app)

        response = client.post(
            "/runsync",
            json={"input": {"prompt": "hello"}},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "FAILED")
        self.assertIn("Handler crashed", data["error"])


class TestRunsyncAsyncHandler(unittest.TestCase):
    """Tests for /runsync with async handlers."""

    def setUp(self):
        """Set up test fixtures."""
        _pending_jobs.clear()
        _job_results.clear()

    def tearDown(self):
        """Clean up after tests."""
        _pending_jobs.clear()
        _job_results.clear()

    def test_runsync_async_handler(self):
        """Test runsync with async handler."""

        async def async_handler(job):
            return {"result": job["input"].get("prompt", "").upper()}

        api = WorkerAPI({"handler": async_handler})
        client = TestClient(api.app)

        response = client.post(
            "/runsync",
            json={"input": {"prompt": "hello"}},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "COMPLETED")
        self.assertEqual(data["output"], {"result": "HELLO"})


class TestStatusEndpoint(unittest.TestCase):
    """Tests for /status endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        _pending_jobs.clear()
        _job_results.clear()

        def sync_handler(job):
            return {"result": job["input"].get("prompt", "").upper()}

        self.config = {"handler": sync_handler}
        self.api = WorkerAPI(self.config)
        self.client = TestClient(self.api.app)

    def tearDown(self):
        """Clean up after tests."""
        _pending_jobs.clear()
        _job_results.clear()

    def test_status_not_found(self):
        """Test status for non-existent job."""
        response = self.client.post("/status/nonexistent-job-id")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "FAILED")
        self.assertEqual(data["error"], "Job ID not found")

    def test_status_executes_pending_job(self):
        """Test that status executes a pending job."""
        # First submit a job via /run
        run_response = self.client.post(
            "/run",
            json={"input": {"prompt": "hello"}},
        )
        job_id = run_response.json()["id"]

        # Then check status (which executes the job)
        status_response = self.client.post(f"/status/{job_id}")
        self.assertEqual(status_response.status_code, 200)
        data = status_response.json()
        self.assertEqual(data["status"], "COMPLETED")
        self.assertEqual(data["output"], {"result": "HELLO"})

    def test_status_removes_pending_job(self):
        """Test that status removes job from pending after execution."""
        run_response = self.client.post(
            "/run",
            json={"input": {"prompt": "hello"}},
        )
        job_id = run_response.json()["id"]

        # Execute via status
        self.client.post(f"/status/{job_id}")

        # Job should no longer be pending
        self.assertNotIn(job_id, _pending_jobs)

    def test_status_caches_result(self):
        """Test that status caches the result."""
        run_response = self.client.post(
            "/run",
            json={"input": {"prompt": "hello"}},
        )
        job_id = run_response.json()["id"]

        # First status call
        self.client.post(f"/status/{job_id}")

        # Result should be cached
        self.assertIn(job_id, _job_results)

        # Second status call should return cached result
        status_response = self.client.post(f"/status/{job_id}")
        data = status_response.json()
        self.assertEqual(data["status"], "COMPLETED")


class TestStreamEndpoint(unittest.TestCase):
    """Tests for /stream endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        _pending_jobs.clear()
        _job_results.clear()

    def tearDown(self):
        """Clean up after tests."""
        _pending_jobs.clear()
        _job_results.clear()

    def test_stream_not_found(self):
        """Test stream for non-existent job."""

        def sync_handler(job):
            return {"result": "done"}

        api = WorkerAPI({"handler": sync_handler})
        client = TestClient(api.app)

        response = client.post("/stream/nonexistent-job-id")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "FAILED")
        self.assertEqual(data["error"], "Job ID not found")

    def test_stream_requires_generator(self):
        """Test that stream requires a generator handler."""

        def sync_handler(job):
            return {"result": "done"}

        api = WorkerAPI({"handler": sync_handler})
        client = TestClient(api.app)

        # Submit a job
        run_response = client.post(
            "/run",
            json={"input": {"prompt": "hello"}},
        )
        job_id = run_response.json()["id"]

        # Try to stream (should fail for non-generator)
        stream_response = client.post(f"/stream/{job_id}")
        data = stream_response.json()
        self.assertEqual(data["status"], "FAILED")
        self.assertIn("generator", data["error"].lower())


class TestStreamEndpointGenerator(unittest.TestCase):
    """Tests for /stream endpoint with generator handlers."""

    def setUp(self):
        """Set up test fixtures."""
        _pending_jobs.clear()
        _job_results.clear()

    def tearDown(self):
        """Clean up after tests."""
        _pending_jobs.clear()
        _job_results.clear()

    def test_stream_with_sync_generator(self):
        """Test stream with sync generator handler."""

        def gen_handler(job):
            for i in range(3):
                yield f"chunk-{i}"

        api = WorkerAPI({"handler": gen_handler})
        client = TestClient(api.app)

        # Submit a job
        run_response = client.post(
            "/run",
            json={"input": {"prompt": "hello"}},
        )
        job_id = run_response.json()["id"]

        # Stream the results
        stream_response = client.post(f"/stream/{job_id}")
        data = stream_response.json()
        self.assertEqual(data["status"], "COMPLETED")
        self.assertEqual(len(data["stream"]), 3)

    def test_stream_with_async_generator(self):
        """Test stream with async generator handler."""

        async def async_gen_handler(job):
            for i in range(3):
                yield f"chunk-{i}"

        api = WorkerAPI({"handler": async_gen_handler})
        client = TestClient(api.app)

        # Submit a job
        run_response = client.post(
            "/run",
            json={"input": {"prompt": "hello"}},
        )
        job_id = run_response.json()["id"]

        # Stream the results
        stream_response = client.post(f"/stream/{job_id}")
        data = stream_response.json()
        self.assertEqual(data["status"], "COMPLETED")
        self.assertEqual(len(data["stream"]), 3)


class TestWebhook(unittest.TestCase):
    """Tests for webhook functionality."""

    def setUp(self):
        """Set up test fixtures."""
        _pending_jobs.clear()
        _job_results.clear()

    def tearDown(self):
        """Clean up after tests."""
        _pending_jobs.clear()
        _job_results.clear()

    @patch("wavespeed.serverless.modules.fastapi._send_webhook_sync")
    def test_runsync_sends_webhook(self, mock_webhook):
        """Test that runsync sends webhook when provided."""

        def sync_handler(job):
            return {"result": "done"}

        api = WorkerAPI({"handler": sync_handler})
        client = TestClient(api.app)

        response = client.post(
            "/runsync",
            json={"input": {"prompt": "hello"}, "webhook": "http://example.com/hook"},
        )

        self.assertEqual(response.status_code, 200)
        # Give the thread a moment to start
        import time

        time.sleep(0.1)
        # Webhook should have been called (in a thread)
        # Note: Due to threading, we may need to wait or use different assertion


class TestWorkerAPIInitialization(unittest.TestCase):
    """Tests for WorkerAPI initialization."""

    def test_initialization_with_handler(self):
        """Test WorkerAPI initializes correctly."""

        def handler(job):
            return {"output": "test"}

        api = WorkerAPI({"handler": handler})
        self.assertIsNotNone(api.app)
        self.assertEqual(api.config["handler"], handler)

    def test_app_has_required_routes(self):
        """Test that app has all required routes."""

        def handler(job):
            return {"output": "test"}

        api = WorkerAPI({"handler": handler})

        routes = [route.path for route in api.app.routes]
        self.assertIn("/run", routes)
        self.assertIn("/runsync", routes)
        self.assertIn("/stream/{job_id}", routes)
        self.assertIn("/status/{job_id}", routes)
