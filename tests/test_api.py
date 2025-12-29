"""Tests for the wavespeed.api module."""

import io
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import wavespeed
from wavespeed.api import Client


class TestClient(unittest.TestCase):
    """Tests for the Client class."""

    def test_init_with_api_key(self):
        """Test client initialization with explicit API key."""
        client = Client(api_key="test-key")
        self.assertEqual(client.api_key, "test-key")
        self.assertEqual(client.base_url, "https://api.wavespeed.ai")

    def test_init_with_custom_base_url(self):
        """Test client initialization with custom base URL."""
        client = Client(api_key="test-key", base_url="https://custom.api.com/")
        self.assertEqual(client.base_url, "https://custom.api.com")

    @patch("wavespeed.api.client.api_config")
    def test_init_from_config(self, mock_config):
        """Test client initialization from config."""
        mock_config.api_key = "config-key"
        mock_config.base_url = "https://api.wavespeed.ai"
        mock_config.connection_timeout = 10.0
        mock_config.max_retries = 0
        mock_config.max_connection_retries = 5
        mock_config.retry_interval = 1.0
        client = Client()
        self.assertEqual(client.api_key, "config-key")

    def test_get_headers_raises_without_api_key(self):
        """Test that _get_headers raises ValueError without API key."""
        client = Client()
        client.api_key = None
        with self.assertRaises(ValueError) as ctx:
            client._get_headers()
        self.assertIn("API key is required", str(ctx.exception))

    def test_get_headers_returns_auth_header(self):
        """Test that _get_headers returns proper authorization header."""
        client = Client(api_key="test-key")
        headers = client._get_headers()
        self.assertEqual(headers["Authorization"], "Bearer test-key")
        self.assertEqual(headers["Content-Type"], "application/json")

    @patch("wavespeed.api.client.requests.post")
    def test_submit_success(self, mock_post):
        """Test successful prediction submission."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"id": "req-123"}}
        mock_post.return_value = mock_response

        client = Client(api_key="test-key")
        request_id, result = client._submit(
            "wavespeed-ai/z-image/turbo", {"prompt": "test"}
        )

        self.assertEqual(request_id, "req-123")
        self.assertIsNone(result)
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("wavespeed-ai/z-image/turbo", call_args[0][0])

    @patch("wavespeed.api.client.requests.post")
    def test_submit_failure(self, mock_post):
        """Test prediction submission failure."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        client = Client(api_key="test-key")
        with self.assertRaises(RuntimeError) as ctx:
            client._submit("wavespeed-ai/z-image/turbo", {"prompt": "test"})
        self.assertIn("HTTP 500", str(ctx.exception))

    @patch("wavespeed.api.client.requests.get")
    def test_get_result_success(self, mock_get):
        """Test successful result retrieval."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"status": "completed", "outputs": ["https://example.com/out.png"]}
        }
        mock_get.return_value = mock_response

        client = Client(api_key="test-key")
        result = client._get_result("req-123")

        self.assertEqual(result["data"]["status"], "completed")
        mock_get.assert_called_once()

    @patch("wavespeed.api.client.requests.get")
    @patch("wavespeed.api.client.requests.post")
    def test_run_success(self, mock_post, mock_get):
        """Test successful run() call."""
        # Mock submission
        mock_post_response = MagicMock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {"data": {"id": "req-123"}}
        mock_post.return_value = mock_post_response

        # Mock result polling
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "data": {"status": "completed", "outputs": ["https://example.com/out.png"]}
        }
        mock_get.return_value = mock_get_response

        client = Client(api_key="test-key")
        result = client.run("wavespeed-ai/z-image/turbo", {"prompt": "test"})

        self.assertEqual(result["outputs"], ["https://example.com/out.png"])

    @patch("wavespeed.api.client.requests.get")
    @patch("wavespeed.api.client.requests.post")
    def test_run_failure(self, mock_post, mock_get):
        """Test run() with failed prediction."""
        # Mock submission
        mock_post_response = MagicMock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {"data": {"id": "req-123"}}
        mock_post.return_value = mock_post_response

        # Mock failed result
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "data": {"status": "failed", "error": "Model error"}
        }
        mock_get.return_value = mock_get_response

        client = Client(api_key="test-key")
        with self.assertRaises(RuntimeError) as ctx:
            client.run("wavespeed-ai/z-image/turbo", {"prompt": "test"})
        self.assertIn("Model error", str(ctx.exception))

    @patch("wavespeed.api.client.time.time")
    @patch("wavespeed.api.client.time.sleep")
    @patch("wavespeed.api.client.requests.get")
    @patch("wavespeed.api.client.requests.post")
    def test_run_timeout(self, mock_post, mock_get, mock_sleep, mock_time):
        """Test run() with timeout."""
        # Mock submission
        mock_post_response = MagicMock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {"data": {"id": "req-123"}}
        mock_post.return_value = mock_post_response

        # Mock pending result
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {"data": {"status": "pending"}}
        mock_get.return_value = mock_get_response

        # Simulate time passing beyond timeout
        mock_time.side_effect = [0, 0, 5, 10, 15]

        client = Client(api_key="test-key")
        with self.assertRaises(TimeoutError) as ctx:
            client.run("wavespeed-ai/z-image/turbo", {"prompt": "test"}, timeout=10)
        self.assertIn("timed out", str(ctx.exception))


class TestModuleLevelRun(unittest.TestCase):
    """Tests for the module-level run() function."""

    @patch("wavespeed.api.client.api_config")
    @patch("wavespeed.api.client.requests.get")
    @patch("wavespeed.api.client.requests.post")
    def test_run_uses_default_client(self, mock_post, mock_get, mock_config):
        """Test that module-level run() uses default client."""
        # Mock config
        mock_config.api_key = "config-key"
        mock_config.base_url = "https://api.wavespeed.ai"
        mock_config.connection_timeout = 10.0
        mock_config.timeout = 36000.0
        mock_config.max_retries = 0
        mock_config.max_connection_retries = 5
        mock_config.retry_interval = 1.0

        # Reset default client
        wavespeed.api._default_client = None

        # Mock submission
        mock_post_response = MagicMock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {"data": {"id": "req-123"}}
        mock_post.return_value = mock_post_response

        # Mock result
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "data": {"status": "completed", "outputs": ["https://example.com/out.png"]}
        }
        mock_get.return_value = mock_get_response

        result = wavespeed.run("wavespeed-ai/z-image/turbo", {"prompt": "test"})
        self.assertEqual(result["outputs"], ["https://example.com/out.png"])


class TestUpload(unittest.TestCase):
    """Tests for the upload functionality."""

    @patch("wavespeed.api.client.requests.post")
    def test_upload_file_path(self, mock_post):
        """Test uploading a file by path."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "code": 200,
            "message": "success",
            "data": {
                "type": "image",
                "download_url": "https://example.com/uploaded.png",
                "filename": "test.png",
                "size": 1024,
            },
        }
        mock_post.return_value = mock_response

        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            f.write(b"fake image data")
            temp_path = f.name

        try:
            client = Client(api_key="test-key")
            url = client.upload(temp_path)

            self.assertEqual(url, "https://example.com/uploaded.png")
            mock_post.assert_called_once()
        finally:
            os.unlink(temp_path)

    @patch("wavespeed.api.client.requests.post")
    def test_upload_file_object(self, mock_post):
        """Test uploading a file-like object."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "code": 200,
            "message": "success",
            "data": {
                "type": "image",
                "download_url": "https://example.com/uploaded.png",
                "filename": "upload",
                "size": 1024,
            },
        }
        mock_post.return_value = mock_response

        client = Client(api_key="test-key")
        file_obj = io.BytesIO(b"fake image data")
        url = client.upload(file_obj)

        self.assertEqual(url, "https://example.com/uploaded.png")
        mock_post.assert_called_once()

    def test_upload_file_not_found(self):
        """Test uploading a non-existent file."""
        client = Client(api_key="test-key")
        with self.assertRaises(FileNotFoundError):
            client.upload("/nonexistent/path/to/file.png")

    def test_upload_raises_without_api_key(self):
        """Test that upload raises ValueError without API key."""
        client = Client()
        client.api_key = None
        with self.assertRaises(ValueError) as ctx:
            client.upload("/some/file.png")
        self.assertIn("API key is required", str(ctx.exception))

    @patch("wavespeed.api.client.requests.post")
    def test_upload_http_error(self, mock_post):
        """Test upload with HTTP error."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            f.write(b"fake image data")
            temp_path = f.name

        try:
            client = Client(api_key="test-key")
            with self.assertRaises(RuntimeError) as ctx:
                client.upload(temp_path)
            self.assertIn("HTTP 500", str(ctx.exception))
        finally:
            os.unlink(temp_path)

    @patch("wavespeed.api.client.requests.post")
    def test_upload_api_error(self, mock_post):
        """Test upload with API error response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "code": 500,
            "message": "Upload failed: invalid file type",
        }
        mock_post.return_value = mock_response

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            f.write(b"fake image data")
            temp_path = f.name

        try:
            client = Client(api_key="test-key")
            with self.assertRaises(RuntimeError) as ctx:
                client.upload(temp_path)
            self.assertIn("invalid file type", str(ctx.exception))
        finally:
            os.unlink(temp_path)


@unittest.skipUnless(
    os.environ.get("WAVESPEED_API_KEY"),
    "WAVESPEED_API_KEY environment variable not set",
)
class TestRealAPI(unittest.TestCase):
    """Integration tests that call the real WaveSpeed API."""

    def test_run_real_api(self):
        """Test a real API call to wavespeed-ai/z-image/turbo."""
        # Reset default client to pick up env var
        wavespeed.api._default_client = None

        output = wavespeed.run(
            "wavespeed-ai/z-image/turbo",
            {"prompt": "A simple red circle on white background"},
        )

        self.assertIn("outputs", output)
        self.assertIsInstance(output["outputs"], list)
        self.assertGreater(len(output["outputs"]), 0)
        # Output should be a URL
        self.assertTrue(output["outputs"][0].startswith("http"))

    def test_upload_real_api(self):
        """Test a real file upload to WaveSpeed."""
        # Reset default client to pick up env var
        wavespeed.api._default_client = None

        # Create a minimal valid PNG file (1x1 red pixel)
        png_data = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00"
            b"\x00\x0cIDATx\x9cc\xf8\xcf\xc0\x00\x00\x00\x03\x00\x01"
            b"\x00\x05\xfe\xd4\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            f.write(png_data)
            temp_path = f.name

        try:
            url = wavespeed.upload(temp_path)
            self.assertIsInstance(url, str)
            self.assertTrue(url.startswith("http"))
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    unittest.main()
