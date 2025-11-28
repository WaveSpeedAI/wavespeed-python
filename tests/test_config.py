"""Tests for the config module."""

import unittest

from wavespeed.config import _resolve_url, serverless


class TestResolveUrl(unittest.TestCase):
    """Tests for the _resolve_url function."""

    def test_replaces_runpod_pod_id(self):
        """Test that $RUNPOD_POD_ID is replaced with pod_id."""
        template = "https://api.runpod.ai/v2/endpoint/job-done/$RUNPOD_POD_ID"
        result = _resolve_url(template, "my-pod-123")
        self.assertEqual(
            result, "https://api.runpod.ai/v2/endpoint/job-done/my-pod-123"
        )

    def test_preserves_id_placeholder(self):
        """Test that $ID is NOT replaced - it's for job ID at runtime."""
        template = "https://api.runpod.ai/v2/endpoint/job-done/$RUNPOD_POD_ID/$ID"
        result = _resolve_url(template, "my-pod-123")
        # $ID should remain as placeholder for job ID replacement later
        self.assertEqual(
            result, "https://api.runpod.ai/v2/endpoint/job-done/my-pod-123/$ID"
        )

    def test_handles_none_template(self):
        """Test that None template returns None."""
        result = _resolve_url(None, "my-pod-123")
        self.assertIsNone(result)

    def test_handles_empty_template(self):
        """Test that empty template returns None (falsy check)."""
        result = _resolve_url("", "my-pod-123")
        self.assertIsNone(result)

    def test_no_placeholders(self):
        """Test URL without any placeholders."""
        template = "https://api.example.com/endpoint"
        result = _resolve_url(template, "my-pod-123")
        self.assertEqual(result, "https://api.example.com/endpoint")

    def test_multiple_pod_id_placeholders(self):
        """Test multiple $RUNPOD_POD_ID placeholders are all replaced."""
        template = "https://api.runpod.ai/$RUNPOD_POD_ID/test/$RUNPOD_POD_ID"
        result = _resolve_url(template, "pod-456")
        self.assertEqual(result, "https://api.runpod.ai/pod-456/test/pod-456")


class TestServerlessConfig(unittest.TestCase):
    """Tests for serverless config loading."""

    def test_serverless_has_expected_attributes(self):
        """Test that serverless config has all expected attributes."""
        self.assertTrue(hasattr(serverless, "pod_id"))
        self.assertTrue(hasattr(serverless, "api_key"))
        self.assertTrue(hasattr(serverless, "job_get_url"))
        self.assertTrue(hasattr(serverless, "job_done_url"))
        self.assertTrue(hasattr(serverless, "job_stream_url"))
        self.assertTrue(hasattr(serverless, "ping_url"))


if __name__ == "__main__":
    unittest.main()
