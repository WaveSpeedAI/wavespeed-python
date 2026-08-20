"""Tests for the config module."""

import unittest

import wavespeed.config as config_module
from wavespeed.config import api


class TestApiConfig(unittest.TestCase):
    """Tests for the api config namespace."""

    def test_api_has_expected_attributes(self):
        """Test that api config exposes every documented setting."""
        for name in (
            "api_key",
            "base_url",
            "connection_timeout",
            "timeout",
            "max_retries",
            "max_connection_retries",
            "retry_interval",
        ):
            self.assertTrue(hasattr(api, name), f"api.{name} is missing")

    def test_defaults(self):
        """Test the shipped default values."""
        self.assertEqual(api.base_url, "https://api.wavespeed.ai")
        self.assertEqual(api.connection_timeout, 10.0)
        self.assertEqual(api.timeout, 36000.0)
        self.assertEqual(api.max_retries, 0)
        self.assertEqual(api.max_connection_retries, 5)
        self.assertEqual(api.retry_interval, 1.0)

    def test_patch_restores_previous_value(self):
        """Test that config.patch() is scoped to the context manager."""
        original = api.base_url
        with config_module.patch("api.base_url", "https://example.invalid"):
            self.assertEqual(api.base_url, "https://example.invalid")
        self.assertEqual(api.base_url, original)

    def test_unknown_attribute_raises(self):
        """Test that unknown config keys are rejected."""
        with self.assertRaises(AttributeError):
            config_module.definitely_not_a_setting


class TestServerlessRemoved(unittest.TestCase):
    """The serverless worker was removed in v2.0.0."""

    def test_serverless_config_is_gone(self):
        """Test that the serverless config namespace no longer exists."""
        with self.assertRaises(AttributeError):
            config_module.serverless

    def test_serverless_package_is_gone(self):
        """Test that the serverless package is no longer importable."""
        with self.assertRaises(ImportError):
            import wavespeed.serverless  # noqa: F401


if __name__ == "__main__":
    unittest.main()
