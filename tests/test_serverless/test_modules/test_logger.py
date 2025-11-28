"""Tests for the logger module."""

import io
import json
import unittest
from unittest.mock import patch

from wavespeed.serverless.modules.logger import log, LogLevel, WaverlessLogger


class TestLogLevel(unittest.TestCase):
    """Tests for the LogLevel enum."""

    def test_log_level_ordering(self):
        """Test that log levels are ordered correctly."""
        self.assertLess(LogLevel.NOTSET, LogLevel.TRACE)
        self.assertLess(LogLevel.TRACE, LogLevel.DEBUG)
        self.assertLess(LogLevel.DEBUG, LogLevel.INFO)
        self.assertLess(LogLevel.INFO, LogLevel.WARN)
        self.assertLess(LogLevel.WARN, LogLevel.ERROR)

    def test_log_level_values(self):
        """Test log level values."""
        self.assertEqual(LogLevel.NOTSET, 0)
        self.assertEqual(LogLevel.TRACE, 5)
        self.assertEqual(LogLevel.DEBUG, 10)
        self.assertEqual(LogLevel.INFO, 20)
        self.assertEqual(LogLevel.WARN, 30)
        self.assertEqual(LogLevel.ERROR, 40)


class TestWaverlessLogger(unittest.TestCase):
    """Tests for the WaverlessLogger class."""

    def setUp(self):
        """Reset singleton before each test."""
        WaverlessLogger._instance = None

    def tearDown(self):
        """Clean up after tests."""
        WaverlessLogger._instance = None

    def test_singleton(self):
        """Test that WaverlessLogger is a singleton."""
        logger1 = WaverlessLogger()
        logger2 = WaverlessLogger()
        self.assertIs(logger1, logger2)

    def test_set_level_string(self):
        """Test setting log level with string."""
        logger = WaverlessLogger()
        logger.set_level("DEBUG")
        self.assertEqual(logger._level, LogLevel.DEBUG)

        logger.set_level("warn")
        self.assertEqual(logger._level, LogLevel.WARN)

    def test_set_level_enum(self):
        """Test setting log level with enum."""
        logger = WaverlessLogger()
        logger.set_level(LogLevel.ERROR)
        self.assertEqual(logger._level, LogLevel.ERROR)

    def test_set_level_invalid(self):
        """Test setting invalid log level defaults to INFO."""
        logger = WaverlessLogger()
        logger.set_level("INVALID")
        self.assertEqual(logger._level, LogLevel.INFO)

    def test_should_log_respects_level(self):
        """Test that _should_log respects the log level."""
        logger = WaverlessLogger()
        logger.set_level(LogLevel.INFO)

        self.assertFalse(logger._should_log(LogLevel.DEBUG))
        self.assertFalse(logger._should_log(LogLevel.TRACE))
        self.assertTrue(logger._should_log(LogLevel.INFO))
        self.assertTrue(logger._should_log(LogLevel.WARN))
        self.assertTrue(logger._should_log(LogLevel.ERROR))

    def test_truncate_long_message(self):
        """Test that long messages are truncated."""
        logger = WaverlessLogger()
        long_message = "x" * 5000

        truncated = logger._truncate(long_message)

        self.assertEqual(len(truncated), logger._max_message_length)
        self.assertTrue(truncated.endswith("..."))

    def test_truncate_short_message(self):
        """Test that short messages are not truncated."""
        logger = WaverlessLogger()
        short_message = "Short message"

        truncated = logger._truncate(short_message)

        self.assertEqual(truncated, short_message)

    def test_log_methods_exist(self):
        """Test that all log methods exist."""
        logger = WaverlessLogger()

        self.assertTrue(callable(logger.trace))
        self.assertTrue(callable(logger.debug))
        self.assertTrue(callable(logger.info))
        self.assertTrue(callable(logger.warn))
        self.assertTrue(callable(logger.error))
        self.assertTrue(callable(logger.log))

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_info_log_output_local(self, mock_stdout):
        """Test info logging output in local mode."""
        with patch("wavespeed.serverless.modules.logger.serverless") as mock_serverless:
            mock_serverless.log_level = "INFO"
            mock_serverless.endpoint_id = None

            WaverlessLogger._instance = None
            logger = WaverlessLogger()
            logger.set_level(LogLevel.INFO)

            logger.info("Test message")

            output = mock_stdout.getvalue()
            self.assertIn("INFO", output)
            self.assertIn("Test message", output)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_info_log_with_job_id(self, mock_stdout):
        """Test logging with job ID."""
        with patch("wavespeed.serverless.modules.logger.serverless") as mock_serverless:
            mock_serverless.log_level = "INFO"
            mock_serverless.endpoint_id = None

            WaverlessLogger._instance = None
            logger = WaverlessLogger()
            logger.set_level(LogLevel.INFO)

            logger.info("Test message", job_id="job_123")

            output = mock_stdout.getvalue()
            self.assertIn("job_123", output)
            self.assertIn("Test message", output)

    @patch("sys.stderr", new_callable=io.StringIO)
    def test_error_logs_to_stderr(self, mock_stderr):
        """Test that error logs go to stderr."""
        with patch("wavespeed.serverless.modules.logger.serverless") as mock_serverless:
            mock_serverless.log_level = "INFO"
            mock_serverless.endpoint_id = None

            WaverlessLogger._instance = None
            logger = WaverlessLogger()
            logger.set_level(LogLevel.INFO)

            logger.error("Error message")

            output = mock_stderr.getvalue()
            self.assertIn("ERROR", output)
            self.assertIn("Error message", output)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_json_output_in_deployed_mode(self, mock_stdout):
        """Test JSON output when endpoint ID is set."""
        with patch("wavespeed.serverless.modules.logger.serverless") as mock_serverless:
            mock_serverless.log_level = "INFO"
            mock_serverless.endpoint_id = "test_endpoint"

            WaverlessLogger._instance = None
            logger = WaverlessLogger()

            logger.info("Test message", job_id="job_123")

            output = mock_stdout.getvalue().strip()
            log_data = json.loads(output)

            self.assertEqual(log_data["message"], "Test message")
            self.assertEqual(log_data["requestId"], "job_123")
            self.assertEqual(log_data["level"], "INFO")
            self.assertIn("timestamp", log_data)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_debug_not_logged_at_info_level(self, mock_stdout):
        """Test that debug messages are not logged at INFO level."""
        with patch("wavespeed.serverless.modules.logger.serverless") as mock_serverless:
            mock_serverless.log_level = "INFO"
            mock_serverless.endpoint_id = None

            WaverlessLogger._instance = None
            logger = WaverlessLogger()
            logger.set_level(LogLevel.INFO)

            logger.debug("Debug message")

            output = mock_stdout.getvalue()
            self.assertEqual(output, "")


class TestGlobalLogger(unittest.TestCase):
    """Tests for the global log instance."""

    def test_global_log_exists(self):
        """Test that the global log instance exists."""
        self.assertIsInstance(log, WaverlessLogger)

    def test_global_log_methods(self):
        """Test that global log has all methods."""
        self.assertTrue(callable(log.trace))
        self.assertTrue(callable(log.debug))
        self.assertTrue(callable(log.info))
        self.assertTrue(callable(log.warn))
        self.assertTrue(callable(log.error))


if __name__ == "__main__":
    unittest.main()
