"""Tests for the local module."""

import json
import os
import tempfile
import unittest
from argparse import Namespace
from unittest.mock import patch

from wavespeed.serverless.modules.local import (
    _load_test_input,
    _print_result,
    run_local,
)


class TestLoadTestInput(unittest.TestCase):
    """Tests for the _load_test_input function."""

    def test_load_from_cli_argument(self):
        """Test loading test input from CLI argument."""
        args = Namespace(test_input='{"input": {"prompt": "hello"}}')
        config = {"_args": args}

        result = _load_test_input(config)

        self.assertEqual(result, {"input": {"prompt": "hello"}})

    def test_load_invalid_json_from_cli(self):
        """Test loading invalid JSON from CLI."""
        args = Namespace(test_input="not valid json")
        config = {"_args": args}

        with patch("wavespeed.serverless.modules.local.log"):
            result = _load_test_input(config)

        self.assertIsNone(result)

    def test_load_from_file(self):
        """Test loading test input from file."""
        test_data = {"input": {"data": "from_file"}}

        with tempfile.TemporaryDirectory() as tmpdir:
            original_cwd = os.getcwd()
            os.chdir(tmpdir)

            try:
                with open("test_input.json", "w") as f:
                    json.dump(test_data, f)

                config = {"_args": None}
                result = _load_test_input(config)

                self.assertEqual(result, test_data)
            finally:
                os.chdir(original_cwd)

    def test_no_test_input_available(self):
        """Test when no test input is available."""
        config = {"_args": None}

        with tempfile.TemporaryDirectory() as tmpdir:
            original_cwd = os.getcwd()
            os.chdir(tmpdir)

            try:
                result = _load_test_input(config)
                self.assertIsNone(result)
            finally:
                os.chdir(original_cwd)


class TestRunLocal(unittest.TestCase):
    """Tests for the run_local function."""

    def test_run_local_sync_handler(self):
        """Test running local with sync handler."""

        def sync_handler(job):
            return {"result": job["input"]["value"] * 2}

        config = {
            "handler": sync_handler,
            "_args": Namespace(test_input='{"input": {"value": 21}}'),
        }

        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            run_local(config)

            # Check that success message was logged
            mock_log.info.assert_any_call("Local test completed successfully")

    def test_run_local_async_handler(self):
        """Test running local with async handler."""

        async def async_handler(job):
            return {"result": "async_result"}

        config = {
            "handler": async_handler,
            "_args": Namespace(test_input='{"input": {}}'),
        }

        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            run_local(config)

            mock_log.info.assert_any_call("Local test completed successfully")

    def test_run_local_sync_generator(self):
        """Test running local with sync generator handler."""

        def gen_handler(job):
            yield "part1"
            yield "part2"

        config = {
            "handler": gen_handler,
            "_args": Namespace(test_input='{"input": {}}'),
        }

        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            run_local(config)

            mock_log.info.assert_any_call("Local test completed successfully")

    def test_run_local_async_generator(self):
        """Test running local with async generator handler."""

        async def async_gen_handler(job):
            yield "async_part1"
            yield "async_part2"

        config = {
            "handler": async_gen_handler,
            "_args": Namespace(test_input='{"input": {}}'),
        }

        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            run_local(config)

            mock_log.info.assert_any_call("Local test completed successfully")

    def test_run_local_no_test_input(self):
        """Test running local with no test input."""

        def handler(job):
            return {"received": job["input"]}

        config = {
            "handler": handler,
            "_args": None,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            original_cwd = os.getcwd()
            os.chdir(tmpdir)

            try:
                with patch("wavespeed.serverless.modules.local.log") as mock_log:
                    run_local(config)

                    # Should warn about no test input
                    mock_log.warn.assert_called()
            finally:
                os.chdir(original_cwd)

    def test_run_local_handler_exception(self):
        """Test running local when handler raises exception."""

        def bad_handler(job):
            raise ValueError("Handler failed")

        config = {
            "handler": bad_handler,
            "_args": Namespace(test_input='{"input": {}}'),
        }

        with patch("wavespeed.serverless.modules.local.log"), self.assertRaises(
            SystemExit
        ):
            run_local(config)

    def test_run_local_wraps_input(self):
        """Test that run_local wraps input in 'input' key if needed."""
        captured_input = {}

        def handler(job):
            captured_input.update(job)
            return "done"

        config = {
            "handler": handler,
            "_args": Namespace(test_input='{"prompt": "hello"}'),
        }

        with patch("wavespeed.serverless.modules.local.log"):
            run_local(config)

        self.assertEqual(captured_input["input"], {"prompt": "hello"})


class TestPrintResult(unittest.TestCase):
    """Tests for the _print_result function."""

    def test_print_none_result(self):
        """Test printing None result."""
        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            _print_result(None)
            mock_log.info.assert_called_once_with("[RESULT] None")

    def test_print_error_result(self):
        """Test printing error result."""
        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            _print_result({"error": "Something went wrong"})
            mock_log.error.assert_called_once_with("[ERROR] Something went wrong")

    def test_print_output_dict_result(self):
        """Test printing output dict result."""
        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            _print_result({"output": {"key": "value"}})
            call_args = mock_log.info.call_args[0][0]
            self.assertIn("[OUTPUT]", call_args)
            self.assertIn("key", call_args)

    def test_print_output_primitive(self):
        """Test printing output primitive."""
        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            _print_result({"output": 42})
            mock_log.info.assert_called_once_with("[OUTPUT] 42")

    def test_print_dict_result(self):
        """Test printing dict without output/error."""
        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            _print_result({"custom": "data"})
            call_args = mock_log.info.call_args[0][0]
            self.assertIn("[RESULT]", call_args)

    def test_print_primitive_result(self):
        """Test printing primitive result."""
        with patch("wavespeed.serverless.modules.local.log") as mock_log:
            _print_result("simple string")
            mock_log.info.assert_called_once_with("[RESULT] simple string")


if __name__ == "__main__":
    unittest.main()
