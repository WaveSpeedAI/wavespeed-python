"""Tests for the overall-wait cap vs. per-request read timeout split."""

import unittest
from unittest.mock import MagicMock, patch

import requests

from wavespeed.api import Client


def _resp(status=200, payload=None):
    r = MagicMock()
    r.status_code = status
    r.text = ""
    r.json.return_value = payload if payload is not None else {}
    return r


def _submit_ok():
    return _resp(200, {"data": {"id": "t1"}})


def _status(status, outputs=None):
    return _resp(200, {"data": {"status": status, "outputs": outputs or []}})


class FakeClock:
    """time.time()/sleep() replacement: sleep advances the clock instantly."""

    def __init__(self):
        """Start at t=0."""
        self.now = 0.0

    def time(self):
        """Return the fake now."""
        return self.now

    def sleep(self, seconds):
        """Advance the fake clock."""
        self.now += seconds


@patch("wavespeed.api.client.time")
@patch("wavespeed.api.client.requests.get")
@patch("wavespeed.api.client.requests.post")
class TestTimeouts(unittest.TestCase):
    """Overall cap and per-request read timeouts."""

    def setUp(self):
        """Create a client."""
        self.client = Client(api_key="k")

    def _clock(self, mock_time):
        clock = FakeClock()
        mock_time.time.side_effect = clock.time
        mock_time.sleep.side_effect = clock.sleep
        return clock

    def test_default_has_no_overall_cap(self, post, get, mock_time):
        """Test default has no overall cap."""
        clock = self._clock(mock_time)
        post.return_value = _submit_ok()
        # Polls at 1000s intervals: stays processing past 36000s, then completes.
        get.side_effect = [_status("processing")] * 40 + [_status("completed", ["u"])]
        out = self.client.run("m/x", {}, poll_interval=1000.0)
        self.assertEqual(out, {"outputs": ["u"]})
        self.assertEqual(get.call_count, 41)
        self.assertGreater(clock.now, 36000.0)

    def test_explicit_timeout_still_raises(self, post, get, mock_time):
        """Test explicit timeout still raises."""
        self._clock(mock_time)
        post.return_value = _submit_ok()
        get.return_value = _status("processing")
        with self.assertRaises(TimeoutError):
            self.client.run("m/x", {}, timeout=50.0, poll_interval=10.0)

    def test_config_timeout_still_caps(self, post, get, mock_time):
        """Test config timeout still caps."""
        self._clock(mock_time)
        post.return_value = _submit_ok()
        get.return_value = _status("processing")
        with patch("wavespeed.api.client.api_config.timeout", 50.0):
            with self.assertRaises(TimeoutError):
                self.client.run("m/x", {}, poll_interval=10.0)

    def test_async_submit_and_get_use_read_timeout(self, post, get, mock_time):
        """Test async submit and get use read timeout."""
        self._clock(mock_time)
        post.return_value = _submit_ok()
        get.return_value = _status("completed")
        self.client.run("m/x", {})
        self.assertEqual(post.call_args.kwargs["timeout"], (10.0, 120.0))
        self.assertEqual(get.call_args.kwargs["timeout"], (10.0, 120.0))

    def test_sync_submit_no_read_timeout_by_default(self, post, get, mock_time):
        """Test sync submit no read timeout by default."""
        post.return_value = _resp(200, {"data": {"status": "completed"}})
        self.client.run("m/x", {}, enable_sync_mode=True)
        self.assertEqual(post.call_args.kwargs["timeout"], (10.0, None))

    def test_sync_submit_explicit_timeout(self, post, get, mock_time):
        """Test sync submit explicit timeout."""
        post.return_value = _resp(200, {"data": {"status": "completed"}})
        self.client.run("m/x", {}, enable_sync_mode=True, timeout=300.0)
        self.assertEqual(post.call_args.kwargs["timeout"], (10.0, 300.0))

    def test_explicit_timeout_applies_to_requests(self, post, get, mock_time):
        """Test explicit timeout applies to requests."""
        self._clock(mock_time)
        post.return_value = _submit_ok()
        get.return_value = _status("completed")
        self.client.run("m/x", {}, timeout=300.0)
        self.assertEqual(post.call_args.kwargs["timeout"], (10.0, 300.0))
        self.assertEqual(get.call_args.kwargs["timeout"], (10.0, 300.0))

    def test_poll_retries_502_then_succeeds(self, post, get, mock_time):
        """Test poll retries 502 then succeeds."""
        self._clock(mock_time)
        post.return_value = _submit_ok()
        get.side_effect = [_resp(502), _status("completed", ["u"])]
        out = self.client.run("m/x", {})
        self.assertEqual(out, {"outputs": ["u"]})
        self.assertEqual(get.call_count, 2)

    def test_submit_timeout_is_not_retried(self, post, get, mock_time):
        """Test submit timeout is not retried."""
        post.side_effect = requests.exceptions.ReadTimeout()
        with self.assertRaises(RuntimeError):
            self.client.run("m/x", {})
        self.assertEqual(post.call_count, 1)
        get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
