"""Heartbeat module for keeping the worker alive."""

import multiprocessing
import os
import time
from typing import Optional

import requests

from wavespeed.config import get_serverless_env

from .logger import log
from .state import get_worker_id


class Heartbeat:
    """Manages periodic heartbeat pings to the serverless platform.

    The heartbeat runs in a separate daemon process to ensure
    it continues running even if the main worker is blocked.

    Attributes:
        interval: Heartbeat interval in milliseconds.
        process: The daemon process running the heartbeat.
    """

    def __init__(self) -> None:
        """Initialize the heartbeat."""
        self.interval = int(get_serverless_env("PING_INTERVAL", "10000"))
        self._process: Optional[multiprocessing.Process] = None
        self._stop_event: Optional[multiprocessing.Event] = None

    def start(self) -> None:
        """Start the heartbeat process."""
        ping_endpoint = get_serverless_env("PING_ENDPOINT")
        if not ping_endpoint:
            log.debug("No ping endpoint configured, heartbeat disabled")
            return

        self._stop_event = multiprocessing.Event()
        self._process = multiprocessing.Process(
            target=self._heartbeat_loop,
            args=(
                ping_endpoint,
                self.interval,
                self._stop_event,
                get_worker_id(),
                get_serverless_env("API_KEY", ""),
            ),
            daemon=True,
        )
        self._process.start()
        log.debug(f"Heartbeat started (interval: {self.interval}ms)")

    def stop(self) -> None:
        """Stop the heartbeat process."""
        if self._stop_event:
            self._stop_event.set()

        if self._process and self._process.is_alive():
            self._process.join(timeout=2)
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=1)

        log.debug("Heartbeat stopped")

    @staticmethod
    def _heartbeat_loop(
        endpoint: str,
        interval_ms: int,
        stop_event: multiprocessing.Event,
        worker_id: str,
        api_key: str,
    ) -> None:
        """The heartbeat loop running in a separate process.

        Args:
            endpoint: The ping endpoint URL.
            interval_ms: Interval between pings in milliseconds.
            stop_event: Event to signal shutdown.
            worker_id: The worker ID to include in pings.
            api_key: API key for authentication.
        """
        interval_sec = interval_ms / 1000.0
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        while not stop_event.is_set():
            try:
                response = requests.post(
                    endpoint,
                    json={"worker_id": worker_id},
                    headers=headers,
                    timeout=10,
                )
                if response.status_code != 200:
                    # Log to stderr since we're in a subprocess
                    print(f"Heartbeat failed: {response.status_code}", flush=True)
            except requests.RequestException as e:
                print(f"Heartbeat error: {e}", flush=True)
            except Exception as e:
                print(f"Unexpected heartbeat error: {e}", flush=True)

            # Sleep in small increments to allow responsive shutdown
            sleep_remaining = interval_sec
            while sleep_remaining > 0 and not stop_event.is_set():
                sleep_time = min(0.5, sleep_remaining)
                time.sleep(sleep_time)
                sleep_remaining -= sleep_time
