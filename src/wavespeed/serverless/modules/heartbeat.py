"""Heartbeat module for keeping the worker alive."""

import multiprocessing
import time
from typing import Optional

import requests

from wavespeed import __version__ as wavespeed_version
from wavespeed.config import serverless

from .logger import log


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
        self.interval = serverless.ping_interval
        self._process: Optional[multiprocessing.Process] = None
        self._stop_event: Optional[multiprocessing.Event] = None

    def start(self) -> None:
        """Start the heartbeat process."""
        # Check prerequisites (matching runpod-python behavior)
        api_key = serverless.api_key
        if not api_key:
            log.debug("No API key configured, heartbeat disabled")
            return

        ping_url = serverless.ping_url
        if not ping_url:
            log.debug("No ping endpoint configured, heartbeat disabled")
            return

        self._stop_event = multiprocessing.Event()
        self._process = multiprocessing.Process(
            target=self._heartbeat_loop,
            args=(
                ping_url,
                self.interval,
                self._stop_event,
                api_key,
                wavespeed_version,
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
        api_key: str,
        version: str,
    ) -> None:
        """Run the heartbeat loop in a separate process.

        Args:
            endpoint: The ping endpoint URL.
            interval_ms: Interval between pings in milliseconds.
            stop_event: Event to signal shutdown.
            api_key: API key for authentication.
            version: The wavespeed package version.
        """
        # Import here to avoid pickling issues with multiprocessing
        from .state import JobsProgress

        interval_sec = interval_ms / 1000.0
        timeout = interval_sec * 2

        # Create session with retry strategy (matching runpod-python)
        session = requests.Session()
        session.headers.update({"Authorization": api_key})

        retry_strategy = requests.adapters.Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1,
        )
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=retry_strategy,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        while not stop_event.is_set():
            try:
                # Get current job IDs as comma-separated string (matching runpod-python)
                jobs = JobsProgress()
                job_ids = jobs.get_job_list()

                # Use GET with query params (matching runpod-python)
                ping_params = {"job_id": job_ids, "wavespeed_version": version}
                response = session.get(
                    endpoint,
                    params=ping_params,
                    timeout=timeout,
                )

                if response.status_code != 200:
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
