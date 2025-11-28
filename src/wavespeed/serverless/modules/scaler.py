"""Job scaler module for concurrent job processing."""

import asyncio
import signal
import sys
import traceback
from typing import Any, Callable, Dict

import aiohttp

from .job import get_job, handle_job
from .logger import log
from .state import IS_LOCAL_TEST, JobsProgress


def _handle_uncaught_exception(
    exc_type: type, exc_value: BaseException, exc_traceback: Any
) -> None:
    """Handle uncaught exceptions by logging them."""
    exc = traceback.format_exception(exc_type, exc_value, exc_traceback)
    log.error(f"Uncaught exception | {exc}")


def _default_concurrency_modifier(current_concurrency: int) -> int:
    """Return unchanged concurrency (default modifier)."""
    return current_concurrency


class JobScaler:
    """Manages concurrent job fetching and processing.

    The JobScaler runs two concurrent tasks:
    1. Job fetching - polls the job endpoint for new jobs
    2. Job running - processes jobs from the queue concurrently

    Attributes:
        config: The worker configuration.
        jobs_queue: Async queue of jobs to process.
        current_concurrency: Current number of concurrent workers.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize the job scaler.

        Args:
            config: The worker configuration.
        """
        self._shutdown_event = asyncio.Event()
        self.current_concurrency = 1
        self.config = config
        self.job_progress = JobsProgress()

        self.jobs_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(
            maxsize=self.current_concurrency
        )
        self.concurrency_modifier: Callable[[int], int] = _default_concurrency_modifier
        self.jobs_fetcher = get_job
        self.jobs_fetcher_timeout = 90
        self.jobs_handler = handle_job

        # Get concurrency modifier if provided
        if concurrency_modifier := config.get("concurrency_modifier"):
            self.concurrency_modifier = concurrency_modifier

        # Allow overriding jobs_fetcher and handler in local test mode
        if not IS_LOCAL_TEST:
            return

        if jobs_fetcher := config.get("jobs_fetcher"):
            self.jobs_fetcher = jobs_fetcher

        if jobs_fetcher_timeout := config.get("jobs_fetcher_timeout"):
            self.jobs_fetcher_timeout = jobs_fetcher_timeout

        if jobs_handler := config.get("jobs_handler"):
            self.jobs_handler = jobs_handler

    async def set_scale(self) -> None:
        """Update the concurrency scale and resize queue if needed."""
        self.current_concurrency = self.concurrency_modifier(self.current_concurrency)

        if self.jobs_queue and (self.current_concurrency == self.jobs_queue.maxsize):
            return

        # Wait for queue to drain before resizing
        while self.current_occupancy() > 0:
            await asyncio.sleep(1)

        self.jobs_queue = asyncio.Queue(maxsize=self.current_concurrency)
        log.debug(
            f"JobScaler.set_scale | New concurrency set to: {self.current_concurrency}"
        )

    def start(self) -> None:
        """Start the job scaler.

        This sets up signal handlers and runs the async event loop.
        """
        sys.excepthook = _handle_uncaught_exception

        try:
            signal.signal(signal.SIGTERM, self.handle_shutdown)
            signal.signal(signal.SIGINT, self.handle_shutdown)
        except ValueError:
            log.warn("Signal handling is only supported in the main thread.")

        asyncio.run(self.run())

    def handle_shutdown(self, signum: int, frame: Any) -> None:
        """Handle termination signals for graceful shutdown."""
        log.debug(f"Received shutdown signal: {signum}.")
        self.kill_worker()

    async def run(self) -> None:
        """Run the job fetching and processing tasks."""
        from wavespeed.config import serverless

        # Build headers with API key authentication
        headers = {}
        if serverless.api_key:
            headers["Authorization"] = f"Bearer {serverless.api_key}"

        async with aiohttp.ClientSession(headers=headers) as session:
            jobtake_task = asyncio.create_task(self.get_jobs(session))
            jobrun_task = asyncio.create_task(self.run_jobs(session))

            await asyncio.gather(jobtake_task, jobrun_task)

    def is_alive(self) -> bool:
        """Indicate if worker is currently running."""
        return not self._shutdown_event.is_set()

    def kill_worker(self) -> None:
        """Trigger worker shutdown."""
        log.debug("Kill worker.")
        self._shutdown_event.set()

    def current_occupancy(self) -> int:
        """Get current occupancy (queue + in-progress jobs)."""
        current_queue_count = self.jobs_queue.qsize()
        current_progress_count = self.job_progress.get_job_count()

        log.debug(
            f"JobScaler.status | concurrency: {self.current_concurrency}; queue: "
            f"{current_queue_count}; progress: {current_progress_count}"
        )
        return current_progress_count + current_queue_count

    async def get_jobs(self, session: aiohttp.ClientSession) -> None:
        """Continuously retrieve jobs from server and add to queue."""
        while self.is_alive():
            await self.set_scale()

            jobs_needed = self.current_concurrency - self.current_occupancy()
            if jobs_needed <= 0:
                log.debug("JobScaler.get_jobs | Queue is full. Retrying soon.")
                await asyncio.sleep(0.2)
                continue

            try:
                log.debug("JobScaler.get_jobs | Starting job acquisition.")

                acquired_jobs = await asyncio.wait_for(
                    self.jobs_fetcher(session, jobs_needed),
                    timeout=self.jobs_fetcher_timeout,
                )

                if not acquired_jobs:
                    log.debug("JobScaler.get_jobs | No jobs acquired.")
                    continue

                for job in acquired_jobs:
                    await self.jobs_queue.put(job)
                    self.job_progress.add(job)
                    log.debug("Job Queued", job["id"])

                log.info(f"Jobs in queue: {self.jobs_queue.qsize()}")

            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    log.debug(
                        "JobScaler.get_jobs | Too many requests. Debounce for 5 seconds."
                    )
                    await asyncio.sleep(5)
                else:
                    log.error(
                        f"Failed to get job. | Error Type: {type(e).__name__} | "
                        f"Error Message: {str(e)}"
                    )
            except asyncio.CancelledError:
                log.debug("JobScaler.get_jobs | Request was cancelled.")
                raise
            except asyncio.TimeoutError:
                log.debug("JobScaler.get_jobs | Job acquisition timed out. Retrying.")
            except TypeError as error:
                log.debug(f"JobScaler.get_jobs | Unexpected error: {error}.")
            except Exception as error:
                log.error(
                    f"Failed to get job. | Error Type: {type(error).__name__} | "
                    f"Error Message: {str(error)}"
                )
            finally:
                await asyncio.sleep(0)

    async def run_jobs(self, session: aiohttp.ClientSession) -> None:
        """Process queued jobs concurrently."""
        tasks: list[asyncio.Task[None]] = []

        while self.is_alive() or not self.jobs_queue.empty():
            while len(tasks) < self.current_concurrency and not self.jobs_queue.empty():
                job = await self.jobs_queue.get()
                task = asyncio.create_task(self.handle_job(session, job))
                tasks.append(task)

            if tasks:
                log.info(f"Jobs in progress: {len(tasks)}")

                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )

                tasks = [t for t in tasks if t not in done]

            await asyncio.sleep(0)

        await asyncio.gather(*tasks)

    async def handle_job(
        self, session: aiohttp.ClientSession, job: Dict[str, Any]
    ) -> None:
        """Process individual job with error handling."""
        try:
            log.debug("Handling Job", job["id"])

            await self.jobs_handler(session, self.config, job)

            if self.config.get("refresh_worker", False):
                self.kill_worker()

        except Exception as err:
            log.error(f"Error handling job: {err}", job["id"])
            raise err

        finally:
            self.jobs_queue.task_done()
            self.job_progress.remove(job)
            log.debug("Finished Job", job["id"])
