"""Job scaler module for concurrent job processing."""

import asyncio
import signal
import sys
import time
from typing import Any, Callable, Dict, Optional

import aiohttp

from wavespeed.config import get_serverless_env

from .job import get_job, handle_job
from .logger import log
from .state import Job, get_jobs_progress, ref_count_zero


class JobScaler:
    """Manages concurrent job fetching and processing.

    The JobScaler runs two concurrent tasks:
    1. Job fetching - polls the job endpoint for new jobs
    2. Job running - processes jobs from the queue concurrently

    Attributes:
        config: The worker configuration.
        jobs_queue: Async queue of jobs to process.
        current_concurrency: Current number of concurrent workers.
        max_concurrency: Maximum number of concurrent workers.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize the job scaler.

        Args:
            config: The worker configuration.
        """
        self.config = config
        self.jobs_queue: asyncio.Queue[Job] = asyncio.Queue()
        self.current_concurrency = 1
        self.max_concurrency = 1
        self._shutdown = False
        self._active_jobs = 0

        # Get concurrency modifier function if provided
        self._concurrency_modifier: Optional[Callable[[int], int]] = config.get(
            "concurrency_modifier"
        )

    def start(self) -> None:
        """Start the job scaler.

        This sets up signal handlers and runs the async event loop.
        """
        # Setup signal handlers
        def shutdown_handler(sig: int, frame: Any) -> None:
            log.info("Shutdown signal received")
            self._shutdown = True

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        # Run the async event loop
        try:
            asyncio.run(self.run())
        except KeyboardInterrupt:
            log.info("Scaler interrupted")

    async def run(self) -> None:
        """Main async run loop.

        Creates and runs the job fetching and processing tasks.
        """
        async with aiohttp.ClientSession() as session:
            # Create tasks for fetching and running jobs
            fetch_task = asyncio.create_task(self._fetch_jobs_loop(session))
            run_task = asyncio.create_task(self._run_jobs_loop(session))

            try:
                # Wait for either task to complete (or shutdown)
                done, pending = await asyncio.wait(
                    [fetch_task, run_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            except asyncio.CancelledError:
                log.info("Scaler tasks cancelled")

    async def _fetch_jobs_loop(self, session: aiohttp.ClientSession) -> None:
        """Continuously fetch jobs from the endpoint.

        Args:
            session: The aiohttp client session.
        """
        while not self._shutdown and not self.config.get("_shutdown"):
            try:
                # Calculate how many jobs to fetch
                queue_space = self.max_concurrency - self.jobs_queue.qsize()
                if queue_space <= 0:
                    await asyncio.sleep(0.1)
                    continue

                # Fetch jobs
                jobs = await get_job(session, num_jobs=queue_space)

                if jobs:
                    for job in jobs:
                        await self.jobs_queue.put(job)
                        log.debug(f"Queued job {job.id}")
                else:
                    # No jobs available, wait before polling again
                    await asyncio.sleep(0.5)

            except Exception as e:
                log.error(f"Error fetching jobs: {e}")
                await asyncio.sleep(1)

    async def _run_jobs_loop(self, session: aiohttp.ClientSession) -> None:
        """Continuously process jobs from the queue.

        Args:
            session: The aiohttp client session.
        """
        tasks: set[asyncio.Task[None]] = set()

        while not self._shutdown and not self.config.get("_shutdown"):
            try:
                # Adjust concurrency if needed
                self._update_concurrency()

                # Clean up completed tasks
                done_tasks = {t for t in tasks if t.done()}
                for task in done_tasks:
                    tasks.discard(task)
                    self._active_jobs -= 1
                    # Check for exceptions
                    try:
                        task.result()
                    except Exception as e:
                        log.error(f"Job task error: {e}")

                # Start new tasks up to concurrency limit
                while (
                    len(tasks) < self.current_concurrency
                    and not self.jobs_queue.empty()
                ):
                    try:
                        job = self.jobs_queue.get_nowait()
                        task = asyncio.create_task(
                            self._handle_job_wrapper(session, job)
                        )
                        tasks.add(task)
                        self._active_jobs += 1
                    except asyncio.QueueEmpty:
                        break

                # Small delay to prevent busy loop
                await asyncio.sleep(0.01)

            except Exception as e:
                log.error(f"Error in job processing loop: {e}")
                await asyncio.sleep(1)

        # Wait for remaining tasks to complete
        if tasks:
            log.info(f"Waiting for {len(tasks)} active jobs to complete...")
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _handle_job_wrapper(
        self,
        session: aiohttp.ClientSession,
        job: Job,
    ) -> None:
        """Wrapper for handling a job with error handling.

        Args:
            session: The aiohttp client session.
            job: The job to handle.
        """
        try:
            await handle_job(session, self.config, job)
        except Exception as e:
            log.error(f"Unhandled error in job {job.id}: {e}")

    def _update_concurrency(self) -> None:
        """Update the current concurrency based on the modifier function."""
        if self._concurrency_modifier:
            try:
                new_concurrency = self._concurrency_modifier(self.current_concurrency)
                if new_concurrency != self.current_concurrency:
                    log.info(
                        f"Concurrency changed: {self.current_concurrency} -> {new_concurrency}"
                    )
                    self.current_concurrency = max(1, new_concurrency)
                    self.max_concurrency = max(self.max_concurrency, new_concurrency)
            except Exception as e:
                log.warn(f"Error in concurrency modifier: {e}")

    def set_scale(self, concurrency: int) -> None:
        """Set the target concurrency level.

        Args:
            concurrency: The new concurrency level.
        """
        self.current_concurrency = max(1, concurrency)
        self.max_concurrency = max(self.max_concurrency, concurrency)
        log.info(f"Scale set to {self.current_concurrency}")

    def kill_worker(self) -> None:
        """Signal the worker to shut down gracefully."""
        log.info("Kill worker requested")
        self._shutdown = True
        self.config["_shutdown"] = True
