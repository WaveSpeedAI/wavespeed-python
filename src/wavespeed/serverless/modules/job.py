"""Job fetching and execution module for the serverless worker."""

import asyncio
import inspect
import traceback
from typing import Any, Callable, Dict, Generator

import aiohttp

from .handler import is_async_generator, is_sync_generator
from .http import fetch_jobs, send_result, stream_result
from .logger import log
from .state import get_jobs_progress, Job


async def get_job(
    session: aiohttp.ClientSession,
    num_jobs: int = 1,
) -> list[Job]:
    """Fetch jobs from the job endpoint.

    Args:
        session: The aiohttp client session.
        num_jobs: Number of jobs to request.

    Returns:
        List of Job objects.
    """
    job_data_list = await fetch_jobs(session, num_jobs)

    jobs = []
    for job_data in job_data_list:
        job = Job(
            id=job_data.get("id", ""),
            input=job_data.get("input", {}),
            webhook=job_data.get("webhook"),
        )
        jobs.append(job)

    return jobs


async def run_job(
    handler: Callable[..., Any],
    job: Job,
) -> Dict[str, Any]:
    """Execute a job handler (sync or async, non-generator).

    Args:
        handler: The handler function to execute.
        job: The job to process.

    Returns:
        The job result dictionary.
    """
    job_input = {"id": job.id, "input": job.input}

    try:
        handler_return = handler(job_input)
        job_output = (
            await handler_return
            if inspect.isawaitable(handler_return)
            else handler_return
        )

        # Normalize result
        run_result = {}
        if isinstance(job_output, dict):
            error_msg = job_output.pop("error", None)
            refresh_worker = job_output.pop("refresh_worker", None)
            run_result["output"] = job_output

            if error_msg:
                run_result["error"] = error_msg
            if refresh_worker:
                run_result["refresh_worker"] = True
        else:
            run_result["output"] = job_output

        return run_result

    except Exception as e:
        log.error(f"Handler error: {e}", job_id=job.id)
        log.debug(traceback.format_exc(), job_id=job.id)
        return {"error": str(e)}


async def run_job_generator(
    handler: Callable[..., Any],
    job: Job,
    session: aiohttp.ClientSession,
    return_aggregate: bool = False,
) -> Dict[str, Any]:
    """Execute a generator job handler with streaming.

    Args:
        handler: The generator handler function.
        job: The job to process.
        session: The aiohttp session for streaming results.
        return_aggregate: Whether to aggregate and return all yielded values.

    Returns:
        The final job result dictionary.
    """
    job_input = {"id": job.id, "input": job.input}
    aggregated_output: list[Any] = []

    try:
        if is_async_generator(handler):
            async for partial_result in handler(job_input):
                await stream_result(session, partial_result, job)
                if return_aggregate:
                    aggregated_output.append(partial_result)
        elif is_sync_generator(handler):
            # Run sync generator in thread-safe way
            loop = asyncio.get_event_loop()

            def iterate_sync_gen() -> Generator[Any, None, None]:
                yield from handler(job_input)

            gen = iterate_sync_gen()
            while True:
                try:
                    partial_result = await loop.run_in_executor(None, next, gen)
                    await stream_result(session, partial_result, job)
                    if return_aggregate:
                        aggregated_output.append(partial_result)
                except StopIteration:
                    break

        if return_aggregate:
            return {"output": aggregated_output}
        return {"output": None}

    except Exception as e:
        log.error(f"Generator handler error: {e}", job_id=job.id)
        log.debug(traceback.format_exc(), job_id=job.id)
        return {"error": str(e)}


async def handle_job(
    session: aiohttp.ClientSession,
    config: Dict[str, Any],
    job: Job,
) -> None:
    """Handle a single job from fetch to completion.

    This function:
    1. Adds the job to progress tracking
    2. Executes the handler
    3. Sends the result
    4. Removes the job from progress tracking

    Args:
        session: The aiohttp client session.
        config: The worker configuration.
        job: The job to handle.
    """
    jobs_progress = get_jobs_progress()
    handler = config["handler"]
    is_generator = config.get("_is_generator", False)
    return_aggregate = config.get("return_aggregate_stream", False)

    log.info(f"Processing job {job.id}", job_id=job.id)
    jobs_progress.add(job.id)

    try:
        if is_generator:
            result = await run_job_generator(handler, job, session, return_aggregate)
        else:
            result = await run_job(handler, job)

        # Check for refresh_worker flag
        if isinstance(result, dict) and result.get("refresh_worker"):
            log.info("Handler requested worker refresh", job_id=job.id)
            config["_shutdown"] = True

        # Send result
        await send_result(session, result, job)
        log.info(f"Job {job.id} completed", job_id=job.id)

    except Exception as e:
        log.error(f"Error handling job: {e}", job_id=job.id)
        await send_result(session, {"error": str(e)}, job)

    finally:
        jobs_progress.remove(job.id)
