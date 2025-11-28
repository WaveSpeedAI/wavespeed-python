"""Job fetching and execution module for the serverless worker."""

import asyncio
import inspect
import json
import traceback
from typing import Any, Callable, Dict, Generator, List, Optional

import aiohttp

from wavespeed import __version__ as wavespeed_version
from wavespeed.config import serverless

from .handler import is_async_generator, is_sync_generator
from .http import fetch_jobs, send_result, stream_result
from .logger import log
from .state import get_jobs_progress, get_worker_id


async def get_job(
    session: aiohttp.ClientSession,
    num_jobs: int = 1,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch jobs from the job endpoint.

    Args:
        session: The aiohttp client session.
        num_jobs: Number of jobs to request.

    Returns:
        List of job dictionaries, or None if no jobs available.
    """
    return await fetch_jobs(session, num_jobs)


async def run_job(
    handler: Callable[..., Any],
    job: Dict[str, Any],
) -> Dict[str, Any]:
    """Execute a job handler (sync or async, non-generator).

    Args:
        handler: The handler function to execute.
        job: The job dictionary with 'id' and 'input'.

    Returns:
        The job result dictionary.
    """
    job_input = {"id": job["id"], "input": job.get("input", {})}

    try:
        handler_return = handler(job_input)
        job_output = (
            await handler_return
            if inspect.isawaitable(handler_return)
            else handler_return
        )

        # Normalize result
        run_result: Dict[str, Any] = {}
        if isinstance(job_output, dict):
            error_msg = job_output.pop("error", None)
            refresh_worker = job_output.pop("refresh_worker", None)

            # Only include output if non-empty
            if job_output:
                run_result["output"] = job_output

            if error_msg:
                run_result["error"] = error_msg
            if refresh_worker:
                run_result["stopPod"] = True
        else:
            run_result["output"] = job_output

        return run_result

    except Exception as e:
        log.error(f"Handler error: {e}", job_id=job["id"])

        # Build detailed error info (matching runpod-python format)
        error_info = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "error_traceback": traceback.format_exc(),
            "hostname": serverless.pod_hostname or "",
            "worker_id": get_worker_id(),
            "wavespeed_version": wavespeed_version,
        }
        return {"error": json.dumps(error_info)}


async def run_job_generator(
    handler: Callable[..., Any],
    job: Dict[str, Any],
    session: aiohttp.ClientSession,
    return_aggregate: bool = False,
) -> Dict[str, Any]:
    """Execute a generator job handler with streaming.

    Args:
        handler: The generator handler function.
        job: The job dictionary with 'id' and 'input'.
        session: The aiohttp session for streaming results.
        return_aggregate: Whether to aggregate and return all yielded values.

    Returns:
        The final job result dictionary.
    """
    job_input = {"id": job["id"], "input": job.get("input", {})}
    aggregated_output: List[Any] = []
    is_stream = False

    try:
        if is_async_generator(handler):
            async for partial_result in handler(job_input):
                # Stream intermediate result
                stream_output = {"output": partial_result}
                await stream_result(session, stream_output, job)
                is_stream = True
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
                    # Stream intermediate result
                    stream_output = {"output": partial_result}
                    await stream_result(session, stream_output, job)
                    is_stream = True
                    if return_aggregate:
                        aggregated_output.append(partial_result)
                except StopIteration:
                    break

        result: Dict[str, Any] = {}
        if return_aggregate:
            result["output"] = aggregated_output
        # Mark that this was a streaming job
        result["_is_stream"] = is_stream
        return result

    except Exception as e:
        log.error(f"Generator handler error: {e}", job_id=job["id"])

        # Build detailed error info (matching runpod-python format)
        error_info = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "error_traceback": traceback.format_exc(),
            "hostname": serverless.pod_hostname or "",
            "worker_id": get_worker_id(),
            "wavespeed_version": wavespeed_version,
        }
        return {"error": json.dumps(error_info)}


async def handle_job(
    session: aiohttp.ClientSession,
    config: Dict[str, Any],
    job: Dict[str, Any],
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
        job: The job dictionary with 'id' and 'input'.
    """
    jobs_progress = get_jobs_progress()
    handler = config["handler"]
    is_generator = config.get("_is_generator", False)
    return_aggregate = config.get("return_aggregate_stream", False)
    job_id = job["id"]

    log.info(f"Processing job {job_id}", job_id=job_id)
    jobs_progress.add(job)

    try:
        if is_generator:
            result = await run_job_generator(handler, job, session, return_aggregate)
        else:
            result = await run_job(handler, job)

        # Check for stopPod flag (refresh_worker)
        if isinstance(result, dict) and result.get("stopPod"):
            log.info("Handler requested worker refresh", job_id=job_id)
            config["refresh_worker"] = True

        # Determine if this was a streaming job
        is_stream = (
            result.pop("_is_stream", False) if isinstance(result, dict) else False
        )

        # Build final job_data to send
        job_data: Dict[str, Any] = {"id": job_id}

        if result.get("error"):
            job_data["error"] = result["error"]
        elif "output" in result:
            job_data["output"] = result["output"]

        if result.get("stopPod"):
            job_data["stopPod"] = True

        # Send final result
        await send_result(session, job_data, job, is_stream=is_stream)
        log.debug(f"Job {job_id} completed", job_id=job_id)

    except Exception as e:
        log.error(f"Error handling job: {e}", job_id=job_id)
        error_info = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "error_traceback": traceback.format_exc(),
            "hostname": serverless.pod_hostname or "",
            "worker_id": get_worker_id(),
            "wavespeed_version": wavespeed_version,
        }
        await send_result(session, {"id": job_id, "error": json.dumps(error_info)}, job)

    finally:
        jobs_progress.remove(job)
