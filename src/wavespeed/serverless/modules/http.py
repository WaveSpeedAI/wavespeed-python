"""HTTP communication module for the serverless worker."""

import json
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp_retry import FibonacciRetry, RetryClient

from wavespeed.config import serverless

from .logger import log
from .state import JobsProgress


async def _transmit(
    session: aiohttp.ClientSession,
    url: str,
    job_data: str,
) -> None:
    """Transmit results via POST with retry logic.

    Args:
        session: The aiohttp client session.
        url: The URL to post to.
        job_data: JSON-encoded job data string.
    """
    retry_options = FibonacciRetry(attempts=3)
    retry_client = RetryClient(client_session=session, retry_options=retry_options)

    headers = {
        "charset": "utf-8",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    async with retry_client.post(
        url,
        data=job_data,
        headers=headers,
        raise_for_status=True,
    ) as response:
        await response.text()


async def _handle_result(
    session: aiohttp.ClientSession,
    job_data: Dict[str, Any],
    job: Dict[str, Any],
    base_url: str,
    log_message: str,
    is_stream: bool = False,
    is_final: bool = False,
) -> None:
    """Handle sending a job result.

    Args:
        session: The aiohttp client session.
        job_data: The job result data.
        job: The job dictionary.
        base_url: Pre-computed base URL (with pod_id already substituted).
        log_message: Message to log on success.
        is_stream: Whether this is a streaming result.
        is_final: Whether this is the final result (for logging).
    """
    try:
        # Set request ID header
        session.headers["X-Request-ID"] = job["id"]

        # Serialize job data
        serialized_job_data = json.dumps(job_data, ensure_ascii=False)

        # Build URL with job ID and stream flag
        is_stream_str = "true" if is_stream else "false"
        url = base_url.replace("$ID", job["id"]) + f"&isStream={is_stream_str}"

        await _transmit(session, url, serialized_job_data)
        log.debug(log_message, job_id=job["id"])

    except aiohttp.ClientError as err:
        log.error(f"Failed to return job results. | {err}", job_id=job["id"])

    except (TypeError, RuntimeError) as err:
        log.error(f"Error while returning job result. | {err}", job_id=job["id"])

    finally:
        # Log completion for non-streaming final results
        if is_final and job_data.get("status", None) != "IN_PROGRESS":
            log.info("Finished.", job_id=job["id"])


async def send_result(
    session: aiohttp.ClientSession,
    job_data: Dict[str, Any],
    job: Dict[str, Any],
    is_stream: bool = False,
) -> None:
    """Send the final job result.

    Args:
        session: The aiohttp client session.
        job_data: The job result data.
        job: The job dictionary.
        is_stream: Whether this job used streaming.
    """
    job_done_url = serverless.job_done_url or ""
    await _handle_result(
        session,
        job_data,
        job,
        job_done_url,
        "Results sent.",
        is_stream=is_stream,
        is_final=True,
    )


async def stream_result(
    session: aiohttp.ClientSession,
    job_data: Dict[str, Any],
    job: Dict[str, Any],
) -> None:
    """Send a streaming/intermediate job result.

    Args:
        session: The aiohttp client session.
        job_data: The partial result data.
        job: The job dictionary.
    """
    job_stream_url = serverless.job_stream_url or ""
    await _handle_result(
        session, job_data, job, job_stream_url, "Intermediate results sent."
    )


def _build_job_get_url(batch_size: int = 1) -> str:
    """Build the URL for fetching jobs.

    Args:
        batch_size: Number of jobs to request.

    Returns:
        The constructed URL for job fetching.
    """
    job_get_url = serverless.job_get_url or ""

    if batch_size > 1:
        # Use batch API for multiple jobs
        job_take_url = job_get_url.replace("/job-take/", "/job-take-batch/")
        job_take_url += f"&batch_size={batch_size}"
    else:
        job_take_url = job_get_url

    # Add job_in_progress flag
    jobs_progress = JobsProgress()
    job_in_progress = "1" if jobs_progress.get_all() else "0"
    job_take_url += f"&job_in_progress={job_in_progress}"

    log.debug(f"Job get URL: {job_take_url}")
    return job_take_url


async def fetch_jobs(
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
    if not serverless.job_get_url:
        log.warn("No job endpoint configured")
        return None

    url = _build_job_get_url(num_jobs)

    try:
        async with session.get(url) as response:
            log.debug(f"Job fetch response: {response.status}")

            if response.status == 204:
                log.debug("No jobs available (204)")
                return None

            if response.status == 400:
                # Expected when FlashBoot is enabled
                log.debug("Received 400, expected when FlashBoot is enabled")
                return None

            if response.status == 429:
                # Too many requests - raise for special handling
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=429,
                    message="Too many requests",
                )

            # All other errors should raise
            response.raise_for_status()

            # Verify content type
            if response.content_type != "application/json":
                log.debug(f"Unexpected content type: {response.content_type}")
                return None

            # Check for empty content
            if response.content_length == 0:
                log.debug("No content to parse")
                return None

            try:
                jobs = await response.json()
                log.debug("Received job(s)")
            except aiohttp.ContentTypeError:
                log.debug("Response content is not valid JSON")
                return None
            except ValueError as json_error:
                log.debug(f"Failed to parse JSON: {json_error}")
                return None

            # Legacy singular job-take API returns dict
            if isinstance(jobs, dict):
                if "id" not in jobs or "input" not in jobs:
                    raise ValueError("Job missing required fields: id or input")
                return [jobs]

            # Batch job-take API returns list
            if isinstance(jobs, list):
                return jobs

            return None

    except aiohttp.ClientResponseError as e:
        if e.status == 429:
            raise  # Re-raise for special handling by caller
        log.error(f"HTTP error fetching jobs: {e}")
        return None
    except aiohttp.ClientError as e:
        log.error(f"Client error fetching jobs: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error fetching jobs: {e}")
        return None
