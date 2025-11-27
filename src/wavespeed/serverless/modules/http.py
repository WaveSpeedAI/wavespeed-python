"""HTTP communication module for the serverless worker."""

from typing import Any, Dict

import aiohttp

from wavespeed.config import get_serverless_env

from .logger import log
from .state import Job


async def _get_auth_header() -> Dict[str, str]:
    """Get the authorization header for API requests.

    Returns:
        Dictionary containing the authorization header.
    """
    api_key = get_serverless_env("API_KEY", "")
    return {"Authorization": f"Bearer {api_key}"}


async def send_result(
    session: aiohttp.ClientSession,
    result: Dict[str, Any],
    job: Job,
) -> bool:
    """Send job result to the output endpoint.

    Args:
        session: The aiohttp client session.
        result: The job result to send.
        job: The job that was processed.

    Returns:
        True if the result was sent successfully.
    """
    output_endpoint = job.webhook or get_serverless_env("OUTPUT_ENDPOINT")
    if not output_endpoint:
        log.warn("No output endpoint configured", job_id=job.id)
        return False

    headers = await _get_auth_header()
    headers["Content-Type"] = "application/json"

    payload = {
        "id": job.id,
        "status": "COMPLETED" if "error" not in result else "FAILED",
    }

    if "error" in result:
        payload["error"] = result["error"]
    else:
        payload["output"] = result.get("output", result)

    try:
        async with session.post(
            output_endpoint,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            if response.status == 200:
                log.debug(f"Result sent successfully for job {job.id}", job_id=job.id)
                return True
            else:
                log.error(
                    f"Failed to send result: {response.status}",
                    job_id=job.id,
                )
                return False
    except aiohttp.ClientError as e:
        log.error(f"HTTP error sending result: {e}", job_id=job.id)
        return False
    except Exception as e:
        log.error(f"Unexpected error sending result: {e}", job_id=job.id)
        return False


async def stream_result(
    session: aiohttp.ClientSession,
    result: Any,
    job: Job,
) -> bool:
    """Send streaming result to the stream endpoint.

    Args:
        session: The aiohttp client session.
        result: The partial result to stream.
        job: The job being processed.

    Returns:
        True if the result was sent successfully.
    """
    stream_endpoint = get_serverless_env("STREAM_ENDPOINT")
    if not stream_endpoint:
        log.warn("No stream endpoint configured", job_id=job.id)
        return False

    headers = await _get_auth_header()
    headers["Content-Type"] = "application/json"

    payload = {
        "id": job.id,
        "status": "IN_PROGRESS",
        "stream": result,
    }

    try:
        async with session.post(
            stream_endpoint,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            if response.status == 200:
                log.trace(f"Stream result sent for job {job.id}", job_id=job.id)
                return True
            else:
                log.warn(
                    f"Failed to send stream result: {response.status}",
                    job_id=job.id,
                )
                return False
    except aiohttp.ClientError as e:
        log.error(f"HTTP error streaming result: {e}", job_id=job.id)
        return False
    except Exception as e:
        log.error(f"Unexpected error streaming result: {e}", job_id=job.id)
        return False


async def fetch_jobs(
    session: aiohttp.ClientSession,
    num_jobs: int = 1,
) -> list[Dict[str, Any]]:
    """Fetch jobs from the job endpoint.

    Args:
        session: The aiohttp client session.
        num_jobs: Number of jobs to request.

    Returns:
        List of job dictionaries.
    """
    job_endpoint = get_serverless_env("JOB_ENDPOINT")
    if not job_endpoint:
        log.warn("No job endpoint configured")
        return []

    headers = await _get_auth_header()

    # Add query parameter for number of jobs
    url = f"{job_endpoint}?batch_size={num_jobs}"

    try:
        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            if response.status == 200:
                data = await response.json()
                jobs = data.get("jobs", [])
                if jobs:
                    log.debug(f"Fetched {len(jobs)} job(s)")
                return jobs
            elif response.status == 204:
                # No jobs available
                return []
            else:
                log.warn(f"Failed to fetch jobs: {response.status}")
                return []
    except aiohttp.ClientError as e:
        log.error(f"HTTP error fetching jobs: {e}")
        return []
    except Exception as e:
        log.error(f"Unexpected error fetching jobs: {e}")
        return []
