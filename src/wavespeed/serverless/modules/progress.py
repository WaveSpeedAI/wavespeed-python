"""Progress update module for the serverless worker."""

import threading
from typing import Any, Dict

import aiohttp
import requests

from wavespeed.config import serverless

from .logger import log
from .state import Job


def progress_update(job: Job | Dict[str, Any], progress: Any) -> bool:
    """Send a progress update for a job.

    This function runs in a separate thread to avoid blocking
    the main job processing.

    Args:
        job: The job or job dict to update progress for.
        progress: The progress data to send.

    Returns:
        True if the update was sent successfully.
    """
    # Extract job ID
    if isinstance(job, Job):
        job_id = job.id
    elif isinstance(job, dict):
        job_id = job.get("id", "")
    else:
        log.warn("Invalid job type for progress update")
        return False

    # Get output endpoint
    if not serverless.webhook_post_output:
        log.warn("No output endpoint configured for progress update")
        return False

    # Start thread to send update
    thread = threading.Thread(
        target=_send_progress_update,
        args=(serverless.webhook_post_output, job_id, progress),
        daemon=True,
    )
    thread.start()
    return True


def _send_progress_update(endpoint: str, job_id: str, progress: Any) -> None:
    """Send progress update in a background thread.

    Args:
        endpoint: The output endpoint URL.
        job_id: The job ID.
        progress: The progress data.
    """
    api_key = serverless.api_key if serverless.api_key else ""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "id": job_id,
        "status": "IN_PROGRESS",
        "output": progress,
    }

    try:
        response = requests.post(
            endpoint,
            json=payload,
            headers=headers,
            timeout=10,
        )
        if response.status_code == 200:
            log.trace(f"Progress update sent for job {job_id}")
        else:
            log.warn(f"Progress update failed: {response.status_code}")
    except requests.RequestException as e:
        log.warn(f"Progress update error: {e}")
    except Exception as e:
        log.error(f"Unexpected progress update error: {e}")


async def async_progress_update(
    session: aiohttp.ClientSession,
    job: Job,
    progress: Any,
) -> bool:
    """Send a progress update asynchronously.

    Args:
        session: The aiohttp client session.
        job: The job to update progress for.
        progress: The progress data to send.

    Returns:
        True if the update was sent successfully.
    """
    if not serverless.webhook_post_output:
        log.warn("No output endpoint configured for progress update")
        return False

    api_key = serverless.api_key if serverless.api_key else ""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "id": job.id,
        "status": "IN_PROGRESS",
        "output": progress,
    }

    try:
        async with session.post(
            serverless.webhook_post_output,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            if response.status == 200:
                log.trace(f"Progress update sent for job {job.id}", job_id=job.id)
                return True
            else:
                log.warn(
                    f"Progress update failed: {response.status}",
                    job_id=job.id,
                )
                return False
    except aiohttp.ClientError as e:
        log.warn(f"Progress update error: {e}", job_id=job.id)
        return False
    except Exception as e:
        log.error(f"Unexpected progress update error: {e}", job_id=job.id)
        return False
