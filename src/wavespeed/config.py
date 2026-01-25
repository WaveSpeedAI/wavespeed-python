"""Configuration module for WaveSpeed SDK."""

import os
import sys
import uuid
from typing import Optional

from ._config_module import install_config_module


_save_config_ignore = {
    # workaround: "Can't pickle <function ...>"
}


class api:
    """API client configuration options."""

    # Authentication
    api_key: Optional[str] = os.environ.get("WAVESPEED_API_KEY")

    # API base URL
    base_url: str = "https://api.wavespeed.ai"

    # Connection timeout in seconds
    connection_timeout: float = 10.0

    # Total API call timeout in seconds
    timeout: float = 36000.0

    # Maximum number of retries for the entire operation (task-level retries)
    max_retries: int = 0

    # Maximum number of retries for individual HTTP requests (connection errors, timeouts)
    max_connection_retries: int = 5

    # Base interval between retries in seconds (actual delay = retry_interval * attempt)
    retry_interval: float = 1.0


class serverless:
    """Serverless configuration options.

    These attributes are populated by load_serverless_config() based on
    the detected environment (RunPod or Waverless).
    """

    # Worker identification
    pod_id: Optional[str] = None

    # API endpoint templates (with $RUNPOD_POD_ID or $ID placeholder)
    webhook_get_job: Optional[str] = None
    webhook_post_output: Optional[str] = None
    webhook_post_stream: Optional[str] = None
    webhook_ping: Optional[str] = None

    # Resolved API endpoints (with placeholders replaced by pod_id)
    job_get_url: Optional[str] = None
    job_done_url: Optional[str] = None
    job_stream_url: Optional[str] = None
    ping_url: Optional[str] = None

    # Authentication
    api_key: Optional[str] = None

    # Logging
    log_level: str = "INFO"

    # Endpoint identification
    endpoint_id: Optional[str] = None
    project_id: Optional[str] = None
    pod_hostname: Optional[str] = None

    # Timing and concurrency
    ping_interval: int = 10000  # milliseconds
    realtime_port: int = 0
    realtime_concurrency: int = 1


def _detect_serverless_env() -> Optional[str]:
    """Detect the serverless environment type.

    Returns:
        The serverless environment type ("runpod", "waverless") or None
        if not running in a known serverless environment.
    """

    # Check for native Waverless environment
    if os.environ.get("WAVERLESS_ENDPOINT_ID"):
        return "waverless"

    # Check for RunPod environment
    if os.environ.get("RUNPOD_ENDPOINT_ID"):
        return "runpod"

    return None


def _generate_pod_id(endpoint_id: Optional[str], raw_pod_id: Optional[str]) -> str:
    """Generate or resolve pod_id.

    Priority: raw_pod_id > DEVICE_ID > auto-generate

    Args:
        endpoint_id: The endpoint identifier.
        raw_pod_id: The raw pod_id from environment variable.

    Returns:
        The resolved pod_id.
    """
    if raw_pod_id:
        return raw_pod_id
    device_id = os.environ.get("DEVICE_ID")
    if device_id:
        return device_id
    prefix = endpoint_id or "worker"
    return f"{prefix}-{uuid.uuid4().hex}"


def _resolve_runpod_url(url_template: Optional[str], pod_id: str) -> Optional[str]:
    """Replace pod ID placeholder in RunPod URL template.

    Args:
        url_template: URL template with $RUNPOD_POD_ID placeholder.
        pod_id: The worker/pod ID to substitute.

    Returns:
        URL with pod ID placeholder replaced, or None if template is None.
    """
    if not url_template:
        return None
    return url_template.replace("$RUNPOD_POD_ID", pod_id)


def _resolve_waverless_url(url_template: Optional[str], pod_id: str) -> Optional[str]:
    """Replace pod ID placeholder in Waverless URL template.

    Args:
        url_template: URL template with $WAVERLESS_POD_ID placeholder.
        pod_id: The worker/pod ID to substitute.

    Returns:
        URL with $WAVERLESS_POD_ID placeholder replaced, or None if template is None.
    """
    if not url_template:
        return None
    return url_template.replace("$WAVERLESS_POD_ID", pod_id)


def _load_runpod_serverless_config() -> None:
    """Load RunPod environment variables into serverless config."""
    # Endpoint identification (load first for pod_id generation)
    serverless.endpoint_id = os.environ.get("RUNPOD_ENDPOINT_ID")
    serverless.project_id = os.environ.get("RUNPOD_PROJECT_ID")

    # Worker identification
    raw_pod_id = os.environ.get("RUNPOD_POD_ID")
    serverless.pod_id = _generate_pod_id(serverless.endpoint_id, raw_pod_id)
    serverless.pod_hostname = os.environ.get("RUNPOD_POD_HOSTNAME", serverless.pod_id)

    # API endpoint templates
    serverless.webhook_get_job = os.environ.get("RUNPOD_WEBHOOK_GET_JOB")
    serverless.webhook_post_output = os.environ.get("RUNPOD_WEBHOOK_POST_OUTPUT")
    serverless.webhook_post_stream = os.environ.get("RUNPOD_WEBHOOK_POST_STREAM")
    serverless.webhook_ping = os.environ.get("RUNPOD_WEBHOOK_PING")

    # Resolved API endpoints (with $RUNPOD_POD_ID substituted)
    job_get_url = _resolve_runpod_url(serverless.webhook_get_job, serverless.pod_id)
    # job_get_url also needs $ID replaced with worker ID (like runpod-python)
    if job_get_url:
        job_get_url = job_get_url.replace("$ID", serverless.pod_id)
    serverless.job_get_url = job_get_url

    # job_done_url keeps $ID for runtime replacement with job_id
    serverless.job_done_url = _resolve_runpod_url(
        serverless.webhook_post_output, serverless.pod_id
    )
    serverless.job_stream_url = _resolve_runpod_url(
        serverless.webhook_post_stream, serverless.pod_id
    )
    serverless.ping_url = _resolve_runpod_url(
        serverless.webhook_ping, serverless.pod_id
    )

    # Authentication
    serverless.api_key = os.environ.get("RUNPOD_AI_API_KEY")

    # Logging (try both log level vars)
    log_level = os.environ.get("RUNPOD_LOG_LEVEL")
    if not log_level:
        log_level = os.environ.get("RUNPOD_DEBUG_LEVEL")
    serverless.log_level = log_level or "INFO"

    # Timing and concurrency
    ping_interval = os.environ.get("RUNPOD_PING_INTERVAL")
    if ping_interval:
        serverless.ping_interval = int(ping_interval)

    realtime_port = os.environ.get("RUNPOD_REALTIME_PORT")
    if realtime_port:
        serverless.realtime_port = int(realtime_port)

    realtime_concurrency = os.environ.get("RUNPOD_REALTIME_CONCURRENCY")
    if realtime_concurrency:
        serverless.realtime_concurrency = int(realtime_concurrency)


def _load_waverless_serverless_config() -> None:
    """Load Waverless environment variables into serverless config."""
    # Endpoint identification (load first for pod_id generation)
    serverless.endpoint_id = os.environ.get("WAVERLESS_ENDPOINT_ID")
    # Endpoint identification (endpoint_id already set above)
    serverless.project_id = os.environ.get("WAVERLESS_PROJECT_ID")

    # Worker identification
    raw_pod_id = os.environ.get("WAVERLESS_POD_ID")
    serverless.pod_id = _generate_pod_id(serverless.endpoint_id, raw_pod_id)
    serverless.pod_hostname = os.environ.get(
        "WAVERLESS_POD_HOSTNAME", serverless.pod_id
    )

    # API endpoint templates
    serverless.webhook_get_job = os.environ.get("WAVERLESS_WEBHOOK_GET_JOB")
    serverless.webhook_post_output = os.environ.get("WAVERLESS_WEBHOOK_POST_OUTPUT")
    serverless.webhook_post_stream = os.environ.get("WAVERLESS_WEBHOOK_POST_STREAM")
    serverless.webhook_ping = os.environ.get("WAVERLESS_WEBHOOK_PING")

    # Resolved API endpoints (with $WAVERLESS_POD_ID substituted)
    job_get_url = _resolve_waverless_url(serverless.webhook_get_job, serverless.pod_id)
    # job_get_url also needs $ID replaced with worker ID (like runpod)
    if job_get_url:
        job_get_url = job_get_url.replace("$ID", serverless.pod_id)
    serverless.job_get_url = job_get_url

    # job_done_url keeps $ID for runtime replacement with job_id
    serverless.job_done_url = _resolve_waverless_url(
        serverless.webhook_post_output, serverless.pod_id
    )
    serverless.job_stream_url = _resolve_waverless_url(
        serverless.webhook_post_stream, serverless.pod_id
    )
    serverless.ping_url = _resolve_waverless_url(
        serverless.webhook_ping, serverless.pod_id
    )

    # Authentication
    serverless.api_key = os.environ.get("WAVERLESS_API_KEY")

    # Logging
    serverless.log_level = os.environ.get("WAVERLESS_LOG_LEVEL", "INFO")

    # Timing and concurrency
    ping_interval = os.environ.get("WAVERLESS_PING_INTERVAL")
    if ping_interval:
        serverless.ping_interval = int(ping_interval)

    realtime_port = os.environ.get("WAVERLESS_REALTIME_PORT")
    if realtime_port:
        serverless.realtime_port = int(realtime_port)

    realtime_concurrency = os.environ.get("WAVERLESS_REALTIME_CONCURRENCY")
    if realtime_concurrency:
        serverless.realtime_concurrency = int(realtime_concurrency)


# adds patch, save_config, etc
install_config_module(sys.modules[__name__])

# Auto-detect and load serverless config at import time
_detected_env = _detect_serverless_env()
if _detected_env == "runpod":
    _load_runpod_serverless_config()
elif _detected_env == "waverless":
    _load_waverless_serverless_config()
