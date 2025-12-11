"""Configuration module for WaveSpeed SDK."""

import os
import sys
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
    # Check for RunPod environment
    if os.environ.get("RUNPOD_POD_ID"):
        return "runpod"

    # Check for native Waverless environment
    if os.environ.get("WAVERLESS_POD_ID"):
        return "waverless"

    return None


def _resolve_url(url_template: Optional[str], pod_id: str) -> Optional[str]:
    """Replace pod ID placeholder in URL template.

    Note: Only $RUNPOD_POD_ID is replaced here. The $ID placeholder is
    replaced later at runtime with the actual job ID in http._handle_result.

    Args:
        url_template: URL template with $RUNPOD_POD_ID placeholder.
        pod_id: The worker/pod ID to substitute.

    Returns:
        URL with pod ID placeholder replaced, or None if template is None.
    """
    if not url_template:
        return None
    return url_template.replace("$RUNPOD_POD_ID", pod_id)


def _load_runpod_serverless_config() -> None:
    """Load RunPod environment variables into serverless config."""
    # Worker identification
    serverless.pod_id = os.environ.get("RUNPOD_POD_ID") or ""

    # API endpoint templates
    serverless.webhook_get_job = os.environ.get("RUNPOD_WEBHOOK_GET_JOB")
    serverless.webhook_post_output = os.environ.get("RUNPOD_WEBHOOK_POST_OUTPUT")
    serverless.webhook_post_stream = os.environ.get("RUNPOD_WEBHOOK_POST_STREAM")
    serverless.webhook_ping = os.environ.get("RUNPOD_WEBHOOK_PING")

    # Resolved API endpoints (with pod_id substituted)
    serverless.job_get_url = _resolve_url(serverless.webhook_get_job, serverless.pod_id)
    serverless.job_done_url = _resolve_url(
        serverless.webhook_post_output, serverless.pod_id
    )
    serverless.job_stream_url = _resolve_url(
        serverless.webhook_post_stream, serverless.pod_id
    )
    serverless.ping_url = _resolve_url(serverless.webhook_ping, serverless.pod_id)

    # Authentication
    serverless.api_key = os.environ.get("RUNPOD_AI_API_KEY")

    # Logging (try both log level vars)
    log_level = os.environ.get("RUNPOD_LOG_LEVEL")
    if not log_level:
        log_level = os.environ.get("RUNPOD_DEBUG_LEVEL")
    serverless.log_level = log_level or "INFO"

    # Endpoint identification
    serverless.endpoint_id = os.environ.get("RUNPOD_ENDPOINT_ID")
    serverless.project_id = os.environ.get("RUNPOD_PROJECT_ID")
    serverless.pod_hostname = os.environ.get("RUNPOD_POD_HOSTNAME")

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
    # Worker identification
    serverless.pod_id = os.environ.get("WAVERLESS_POD_ID") or ""

    # API endpoint templates
    serverless.webhook_get_job = os.environ.get("WAVERLESS_WEBHOOK_GET_JOB")
    serverless.webhook_post_output = os.environ.get("WAVERLESS_WEBHOOK_POST_OUTPUT")
    serverless.webhook_post_stream = os.environ.get("WAVERLESS_WEBHOOK_POST_STREAM")
    serverless.webhook_ping = os.environ.get("WAVERLESS_WEBHOOK_PING")

    # Resolved API endpoints (with pod_id substituted)
    serverless.job_get_url = _resolve_url(serverless.webhook_get_job, serverless.pod_id)
    serverless.job_done_url = _resolve_url(
        serverless.webhook_post_output, serverless.pod_id
    )
    serverless.job_stream_url = _resolve_url(
        serverless.webhook_post_stream, serverless.pod_id
    )
    serverless.ping_url = _resolve_url(serverless.webhook_ping, serverless.pod_id)

    # Authentication
    serverless.api_key = os.environ.get("WAVERLESS_API_KEY")

    # Logging
    serverless.log_level = os.environ.get("WAVERLESS_LOG_LEVEL", "INFO")

    # Endpoint identification
    serverless.endpoint_id = os.environ.get("WAVERLESS_ENDPOINT_ID")
    serverless.project_id = os.environ.get("WAVERLESS_PROJECT_ID")
    serverless.pod_hostname = os.environ.get("WAVERLESS_POD_HOSTNAME")

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
