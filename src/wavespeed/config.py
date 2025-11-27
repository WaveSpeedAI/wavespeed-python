"""Configuration module for WaveSpeed SDK."""

import os
import sys
from typing import Optional

from ._config_module import install_config_module


_save_config_ignore = {
    # workaround: "Can't pickle <function ...>"
}


# Environment variable mapping from RunPod to Waverless
_RUNPOD_TO_WAVERLESS_ENV_MAP = {
    "RUNPOD_POD_ID": "WAVERLESS_WORKER_ID",
    "RUNPOD_WEBHOOK_GET_JOB": "WAVERLESS_JOB_ENDPOINT",
    "RUNPOD_WEBHOOK_POST_OUTPUT": "WAVERLESS_OUTPUT_ENDPOINT",
    "RUNPOD_WEBHOOK_POST_STREAM": "WAVERLESS_STREAM_ENDPOINT",
    "RUNPOD_WEBHOOK_PING": "WAVERLESS_PING_ENDPOINT",
    "RUNPOD_AI_API_KEY": "WAVERLESS_API_KEY",
    "RUNPOD_LOG_LEVEL": "WAVERLESS_LOG_LEVEL",
    "RUNPOD_DEBUG_LEVEL": "WAVERLESS_LOG_LEVEL",
    "RUNPOD_ENDPOINT_ID": "WAVERLESS_ENDPOINT_ID",
    "RUNPOD_PROJECT_ID": "WAVERLESS_PROJECT_ID",
    "RUNPOD_POD_HOSTNAME": "WAVERLESS_POD_HOSTNAME",
    "RUNPOD_PING_INTERVAL": "WAVERLESS_PING_INTERVAL",
    "RUNPOD_REALTIME_PORT": "WAVERLESS_REALTIME_PORT",
    "RUNPOD_REALTIME_CONCURRENCY": "WAVERLESS_REALTIME_CONCURRENCY",
}


def detect_serverless_env() -> str | None:
    """Detect the serverless environment type.

    Returns:
        The serverless environment type ("runpod", "waverless") or None
        if not running in a known serverless environment.
    """
    # Check for RunPod environment
    if os.environ.get("RUNPOD_POD_ID") or os.environ.get("RUNPOD_WEBHOOK_GET_JOB"):
        return "runpod"

    # Check for native Waverless environment
    if os.environ.get("WAVERLESS_WORKER_ID") or os.environ.get(
        "WAVERLESS_JOB_ENDPOINT"
    ):
        return "waverless"

    return None


def load_runpod_serverless_config() -> dict[str, Optional[str]]:
    """Parse RunPod environment variables and set corresponding Waverless configs.

    This function reads all RunPod-specific environment variables and maps
    them to their Waverless equivalents. The mapped values are set in the
    environment so that wavespeed serverless can use them transparently.

    Returns:
        A dictionary of the mapped environment variables and their values.
    """
    mapped_config: dict[str, Optional[str]] = {}

    for runpod_var, waverless_var in _RUNPOD_TO_WAVERLESS_ENV_MAP.items():
        value = os.environ.get(runpod_var)
        if value is not None:
            # Set the Waverless environment variable if not already set
            if waverless_var not in os.environ:
                os.environ[waverless_var] = value
            mapped_config[waverless_var] = value

    return mapped_config


def get_serverless_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get serverless environment variable with WAVERLESS_ prefix.

    Args:
        key: The configuration key (without WAVERLESS_ prefix).
        default: Default value if not found.

    Returns:
        The environment variable value or default.
    """
    return os.environ.get(f"WAVERLESS_{key}", default)


class serverless:
    """Serverless configuration options."""

    # Worker identification
    worker_id: Optional[str] = None

    # API endpoints
    job_endpoint: Optional[str] = None
    output_endpoint: Optional[str] = None
    stream_endpoint: Optional[str] = None
    ping_endpoint: Optional[str] = None

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


# adds patch, save_config, etc
install_config_module(sys.modules[__name__])
