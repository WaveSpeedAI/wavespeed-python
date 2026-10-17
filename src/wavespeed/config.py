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

    # Overall wait cap in seconds for a task. None (default) means no cap: the
    # SDK waits until the task reaches a terminal status. An explicit
    # ``timeout=`` argument takes priority over this value.
    timeout: Optional[float] = None

    # Per-request read timeout in seconds (time waiting for the server, not a
    # total-transfer deadline). Bounds a half-open connection to one request
    # instead of the whole task. Defaults by request type:
    # result queries and other read-only GETs
    read_timeout_get: float = 30.0
    # async submit POST (a timeout here is an ambiguous outcome, so more headroom)
    read_timeout_submit: float = 60.0
    # upload ticket POST and file PUT
    read_timeout_upload: float = 120.0
    # Sync-mode submit stays open until the task finishes: no default limit.

    # Optional single override applied to all of the above (not sync-mode
    # submit). None keeps the per-type defaults.
    read_timeout: Optional[float] = None

    # Maximum number of retries for the entire operation (task-level retries)
    max_retries: int = 0

    # Maximum retries for idempotent result-query GET requests. Submission
    # POSTs are intentionally sent at most once.
    max_connection_retries: int = 5

    # Base interval between retries in seconds (actual delay = retry_interval * attempt)
    retry_interval: float = 1.0


# adds patch, save_config, etc
install_config_module(sys.modules[__name__])
