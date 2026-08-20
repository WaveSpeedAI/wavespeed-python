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

    # Maximum retries for idempotent result-query GET requests. Submission
    # POSTs are intentionally sent at most once.
    max_connection_retries: int = 5

    # Base interval between retries in seconds (actual delay = retry_interval * attempt)
    retry_interval: float = 1.0


# adds patch, save_config, etc
install_config_module(sys.modules[__name__])
