"""
WaveSpeedAI Python Client — Official Python SDK for WaveSpeedAI inference platform.

This library provides a clean, unified, and high-performance API and serverless
integration layer for your applications. Effortlessly connect to all
WaveSpeedAI models and inference services with zero infrastructure overhead.

Example usage:
    import wavespeed

    output = wavespeed.run(
        "wavespeed-ai/z-image/turbo",
        {"prompt": "A beautiful sunset"}
    )
    print(output["outputs"][0])
"""

try:
    from wavespeed._version import __version__
except ImportError:
    # Version file doesn't exist yet (e.g., during initial development)
    __version__ = "0.0.0.dev0"

# Import config to auto-detect and load serverless environment
from wavespeed import config  # noqa: F401

# Import API client
from wavespeed.api import Client, run, upload

__all__ = ["__version__", "Client", "run", "upload"]
