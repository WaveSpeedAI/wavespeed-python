"""WaveSpeed API client module.

Provides a simple interface to run WaveSpeed AI models.

Example usage:
    import wavespeed

    output = wavespeed.run(
        "wavespeed-ai/z-image/turbo",
        {"prompt": "A beautiful sunset over mountains"}
    )

    print(output["outputs"][0])  # First output URL

    # Upload a file
    result = wavespeed.upload("/path/to/image.png")
    print(result["download_url"])
"""

from typing import BinaryIO

from wavespeed.api.client import Client

__all__ = ["Client", "run", "upload"]

# Default client instance
_default_client: Client | None = None


def _get_default_client() -> Client:
    """Get or create the default client instance."""
    global _default_client
    if _default_client is None:
        _default_client = Client()
    return _default_client


def run(
    model: str,
    input: dict | None = None,
    *,
    timeout: float | None = None,
    poll_interval: float = 1.0,
    enable_sync_mode: bool = False,
    max_retries: int | None = None,
) -> dict:
    """Run a model and wait for the output.

    Args:
        model: Model identifier (e.g., "wavespeed-ai/flux-dev").
        input: Input parameters for the model.
        timeout: Maximum time to wait for completion (None = no timeout).
        poll_interval: Interval between status checks in seconds.
        enable_sync_mode: If True, use synchronous mode (single request).
        max_retries: Maximum retries for this request (overrides default setting).

    Returns:
        Dict containing "outputs" array with model outputs.

    Raises:
        ValueError: If API key is not configured.
        RuntimeError: If the prediction fails.
        TimeoutError: If the prediction times out.

    Example:
        output = wavespeed.run(
            "wavespeed-ai/z-image/turbo",
            {"prompt": "A cat sitting on a windowsill"}
        )
        print(output["outputs"][0])  # First output URL

        # With sync mode
        output = wavespeed.run(
            "wavespeed-ai/z-image/turbo",
            {"prompt": "A cat"},
            enable_sync_mode=True
        )

        # With retry
        output = wavespeed.run(
            "wavespeed-ai/z-image/turbo",
            {"prompt": "A cat"},
            max_retries=3
        )
    """
    return _get_default_client().run(
        model,
        input,
        timeout=timeout,
        poll_interval=poll_interval,
        enable_sync_mode=enable_sync_mode,
        max_retries=max_retries,
    )


def upload(file: str | BinaryIO, *, timeout: float | None = None) -> str:
    """Upload a file to WaveSpeed.

    Args:
        file: File path string or file-like object to upload.
        timeout: Total API call timeout in seconds.

    Returns:
        URL of the uploaded file.

    Raises:
        ValueError: If API key is not configured.
        FileNotFoundError: If file path does not exist.
        RuntimeError: If upload fails.

    Example:
        url = wavespeed.upload("/path/to/image.png")
        print(url)
    """
    return _get_default_client().upload(file, timeout=timeout)
