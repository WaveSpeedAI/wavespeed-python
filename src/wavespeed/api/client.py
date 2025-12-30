"""WaveSpeed API client implementation."""

import os
import time
import traceback
from typing import Any, BinaryIO

import requests

from wavespeed.config import api as api_config


class Client:
    """WaveSpeed API client.

    Args:
        api_key: WaveSpeed API key. If not provided, uses wavespeed.config.api.api_key.
        base_url: Base URL for the API. If not provided, uses wavespeed.config.api.base_url.
        connection_timeout: Timeout for HTTP requests in seconds.
        max_retries: Maximum number of retries for the entire operation.
        max_connection_retries: Maximum retries for individual HTTP requests.
        retry_interval: Base interval between retries in seconds.

    Example:
        client = Client(api_key="your-api-key")
        output = client.run("wavespeed-ai/z-image/turbo", {"prompt": "Cat"})

        # With sync mode (single request, waits for result)
        output = client.run("wavespeed-ai/z-image/turbo", {"prompt": "Cat"}, enable_sync_mode=True)

        # With retry
        output = client.run("wavespeed-ai/z-image/turbo", {"prompt": "Cat"}, max_retries=3)
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        connection_timeout: float | None = None,
        max_retries: int | None = None,
        max_connection_retries: int | None = None,
        retry_interval: float | None = None,
    ) -> None:
        """Initialize the client."""
        self.api_key = api_key or api_config.api_key
        self.base_url = (base_url or api_config.base_url).rstrip("/")
        self.connection_timeout = connection_timeout or api_config.connection_timeout
        self.max_retries = (
            max_retries if max_retries is not None else api_config.max_retries
        )
        self.max_connection_retries = (
            max_connection_retries
            if max_connection_retries is not None
            else api_config.max_connection_retries
        )
        self.retry_interval = (
            retry_interval if retry_interval is not None else api_config.retry_interval
        )

    def _get_headers(self) -> dict[str, str]:
        """Get request headers with authentication."""
        if not self.api_key:
            raise ValueError(
                "API key is required. Set WAVESPEED_API_KEY environment variable "
                "or pass api_key to Client()."
            )
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    def _submit(
        self,
        model: str,
        input: dict[str, Any] | None,
        enable_sync_mode: bool = False,
        timeout: float | None = None,
    ) -> tuple[str | None, dict[str, Any] | None]:
        """Submit a prediction request.

        Args:
            model: Model identifier.
            input: Input parameters.
            enable_sync_mode: If True, wait for result in single request.
            timeout: Request timeout in seconds.

        Returns:
            Tuple of (request_id, result). In async mode, result is None.
            In sync mode, request_id is None and result contains the response.

        Raises:
            RuntimeError: If submission fails after retries.
        """
        url = f"{self.base_url}/api/v3/{model}"
        body = dict(input) if input else {}

        if enable_sync_mode:
            body["enable_sync_mode"] = True

        request_timeout = timeout if timeout is not None else api_config.timeout
        # Use connection timeout for connect, request_timeout for read
        connect_timeout = (
            min(self.connection_timeout, request_timeout)
            if request_timeout
            else self.connection_timeout
        )
        timeouts = (connect_timeout, request_timeout)

        for retry in range(self.max_connection_retries + 1):
            try:
                response = requests.post(
                    url, json=body, headers=self._get_headers(), timeout=timeouts
                )

                if response.status_code != 200:
                    raise RuntimeError(
                        f"Failed to submit prediction: HTTP {response.status_code}: "
                        f"{response.text}"
                    )

                result = response.json()

                if enable_sync_mode:
                    return None, result

                request_id = result.get("data", {}).get("id")
                if not request_id:
                    raise RuntimeError(f"No request ID in response: {result}")

                return request_id, None

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                print(
                    f"Connection error on attempt {retry + 1}/{self.max_connection_retries + 1}:"
                )
                traceback.print_exc()

                if retry < self.max_connection_retries:
                    delay = self.retry_interval * (retry + 1)
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise RuntimeError(
                        f"Failed to submit prediction after {self.max_connection_retries + 1} attempts"
                    ) from e

    def _get_result(
        self, request_id: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """Get prediction result.

        Args:
            request_id: The prediction request ID.
            timeout: Request timeout in seconds.

        Returns:
            Full API response.

        Raises:
            RuntimeError: If fetching result fails after retries.
        """
        url = f"{self.base_url}/api/v3/predictions/{request_id}/result"
        request_timeout = timeout if timeout is not None else api_config.timeout
        connect_timeout = (
            min(self.connection_timeout, request_timeout)
            if request_timeout
            else self.connection_timeout
        )
        timeouts = (connect_timeout, request_timeout)

        for retry in range(self.max_connection_retries + 1):
            try:
                response = requests.get(
                    url, headers=self._get_headers(), timeout=timeouts
                )

                if response.status_code != 200:
                    raise RuntimeError(
                        f"Failed to get result for task {request_id}: "
                        f"HTTP {response.status_code}: {response.text}"
                    )

                return response.json()

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                print(
                    f"Connection error getting result on attempt {retry + 1}/{self.max_connection_retries + 1}:"
                )
                traceback.print_exc()

                if retry < self.max_connection_retries:
                    delay = self.retry_interval * (retry + 1)
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise RuntimeError(
                        f"Failed to get result for task {request_id} "
                        f"after {self.max_connection_retries + 1} attempts"
                    ) from e

    def _wait(
        self,
        request_id: str,
        timeout: float | None,
        poll_interval: float,
    ) -> dict[str, Any]:
        """Wait for prediction to complete.

        Args:
            request_id: The prediction request ID.
            timeout: Maximum wait time in seconds (None = no timeout).
            poll_interval: Time between polls in seconds.

        Returns:
            Dict with "outputs" array.

        Raises:
            RuntimeError: If prediction fails.
            TimeoutError: If prediction times out.
        """
        start_time = time.time()

        while True:
            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(
                        f"Prediction timed out after {timeout} seconds (task_id: {request_id})"
                    )

            result = self._get_result(request_id, timeout=timeout)
            data = result.get("data", {})
            status = data.get("status")

            if status == "completed":
                return {"outputs": data.get("outputs", [])}

            if status == "failed":
                error = data.get("error") or "Unknown error"
                raise RuntimeError(
                    f"Prediction failed (task_id: {request_id}): {error}"
                )

            time.sleep(poll_interval)

    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error is worth retrying at the task level.

        Args:
            error: The exception to check.

        Returns:
            True if the error is retryable.
        """
        # Always retry timeout and connection errors
        if isinstance(
            error, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
        ):
            return True

        # Retry server errors (5xx) and rate limiting (429)
        if isinstance(error, RuntimeError):
            error_str = str(error)
            if "HTTP 5" in error_str or "HTTP 429" in error_str:
                return True

        return False

    def run(
        self,
        model: str,
        input: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
        poll_interval: float = 1.0,
        enable_sync_mode: bool = False,
        max_retries: int | None = None,
    ) -> dict[str, Any]:
        """Run a model and wait for the output.

        Args:
            model: Model identifier (e.g., "wavespeed-ai/flux-dev").
            input: Input parameters for the model.
            timeout: Maximum time to wait for completion (None = no timeout).
            poll_interval: Interval between status checks in seconds.
            enable_sync_mode: If True, use synchronous mode (single request).
            max_retries: Maximum task-level retries (overrides client setting).

        Returns:
            Dict containing "outputs" array with model outputs.

        Raises:
            ValueError: If API key is not configured.
            RuntimeError: If the prediction fails.
            TimeoutError: If the prediction times out.
        """
        task_retries = max_retries if max_retries is not None else self.max_retries
        last_error = None

        for attempt in range(task_retries + 1):
            try:
                request_id, sync_result = self._submit(
                    model, input, enable_sync_mode=enable_sync_mode, timeout=timeout
                )

                if enable_sync_mode:
                    # In sync mode, extract outputs from the result
                    data = sync_result.get("data", {})
                    return {"outputs": data.get("outputs", [])}

                return self._wait(request_id, timeout, poll_interval)

            except Exception as e:
                last_error = e
                is_retryable = self._is_retryable_error(e)

                if not is_retryable or attempt >= task_retries:
                    raise

                print(f"Task attempt {attempt + 1}/{task_retries + 1} failed: {e}")
                delay = self.retry_interval * (attempt + 1)
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)

        # Should not reach here, but just in case
        if last_error:
            raise last_error
        raise RuntimeError(f"All {task_retries + 1} attempts failed")

    def upload(self, file: str | BinaryIO, *, timeout: float | None = None) -> str:
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
            url = client.upload("/path/to/image.png")
            print(url)
        """
        if not self.api_key:
            raise ValueError(
                "API key is required. Set WAVESPEED_API_KEY environment variable "
                "or pass api_key to Client()."
            )

        url = f"{self.base_url}/api/v3/media/upload/binary"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        timeout = timeout or api_config.timeout
        request_timeout = (min(self.connection_timeout, timeout), timeout)

        if isinstance(file, str):
            if not os.path.exists(file):
                raise FileNotFoundError(f"File not found: {file}")
            with open(file, "rb") as f:
                files = {"file": (os.path.basename(file), f)}
                response = requests.post(
                    url, headers=headers, files=files, timeout=request_timeout
                )
        else:
            filename = getattr(file, "name", "upload")
            if isinstance(filename, str) and os.path.sep in filename:
                filename = os.path.basename(filename)
            files = {"file": (filename, file)}
            response = requests.post(
                url, headers=headers, files=files, timeout=request_timeout
            )

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to upload file: HTTP {response.status_code}: {response.text}"
            )

        result = response.json()
        if result.get("code") != 200:
            raise RuntimeError(
                f"Upload failed: {result.get('message', 'Unknown error')}"
            )

        download_url = result.get("data", {}).get("download_url")
        if not download_url:
            raise RuntimeError("Upload failed: no download_url in response")

        return download_url
