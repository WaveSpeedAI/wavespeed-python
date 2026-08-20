"""WaveSpeed API client implementation."""

from __future__ import annotations

import io
import mimetypes
import os
import platform
import re
import time
import traceback
from typing import Any, BinaryIO

import requests

from wavespeed.config import api as api_config

try:
    from wavespeed._version import __version__
except ImportError:  # pragma: no cover - version file generated at build time
    __version__ = "0.0.0.dev0"

# Default channel-attribution client name (see X-Client-Name header)
_DEFAULT_CLIENT_NAME = "wavespeed-python"


def _get_client_os() -> str:
    """Get the client OS name using the desktop client's vocabulary.

    Returns:
        Lowercase OS identifier ("darwin", "linux", "windows", ...).
    """
    system = platform.system().lower()
    if system == "windows":
        return "windows"
    return system or "unknown"


# HTTP status codes that are safe to retry
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class _SubmissionError(RuntimeError):
    """A task submission failed and must not be retried automatically."""


class Client:
    """WaveSpeed API client.

    Args:
        api_key: WaveSpeed API key. If not provided, uses wavespeed.config.api.api_key.
        base_url: Base URL for the API. If not provided, uses wavespeed.config.api.base_url.
        connection_timeout: Timeout for HTTP requests in seconds.
        max_retries: Maximum number of retries for the entire operation.
        max_connection_retries: Maximum retries for result-query GET requests.
        retry_interval: Base interval between retries in seconds.
        client_name: Channel-attribution name sent as the X-Client-Name header.
            The WAVESPEED_CLIENT_NAME environment variable takes priority,
            then this parameter, then the default "wavespeed-python".

    Example:
        client = Client(api_key="your-api-key")
        output = client.run("wavespeed-ai/z-image/turbo", {"prompt": "Cat"})

        # With sync mode (best-effort single request, waits for result)
        output = client.run("wavespeed-ai/z-image/turbo", {"prompt": "Cat"}, enable_sync_mode=True)

        # Task-level replacement attempts are opt-in; submission POSTs are
        # always sent at most once.
        output = client.run("wavespeed-ai/z-image/turbo", {"prompt": "Cat"}, max_retries=1)
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        connection_timeout: float | None = None,
        max_retries: int | None = None,
        max_connection_retries: int | None = None,
        retry_interval: float | None = None,
        client_name: str | None = None,
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
        # Channel attribution: env var > explicit parameter > default
        self.client_name = (
            os.environ.get("WAVESPEED_CLIENT_NAME")
            or client_name
            or _DEFAULT_CLIENT_NAME
        )

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        """Check if an HTTP status code is retryable.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if the status code indicates a transient error worth retrying.
        """
        return status_code in _RETRYABLE_STATUS_CODES

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
            "X-Client-Name": self.client_name,
            "X-Client-Version": __version__,
            "X-Client-OS": _get_client_os(),
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

        try:
            response = requests.post(
                url, json=body, headers=self._get_headers(), timeout=timeouts
            )
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            raise _SubmissionError(
                "Prediction submission did not return a response. The task may already "
                "have been created, so the SDK will not retry the POST automatically."
            ) from e

        if response.status_code != 200:
            raise _SubmissionError(
                f"Failed to submit prediction: HTTP {response.status_code}: {response.text}"
            )

        result = response.json()

        if enable_sync_mode:
            return None, result

        request_id = result.get("data", {}).get("id")
        if not request_id:
            raise _SubmissionError(f"No request ID in response: {result}")

        return request_id, None

    def get_result(
        self, request_id: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """Fetch a prediction's current state by id.

        Useful for recovering a task whose local wait timed out: the task keeps
        running server-side, and this returns whatever state it has reached.

        Args:
            request_id: The prediction request ID.
            timeout: Request timeout in seconds.

        Returns:
            Full API response, including ``data.status`` and ``data.outputs``.

        Raises:
            RuntimeError: If fetching the result fails after retries.
        """
        return self._get_result(request_id, timeout=timeout)

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

        last_error: Exception | None = None

        for retry in range(self.max_connection_retries + 1):
            try:
                response = requests.get(
                    url, headers=self._get_headers(), timeout=timeouts
                )

                if response.status_code != 200:
                    # Retry on transient server errors (5xx) and rate limiting (429)
                    if self._is_retryable_status(response.status_code):
                        last_error = RuntimeError(
                            f"Failed to get result for task {request_id}: "
                            f"HTTP {response.status_code}: {response.text}"
                        )
                        if retry < self.max_connection_retries:
                            delay = self.retry_interval * (retry + 1)
                            print(
                                f"Server error (HTTP {response.status_code}) getting result "
                                f"on attempt {retry + 1}/{self.max_connection_retries + 1}, "
                                f"retrying in {delay} seconds..."
                            )
                            time.sleep(delay)
                            continue
                        raise last_error

                    raise RuntimeError(
                        f"Failed to get result for task {request_id}: "
                        f"HTTP {response.status_code}: {response.text}"
                    )

                return response.json()

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                last_error = e
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

            if status in ("failed", "cancelled", "timeout"):
                error = data.get("error") or "Unknown error"
                raise RuntimeError(
                    f"Prediction {status} (task_id: {request_id}): {error}"
                )

            time.sleep(poll_interval)

    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error is worth retrying at the task level.

        Args:
            error: The exception to check.

        Returns:
            True if the error is retryable.
        """
        # Submission errors are ambiguous: the server may already have created
        # the task, so never turn them into another POST automatically.
        if isinstance(error, _SubmissionError):
            return False

        # Retry timeout and connection errors from result-query GETs.
        if isinstance(
            error,
            (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                TimeoutError,
            ),
        ):
            return True

        # Retry server errors (5xx) and rate limiting (429)
        if isinstance(error, RuntimeError):
            error_str = str(error)
            if "HTTP 5" in error_str or "HTTP 429" in error_str:
                return True

        return False

    @staticmethod
    def _format_sync_mode_error(data: dict[str, Any]) -> str:
        """Build an actionable error for a non-completed sync-mode response."""
        request_id = data.get("id") or "unknown"
        error = data.get("error") or "Unknown error"
        urls = data.get("urls") or {}
        result_url = urls.get("get") if isinstance(urls, dict) else None

        is_sync_timeout = data.get("code") == 5004 or (
            data.get("status") == "processing" and "Sync mode timed out" in error
        )
        if is_sync_timeout:
            message = f"Sync mode timed out (task_id: {request_id}): {error}"
            if result_url and result_url not in message:
                message += f" Query the result later at: {result_url}"
            return message

        return f"Prediction failed (task_id: {request_id}): {error}"

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
            model: Model identifier (e.g., "wavespeed-ai/z-image/turbo").
            input: Input parameters for the model.
            timeout: Maximum time to wait for completion (None = no timeout).
            poll_interval: Interval between status checks in seconds.
            enable_sync_mode: If True, use synchronous mode (best-effort single
                request). If the server-side sync wait times out, the SDK raises
                an error with the task ID so the result can be queried later.
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
                    status = sync_result.get("data", {}).get("status")
                    if status != "completed":
                        raise RuntimeError(
                            self._format_sync_mode_error(sync_result.get("data", {}))
                        )
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

    @staticmethod
    def _task_id_from_error(error: Exception) -> str:
        """Extract a task ID from an error message, mirroring the JS SDK."""
        match = re.search(r"task_id:\s*([^)]+)", str(error))
        return match.group(1).strip() if match else "unknown"

    def run_no_throw(
        self,
        model: str,
        input: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
        poll_interval: float = 1.0,
        enable_sync_mode: bool = False,
        max_retries: int | None = None,
    ) -> dict[str, Any]:
        """Run a model and return a structured result instead of raising.

        Mirrors the JavaScript SDK's runNoThrow: failures (including
        server-side sync-mode timeouts) are reported in the returned dict
        rather than raised, and the task ID is extracted whenever available
        so the result can be queried later.

        Args:
            model: Model identifier (e.g., "wavespeed-ai/z-image/turbo").
            input: Input parameters for the model.
            timeout: Maximum time to wait for completion (None = no timeout).
            poll_interval: Interval between status checks in seconds.
            enable_sync_mode: If True, use synchronous mode (best-effort
                single request).
            max_retries: Maximum task-level retries (overrides client setting).

        Returns:
            Dict with keys:
                status: "completed", "failed", or "processing" (the latter for
                    server-side sync-mode timeouts where the task is still
                    running).
                outputs: List of model outputs, or None if not completed.
                task_id: The task ID when known, otherwise "unknown".
                error: Error message string, or None on success.

        Example:
            result = client.run_no_throw("wavespeed-ai/z-image/turbo", {"prompt": "Cat"})
            if result["outputs"] is not None:
                print("Success:", result["outputs"], result["task_id"])
            else:
                print("Failed:", result["error"], result["task_id"])
        """
        task_retries = max_retries if max_retries is not None else self.max_retries

        for attempt in range(task_retries + 1):
            try:
                request_id, sync_result = self._submit(
                    model, input, enable_sync_mode=enable_sync_mode, timeout=timeout
                )

                if enable_sync_mode:
                    data = (sync_result or {}).get("data", {})
                    status = data.get("status")
                    task_id = data.get("id") or "unknown"

                    if status != "completed":
                        error_msg = self._format_sync_mode_error(data)
                        is_sync_timeout = data.get("code") == 5004 or (
                            status == "processing"
                            and "Sync mode timed out" in (data.get("error") or "")
                        )
                        return {
                            "status": "processing" if is_sync_timeout else "failed",
                            "outputs": None,
                            "task_id": task_id,
                            "error": error_msg,
                        }

                    return {
                        "status": "completed",
                        "outputs": data.get("outputs", []),
                        "task_id": task_id,
                        "error": None,
                    }

                result = self._wait(request_id, timeout, poll_interval)
                return {
                    "status": "completed",
                    "outputs": result.get("outputs", []),
                    "task_id": request_id,
                    "error": None,
                }

            except Exception as e:  # noqa: BLE001 - by design: never raises
                if self._is_retryable_error(e) and attempt < task_retries:
                    print(f"Task attempt {attempt + 1}/{task_retries + 1} failed: {e}")
                    delay = self.retry_interval * (attempt + 1)
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue

                # Extract the task ID from the error message like the JS SDK
                task_id = self._task_id_from_error(e)
                is_sync_timeout = "sync mode timed out" in str(e).lower()
                return {
                    "status": "processing" if is_sync_timeout else "failed",
                    "outputs": None,
                    "task_id": task_id,
                    "error": str(e),
                }

        # Should not reach here, but just in case
        return {
            "status": "failed",
            "outputs": None,
            "task_id": "unknown",
            "error": f"All {task_retries + 1} attempts failed",
        }

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

        ticket_url = f"{self.base_url}/api/v3/media/uploads"
        headers = self._get_headers()
        timeout = timeout or api_config.timeout
        request_timeout = (min(self.connection_timeout, timeout), timeout)

        close_stream = False
        if isinstance(file, str):
            if not os.path.exists(file):
                raise FileNotFoundError(f"File not found: {file}")
            filename = os.path.basename(file)
            size = os.path.getsize(file)
            stream = open(
                file, "rb"
            )  # noqa: SIM115 - closed in the method's finally block
            close_stream = True
        else:
            filename = getattr(file, "name", "upload")
            if isinstance(filename, str) and os.path.sep in filename:
                filename = os.path.basename(filename)
            stream = file
            try:
                start = stream.tell()
                stream.seek(0, os.SEEK_END)
                size = stream.tell() - start
                stream.seek(start)
            except (AttributeError, OSError, io.UnsupportedOperation):
                data = stream.read(200 * 1024 * 1024 + 1)
                stream = io.BytesIO(data)
                size = len(data)

        content_type = mimetypes.guess_type(str(filename))[0]
        payload: dict[str, Any] = {"filename": str(filename), "size": size}
        if content_type:
            payload["content_type"] = content_type

        try:
            response = requests.post(
                ticket_url, headers=headers, json=payload, timeout=request_timeout
            )
            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to create upload: HTTP {response.status_code}: {response.text}"
                )

            result = response.json()
            if result.get("code") != 200:
                raise RuntimeError(
                    f"Upload failed: {result.get('message', 'Unknown error')}"
                )

            ticket = result.get("data", {})
            instruction = ticket.get("upload", {})
            if instruction.get("method", "").upper() != "PUT" or not instruction.get(
                "url"
            ):
                raise RuntimeError("Upload failed: invalid upload instruction")

            upload_response = requests.put(
                instruction["url"],
                headers=instruction.get("headers", {}),
                data=stream,
                timeout=request_timeout,
            )
            if not 200 <= upload_response.status_code < 300:
                raise RuntimeError(
                    f"Failed to upload file: HTTP {upload_response.status_code}: "
                    f"{upload_response.text}"
                )

            download_url = ticket.get("download_url")
            if not download_url:
                raise RuntimeError("Upload failed: no download_url in response")
            return download_url
        finally:
            if close_stream:
                stream.close()
