"""WaveSpeed API client implementation."""

import os
import time
from typing import Any, BinaryIO

import requests

from wavespeed.config import api as api_config


class Client:
    """WaveSpeed API client.

    Args:
        api_key: WaveSpeed API key. If not provided, uses wavespeed.config.api.api_key.
        base_url: Base URL for the API. If not provided, uses wavespeed.config.api.base_url.
        connection_timeout: Timeout for HTTP requests in seconds.

    Example:
        client = Client(api_key="your-api-key")
        output = client.run("wavespeed-ai/z-image/turbo", input={"prompt": "Hello"})
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        connection_timeout: float | None = None,
    ) -> None:
        """Initialize the client."""
        self.api_key = api_key or api_config.api_key
        self.base_url = (base_url or api_config.base_url).rstrip("/")
        self.connection_timeout = connection_timeout or api_config.connection_timeout

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

    def _submit(self, model: str, input: dict[str, Any] | None) -> str:
        """Submit a prediction request.

        Args:
            model: Model identifier.
            input: Input parameters.

        Returns:
            Request ID for polling.

        Raises:
            RuntimeError: If submission fails.
        """
        url = f"{self.base_url}/api/v3/{model}"
        body = input or {}

        response = requests.post(
            url, json=body, headers=self._get_headers(), timeout=self.connection_timeout
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to submit prediction: HTTP {response.status_code}: "
                f"{response.text}"
            )

        result = response.json()
        request_id = result.get("data", {}).get("id")

        if not request_id:
            raise RuntimeError(f"No request ID in response: {result}")

        return request_id

    def _get_result(self, request_id: str) -> dict[str, Any]:
        """Get prediction result.

        Args:
            request_id: The prediction request ID.

        Returns:
            Full API response.

        Raises:
            RuntimeError: If fetching result fails.
        """
        url = f"{self.base_url}/api/v3/predictions/{request_id}/result"

        response = requests.get(
            url, headers=self._get_headers(), timeout=self.connection_timeout
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to get result: HTTP {response.status_code}: {response.text}"
            )

        return response.json()

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
                    raise TimeoutError(f"Prediction timed out after {timeout} seconds")

            result = self._get_result(request_id)
            data = result.get("data", {})
            status = data.get("status")

            if status == "completed":
                return {"outputs": data.get("outputs", [])}

            if status == "failed":
                error = data.get("error") or "Unknown error"
                raise RuntimeError(f"Prediction failed: {error}")

            time.sleep(poll_interval)

    def run(
        self,
        model: str,
        input: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> dict[str, Any]:
        """Run a model and wait for the output.

        Args:
            model: Model identifier (e.g., "wavespeed-ai/flux-dev").
            input: Input parameters for the model.
            timeout: Maximum time to wait for completion (None = no timeout).
            poll_interval: Interval between status checks in seconds.

        Returns:
            Dict containing "outputs" array with model outputs.

        Raises:
            ValueError: If API key is not configured.
            RuntimeError: If the prediction fails.
            TimeoutError: If the prediction times out.
        """
        request_id = self._submit(model, input)
        return self._wait(request_id, timeout, poll_interval)

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
