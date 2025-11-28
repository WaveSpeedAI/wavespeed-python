"""FastAPI server for local development and testing."""

import asyncio
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp
import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import RedirectResponse

from .handler import is_async_generator, is_generator, is_sync_generator
from .heartbeat import Heartbeat
from .job import run_job
from .logger import log
from .state import JobsProgress

TITLE = "WaveSpeed | Development Worker API"

DESCRIPTION = """
The Development Worker API facilitates testing and debugging of your WaveSpeed workers.
It offers a sandbox environment for executing code and simulating interactions with your worker.

Use this API for comprehensive testing of request submissions and result retrieval,
mimicking the behavior of the operational environment.

---
*Note: This API serves as a local testing tool and will not be utilized once your worker is deployed.*
"""

RUN_DESCRIPTION = """
Initiates processing jobs asynchronously, returning a unique job ID.

**Parameters:**
- **input** (dict): The data to be processed by the worker.
- **webhook** (string, optional): A callback URL for result notification upon completion.

**Returns:**
- **id** (string): A unique identifier for the job.
- **status** (string): The job status (IN_PROGRESS).
"""

RUNSYNC_DESCRIPTION = """
Executes processing jobs synchronously, returning the job's output directly.

**Parameters:**
- **input** (dict): The data to be processed by the worker.
- **webhook** (string, optional): A callback URL for async result notification.

**Returns:**
- **id** (string): The job identifier.
- **status** (string): COMPLETED or FAILED.
- **output** (Any): The job output (if successful).
- **error** (string): Error message (if failed).
"""

STREAM_DESCRIPTION = """
Aggregates the output of a streaming/generator job.

**Parameters:**
- **job_id** (string): The unique identifier of the job.

**Returns:**
- **id** (string): The job identifier.
- **status** (string): COMPLETED or FAILED.
- **stream** (list): The aggregated stream output.
"""

STATUS_DESCRIPTION = """
Checks the completion status of a job and returns its output.

**Parameters:**
- **job_id** (string): The unique identifier for the job.

**Returns:**
- **id** (string): The job identifier.
- **status** (string): COMPLETED, IN_PROGRESS, or FAILED.
- **output** (Any): The job output (if complete).
"""


# Store pending jobs for async execution
_pending_jobs: Dict[str, Dict[str, Any]] = {}
_job_results: Dict[str, Dict[str, Any]] = {}


@dataclass
class JobRequest:
    """Represents a job request."""

    input: Dict[str, Any]
    webhook: Optional[str] = None


@dataclass
class JobResponse:
    """Represents a job response."""

    id: str
    status: str
    output: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class StreamResponse:
    """Represents a stream response."""

    id: str
    status: str
    stream: Optional[List[Any]] = None
    error: Optional[str] = None


async def _send_webhook(url: str, payload: Dict[str, Any]) -> bool:
    """Send a webhook notification."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                return response.status < 400
    except Exception as e:
        log.error(f"Webhook to {url} failed: {e}")
        return False


def _send_webhook_sync(url: str, payload: Dict[str, Any]) -> None:
    """Send webhook in a background thread."""
    asyncio.run(_send_webhook(url, payload))


class WorkerAPI:
    """FastAPI server for local development and testing."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the WorkerAPI.

        Args:
            config: Worker configuration containing the handler.
        """
        self.config = config
        self.heartbeat = Heartbeat()
        self.jobs_progress = JobsProgress()

        tags_metadata = [
            {
                "name": "Sync",
                "description": "Synchronous job execution endpoints.",
            },
            {
                "name": "Async",
                "description": "Asynchronous job submission endpoints.",
            },
            {
                "name": "Status",
                "description": "Job status and result endpoints.",
            },
        ]

        self.app = FastAPI(
            title=TITLE,
            description=DESCRIPTION,
            version="1.0.0",
            docs_url="/",
            openapi_tags=tags_metadata,
        )

        router = APIRouter()

        # Redirect /docs to /
        router.add_api_route(
            "/docs",
            lambda: RedirectResponse(url="/"),
            include_in_schema=False,
        )

        # Async run endpoint
        router.add_api_route(
            "/run",
            self._run,
            methods=["POST"],
            response_model_exclude_none=True,
            summary="Submit a job asynchronously",
            description=RUN_DESCRIPTION,
            tags=["Async"],
        )

        # Sync run endpoint
        router.add_api_route(
            "/runsync",
            self._runsync,
            methods=["POST"],
            response_model_exclude_none=True,
            summary="Submit and wait for job completion",
            description=RUNSYNC_DESCRIPTION,
            tags=["Sync"],
        )

        # Stream endpoint
        router.add_api_route(
            "/stream/{job_id}",
            self._stream,
            methods=["POST"],
            response_model_exclude_none=True,
            summary="Get streaming job output",
            description=STREAM_DESCRIPTION,
            tags=["Status"],
        )

        # Status endpoint
        router.add_api_route(
            "/status/{job_id}",
            self._status,
            methods=["POST"],
            response_model_exclude_none=True,
            summary="Check job status",
            description=STATUS_DESCRIPTION,
            tags=["Status"],
        )

        self.app.include_router(router)

    def start(
        self,
        host: str = "localhost",
        port: int = 8000,
        workers: int = 1,
    ) -> None:
        """Start the FastAPI server.

        Args:
            host: Host to bind to.
            port: Port to bind to.
            workers: Number of worker processes.
        """
        log.info(f"Starting API server at http://{host}:{port}")
        self.heartbeat.start()

        try:
            uvicorn.run(
                self.app,
                host=host,
                port=port,
                workers=workers,
                log_level="info",
                access_log=False,
            )
        finally:
            self.heartbeat.stop()

    async def _run_generator(
        self, handler: Any, job_input: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Run a generator handler and collect outputs.

        Args:
            handler: The generator handler function.
            job_input: The job input dict.

        Returns:
            Dict with aggregated output or error.
        """
        try:
            outputs = []
            if is_async_generator(handler):
                async for partial in handler(job_input):
                    outputs.append(partial)
            elif is_sync_generator(handler):
                for partial in handler(job_input):
                    outputs.append(partial)
            return {"output": outputs}
        except Exception as e:
            log.error(f"Generator handler error: {e}")
            return {"error": str(e)}

    async def _run(self, job_request: JobRequest) -> Dict[str, Any]:
        """Submit a job asynchronously."""
        job_id = f"test-{uuid.uuid4()}"

        # Store the pending job
        _pending_jobs[job_id] = {
            "id": job_id,
            "input": job_request.input,
            "webhook": job_request.webhook,
        }

        return jsonable_encoder({"id": job_id, "status": "IN_PROGRESS"})

    async def _runsync(self, job_request: JobRequest) -> Dict[str, Any]:
        """Submit a job and wait for completion."""
        job_id = f"test-{uuid.uuid4()}"
        job = {"id": job_id, "input": job_request.input}
        job_input = {"id": job_id, "input": job_request.input}

        handler = self.config["handler"]

        if is_generator(handler):
            job_output = await self._run_generator(handler, job_input)
        else:
            job_output = await run_job(handler, job)

        if job_output.get("error"):
            return jsonable_encoder(
                {
                    "id": job_id,
                    "status": "FAILED",
                    "error": job_output["error"],
                }
            )

        # Send webhook if provided
        if job_request.webhook:
            thread = threading.Thread(
                target=_send_webhook_sync,
                args=(job_request.webhook, job_output),
                daemon=True,
            )
            thread.start()

        return jsonable_encoder(
            {
                "id": job_id,
                "status": "COMPLETED",
                "output": job_output.get("output"),
            }
        )

    async def _stream(self, job_id: str) -> Dict[str, Any]:
        """Get streaming job output."""
        if job_id not in _pending_jobs:
            return jsonable_encoder(
                {
                    "id": job_id,
                    "status": "FAILED",
                    "error": "Job ID not found",
                }
            )

        pending = _pending_jobs[job_id]
        job_input = {"id": job_id, "input": pending["input"]}
        handler = self.config["handler"]

        if not is_generator(handler):
            return jsonable_encoder(
                {
                    "id": job_id,
                    "status": "FAILED",
                    "error": "Stream not supported, handler must be a generator.",
                }
            )

        job_output = await self._run_generator(handler, job_input)

        # Clean up
        del _pending_jobs[job_id]

        if job_output.get("error"):
            return jsonable_encoder(
                {
                    "id": job_id,
                    "status": "FAILED",
                    "error": job_output["error"],
                }
            )

        # Format stream output
        stream_output = [{"output": item} for item in job_output.get("output", [])]

        # Send webhook if provided
        if pending.get("webhook"):
            thread = threading.Thread(
                target=_send_webhook_sync,
                args=(pending["webhook"], stream_output),
                daemon=True,
            )
            thread.start()

        return jsonable_encoder(
            {
                "id": job_id,
                "status": "COMPLETED",
                "stream": stream_output,
            }
        )

    async def _status(self, job_id: str) -> Dict[str, Any]:
        """Check job status and get output."""
        if job_id not in _pending_jobs:
            # Check if we have cached results
            if job_id in _job_results:
                return jsonable_encoder(_job_results[job_id])

            return jsonable_encoder(
                {
                    "id": job_id,
                    "status": "FAILED",
                    "error": "Job ID not found",
                }
            )

        pending = _pending_jobs[job_id]
        job = {"id": job_id, "input": pending["input"]}
        job_input = {"id": job_id, "input": pending["input"]}
        handler = self.config["handler"]

        if is_generator(handler):
            job_output = await self._run_generator(handler, job_input)
        else:
            job_output = await run_job(handler, job)

        # Clean up pending job
        del _pending_jobs[job_id]

        if job_output.get("error"):
            result = {
                "id": job_id,
                "status": "FAILED",
                "error": job_output["error"],
            }
        else:
            result = {
                "id": job_id,
                "status": "COMPLETED",
                "output": job_output.get("output"),
            }

        # Cache the result
        _job_results[job_id] = result

        # Send webhook if provided
        if pending.get("webhook"):
            thread = threading.Thread(
                target=_send_webhook_sync,
                args=(pending["webhook"], job_output),
                daemon=True,
            )
            thread.start()

        return jsonable_encoder(result)
