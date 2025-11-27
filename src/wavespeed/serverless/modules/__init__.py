"""WaveSpeedAI serverless modules."""

from .handler import (
    get_handler_type,
    is_async,
    is_async_generator,
    is_generator,
    is_sync_generator,
)
from .heartbeat import Heartbeat
from .http import fetch_jobs, send_result, stream_result
from .job import get_job, handle_job, run_job, run_job_generator
from .local import run_local
from .logger import log, LogLevel, WaverlessLogger
from .progress import async_progress_update, progress_update
from .scaler import JobScaler
from .state import get_jobs_progress, get_worker_id, Job, JobsProgress, set_worker_id

__all__ = [
    # State
    "Job",
    "JobsProgress",
    "get_jobs_progress",
    "get_worker_id",
    "set_worker_id",
    # Handler
    "get_handler_type",
    "is_async",
    "is_async_generator",
    "is_generator",
    "is_sync_generator",
    # Job
    "get_job",
    "handle_job",
    "run_job",
    "run_job_generator",
    # HTTP
    "fetch_jobs",
    "send_result",
    "stream_result",
    # Logger
    "LogLevel",
    "WaverlessLogger",
    "log",
    # Progress
    "async_progress_update",
    "progress_update",
    # Scaler
    "JobScaler",
    # Heartbeat
    "Heartbeat",
    # Local
    "run_local",
]
