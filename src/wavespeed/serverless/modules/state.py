"""Global state management for the serverless worker."""

import time
import uuid
from typing import Any, Dict, Optional, Set

from wavespeed.config import serverless


# Global worker ID (pre-computed at config load time, or generated UUID)
WORKER_ID: str = serverless.pod_id or str(uuid.uuid4())

# Reference time for benchmarking (when job count reaches zero)
REF_COUNT_ZERO: float = time.perf_counter()

# Flag indicating local test mode
IS_LOCAL_TEST: bool = serverless.webhook_get_job is None


def get_worker_id() -> str:
    """Get the worker ID.

    Returns:
        The worker ID from config or a generated UUID.
    """
    return WORKER_ID


def set_worker_id(worker_id: str) -> None:
    """Set the worker ID explicitly.

    Args:
        worker_id: The worker ID to set.
    """
    global WORKER_ID
    WORKER_ID = worker_id


class Job:
    """Represents a serverless job.

    Attributes:
        id: Unique job identifier.
        input: The job input data.
        webhook: Optional webhook URL for job completion.
    """

    def __init__(
        self,
        id: str,
        input: Optional[Dict[str, Any]] = None,
        webhook: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a Job.

        Args:
            id: The job ID.
            input: The job input data.
            webhook: Optional webhook URL.
            **kwargs: Additional attributes to set.
        """
        self.id = id
        self.input = input
        self.webhook = webhook
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __hash__(self) -> int:
        """Hash based on job ID."""
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        """Equality based on job ID."""
        if isinstance(other, Job):
            return self.id == other.id
        return False

    def __str__(self) -> str:
        """Return the job ID as string."""
        return self.id


class JobsProgress(Set[Job]):
    """Singleton class to track jobs in progress.

    This class maintains a set of Job objects that are currently being processed.
    Compatible with runpod-python's JobsProgress API.
    """

    _instance: Optional["JobsProgress"] = None

    def __new__(cls) -> "JobsProgress":
        """Create or return the singleton instance."""
        if cls._instance is None:
            cls._instance = set.__new__(cls)
            set.__init__(cls._instance)
        return cls._instance

    def __init__(self) -> None:
        """Initialize (no-op for singleton)."""
        pass

    def add(self, element: Any) -> None:
        """Add a job to the progress tracker.

        Args:
            element: Job ID (string), job dict, or Job object.
        """
        if isinstance(element, str):
            element = Job(id=element)
        elif isinstance(element, dict):
            element = Job(**element)

        if not isinstance(element, Job):
            raise TypeError("Only Job objects can be added to JobsProgress.")

        set.add(self, element)

    def remove(self, element: Any) -> None:
        """Remove a job from the progress tracker.

        Args:
            element: Job ID (string), job dict, or Job object.
        """
        if isinstance(element, str):
            element = Job(id=element)
        elif isinstance(element, dict):
            element = Job(**element)

        if not isinstance(element, Job):
            raise TypeError("Only Job objects can be removed from JobsProgress.")

        set.discard(self, element)

    def contains(self, job_id: str) -> bool:
        """Check if a job ID is in progress.

        Args:
            job_id: The job ID to check.

        Returns:
            True if the job is in progress.
        """
        return Job(id=job_id) in self

    def get_all(self) -> Set[str]:
        """Get all job IDs in progress.

        Returns:
            Set of job IDs currently in progress.
        """
        return {str(job) for job in self}

    def get_job_list(self) -> Optional[str]:
        """Get job IDs as comma-separated string.

        Returns:
            Comma-separated job IDs, or None if empty.
        """
        if not len(self):
            return None
        return ",".join(str(job) for job in self)

    def get_job_count(self) -> int:
        """Get the number of jobs in progress.

        Returns:
            Number of jobs.
        """
        return len(self)

    def clear(self) -> None:
        """Clear all jobs from progress tracker."""
        set.clear(self)


# Singleton instance accessor
def get_jobs_progress() -> JobsProgress:
    """Get the JobsProgress singleton instance.

    Returns:
        The JobsProgress singleton.
    """
    return JobsProgress()
