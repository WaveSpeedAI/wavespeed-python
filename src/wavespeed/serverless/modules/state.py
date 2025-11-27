"""Global state management for the serverless worker."""

import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from wavespeed.config import get_serverless_env


# Global worker ID
_worker_id: Optional[str] = None

# Flag indicating local test mode
is_local_test: bool = False

# Reference time for benchmarking (when job count reaches zero)
ref_count_zero: float = 0.0


def get_worker_id() -> str:
    """Get the worker ID, generating one if necessary.

    Returns:
        The worker ID from environment or a generated UUID.
    """
    global _worker_id
    if _worker_id is None:
        _worker_id = get_serverless_env("WORKER_ID") or str(uuid.uuid4())
    return _worker_id


def set_worker_id(worker_id: str) -> None:
    """Set the worker ID explicitly.

    Args:
        worker_id: The worker ID to set.
    """
    global _worker_id
    _worker_id = worker_id


@dataclass
class Job:
    """Represents a serverless job.

    Attributes:
        id: Unique job identifier.
        input: The job input data.
        webhook: Optional webhook URL for job completion.
    """

    id: str
    input: Dict[str, Any]
    webhook: Optional[str] = None

    def __hash__(self) -> int:
        """Hash based on job ID."""
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        """Equality based on job ID."""
        if isinstance(other, Job):
            return self.id == other.id
        return False


class JobsProgress:
    """Singleton class to track jobs in progress.

    This class maintains a set of job IDs that are currently being processed.
    """

    _instance: Optional["JobsProgress"] = None
    _jobs: Set[str]

    def __new__(cls) -> "JobsProgress":
        """Create or return the singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._jobs = set()
        return cls._instance

    def add(self, job_id: str) -> None:
        """Add a job ID to the progress tracker.

        Args:
            job_id: The job ID to add.
        """
        self._jobs.add(job_id)

    def remove(self, job_id: str) -> None:
        """Remove a job ID from the progress tracker.

        Args:
            job_id: The job ID to remove.
        """
        self._jobs.discard(job_id)

    def contains(self, job_id: str) -> bool:
        """Check if a job ID is in progress.

        Args:
            job_id: The job ID to check.

        Returns:
            True if the job is in progress.
        """
        return job_id in self._jobs

    def get_all(self) -> Set[str]:
        """Get all job IDs in progress.

        Returns:
            Set of job IDs currently in progress.
        """
        return self._jobs.copy()

    def clear(self) -> None:
        """Clear all jobs from progress tracker."""
        self._jobs.clear()

    def __len__(self) -> int:
        """Return the number of jobs in progress."""
        return len(self._jobs)

    def __contains__(self, job_id: str) -> bool:
        """Check if a job ID is in progress."""
        return job_id in self._jobs


# Singleton instance accessor
def get_jobs_progress() -> JobsProgress:
    """Get the JobsProgress singleton instance.

    Returns:
        The JobsProgress singleton.
    """
    return JobsProgress()
