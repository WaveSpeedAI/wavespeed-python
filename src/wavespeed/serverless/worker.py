"""Worker loop orchestration for the serverless worker."""

from typing import Any, Dict

from wavespeed.config import serverless

from .modules.heartbeat import Heartbeat
from .modules.local import run_local
from .modules.logger import log
from .modules.scaler import JobScaler


def _is_local(config: Dict[str, Any]) -> bool:
    """Determine if the worker should run in local mode.

    Local mode is used when:
    - No job endpoint is configured (not deployed)
    - Test input is provided via CLI

    Args:
        config: The worker configuration.

    Returns:
        True if the worker should run in local mode.
    """
    # Check if test input was provided
    args = config.get("_args")
    if args and args.test_input:
        return True

    # Check if job endpoint is configured
    if not serverless.webhook_get_job:
        return True

    return False


def run_worker(config: Dict[str, Any]) -> None:
    """Run the serverless worker.

    This function orchestrates the worker lifecycle:
    1. Determines if running in local or deployed mode
    2. Starts the heartbeat process (in deployed mode)
    3. Starts the job scaler to process jobs

    Args:
        config: The worker configuration containing the handler.
    """
    # Check if we should run locally
    if _is_local(config):
        log.info("No job endpoint configured, running in local mode")
        run_local(config)
        return

    log.info(f"Worker ID: {serverless.pod_id or 'unknown'}")
    log.info(f"Job endpoint: {serverless.webhook_get_job or 'not set'}")

    # Start heartbeat process
    heartbeat = Heartbeat()
    heartbeat.start()

    try:
        # Start the job scaler
        scaler = JobScaler(config)
        scaler.start()
    except KeyboardInterrupt:
        log.info("Worker interrupted by user")
    except Exception as e:
        log.error(f"Worker error: {e}")
        raise
    finally:
        # Stop heartbeat
        heartbeat.stop()
        log.info("Worker stopped")
