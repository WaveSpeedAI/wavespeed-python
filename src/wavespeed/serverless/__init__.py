"""WaveSpeedAI Serverless SDK.

This module provides a serverless worker implementation compatible with
RunPod serverless infrastructure (Waverless).

Example usage:
    import wavespeed.serverless as serverless

    def handler(job):
        return {"output": job["input"]["data"]}

    serverless.start({"handler": handler})
"""

import argparse
import signal
import sys
from typing import Any, Dict

from wavespeed.config import _detect_serverless_env

from .modules.handler import is_generator
from .modules.local import run_local
from .modules.logger import log
from .worker import run_worker

__all__ = ["start", "log"]


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the serverless worker."""
    parser = argparse.ArgumentParser(description="WaveSpeedAI Serverless Worker")

    parser.add_argument(
        "--waverless_log_level",
        "--rp_log_level",
        type=str,
        default=None,
        help="Log level (NOTSET, TRACE, DEBUG, INFO, WARN, ERROR)",
    )
    parser.add_argument(
        "--waverless_debugger",
        "--rp_debugger",
        action="store_true",
        default=False,
        help="Enable debugger/profiler",
    )
    parser.add_argument(
        "--waverless_serve_api",
        "--rp_serve_api",
        action="store_true",
        default=False,
        help="Serve API server for local development",
    )
    parser.add_argument(
        "--waverless_api_host",
        "--rp_api_host",
        type=str,
        default="localhost",
        help="API server host (default: localhost)",
    )
    parser.add_argument(
        "--waverless_api_port",
        "--rp_api_port",
        type=int,
        default=8000,
        help="API server port (default: 8000)",
    )
    parser.add_argument(
        "--test_input",
        type=str,
        default=None,
        help="JSON test input for local testing",
    )

    return parser.parse_args()


def _setup_signal_handlers(worker_config: Dict[str, Any]) -> None:
    """Set up signal handlers for graceful shutdown."""

    def signal_handler(sig: int, frame: Any) -> None:
        log.info("Received shutdown signal, stopping worker...")
        worker_config["_shutdown"] = True
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def start(config: Dict[str, Any]) -> None:
    """Start the serverless worker.

    This is the main entry point for the serverless worker. It handles:
    - Parsing command-line arguments
    - Detecting serverless environment (RunPod/Waverless) and loading config
    - Setting up signal handlers
    - Running in local test mode or worker mode

    Args:
        config: Configuration dictionary containing:
            - handler: The handler function to process jobs
            - return_aggregate_stream (optional): Whether to aggregate
              streaming results
            - concurrency_modifier (optional): Function to adjust concurrency

    Example:
        def handler(job):
            return {"output": "processed"}

        serverless.start({"handler": handler})
    """
    if "handler" not in config:
        raise ValueError("config must contain a 'handler' function")

    # Parse CLI arguments
    args = _parse_args()

    # Set log level from CLI or environment
    if args.waverless_log_level:
        log.set_level(args.waverless_log_level)

    # Config is auto-loaded at import time in wavespeed.config
    # Just detect environment for logging and storing in config
    serverless_env = _detect_serverless_env()
    if serverless_env == "runpod":
        log.debug("Detected RunPod environment")
    elif serverless_env == "waverless":
        log.debug("Detected native Waverless environment")

    # Store parsed args in config
    config["_args"] = args
    config["_shutdown"] = False
    config["_serverless_env"] = serverless_env

    # Setup signal handlers
    _setup_signal_handlers(config)

    # Check handler type
    handler = config["handler"]
    if is_generator(handler):
        log.debug("Handler is a generator function")
        config["_is_generator"] = True
    else:
        config["_is_generator"] = False

    # Determine run mode
    if args.test_input is not None:
        # Local test mode with CLI input
        log.info("Running in local test mode (CLI input)")
        run_local(config)
    elif args.waverless_serve_api:
        # API server mode
        log.info("Running in API server mode")
        from .modules.fastapi import WorkerAPI

        api = WorkerAPI(config)
        api.start(
            host=args.waverless_api_host,
            port=args.waverless_api_port,
        )
    else:
        # Standard worker mode
        log.info("Starting serverless worker...")
        run_worker(config)
