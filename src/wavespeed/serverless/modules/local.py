"""Local testing module for the serverless worker."""

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from .handler import is_async, is_async_generator, is_sync_generator
from .logger import log
from .state import Job


def _load_test_input(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Load test input from CLI argument or file.

    Args:
        config: The worker configuration.

    Returns:
        The test input dictionary, or None if not found.
    """
    args = config.get("_args")

    # Check CLI argument first
    if args and args.test_input:
        try:
            return json.loads(args.test_input)
        except json.JSONDecodeError as e:
            log.error(f"Invalid JSON in --test_input: {e}")
            return None

    # Check for test_input.json file
    test_file = Path("test_input.json")
    if test_file.exists():
        try:
            with open(test_file, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            log.error(f"Invalid JSON in test_input.json: {e}")
            return None
        except IOError as e:
            log.error(f"Error reading test_input.json: {e}")
            return None

    return None


def run_local(config: Dict[str, Any]) -> None:
    """Run the handler locally with test input.

    This function is used for local development and testing.
    It loads test input and runs the handler once.

    Args:
        config: The worker configuration containing the handler.
    """
    import asyncio

    # Set local test mode
    import wavespeed.serverless.modules.state as state

    state.is_local_test = True

    # Load test input
    test_input = _load_test_input(config)
    if test_input is None:
        log.warn("No test input provided. Use --test_input or create test_input.json")
        test_input = {"input": {}}

    # Ensure input has proper structure
    if "input" not in test_input:
        test_input = {"input": test_input}

    # Create a mock job
    job = Job(
        id="local-test-job",
        input=test_input.get("input", {}),
        webhook=None,
    )

    job_input = {"id": job.id, "input": job.input}
    handler = config["handler"]

    log.info("Running handler with test input...")
    log.debug(f"Input: {json.dumps(job_input, indent=2)}")

    try:
        if is_async_generator(handler):
            # Async generator
            async def run_async_gen() -> None:
                results = []
                async for partial in handler(job_input):
                    log.info(f"[STREAM] {partial}")
                    results.append(partial)
                log.info(f"[FINAL] Aggregated {len(results)} stream outputs")

            asyncio.run(run_async_gen())

        elif is_sync_generator(handler):
            # Sync generator
            results = []
            for partial in handler(job_input):
                log.info(f"[STREAM] {partial}")
                results.append(partial)
            log.info(f"[FINAL] Aggregated {len(results)} stream outputs")

        elif is_async(handler):
            # Async function
            result = asyncio.run(handler(job_input))
            _print_result(result)

        else:
            # Sync function
            result = handler(job_input)
            _print_result(result)

    except Exception as e:
        log.error(f"Handler error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    log.info("Local test completed successfully")


def _print_result(result: Any) -> None:
    """Print the handler result in a readable format.

    Args:
        result: The handler result to print.
    """
    if result is None:
        log.info("[RESULT] None")
    elif isinstance(result, dict):
        if "error" in result:
            log.error(f"[ERROR] {result['error']}")
        elif "output" in result:
            output = result["output"]
            if isinstance(output, (dict, list)):
                log.info(f"[OUTPUT]\n{json.dumps(output, indent=2)}")
            else:
                log.info(f"[OUTPUT] {output}")
        else:
            log.info(f"[RESULT]\n{json.dumps(result, indent=2)}")
    else:
        log.info(f"[RESULT] {result}")
