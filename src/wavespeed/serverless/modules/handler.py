"""Handler utilities for the serverless worker."""

import asyncio
import inspect
from typing import Any, Callable


def is_generator(handler: Callable[..., Any]) -> bool:
    """Check if a handler is a generator function.

    This detects both synchronous generators and async generators.

    Args:
        handler: The handler function to check.

    Returns:
        True if the handler is a generator function.
    """
    return inspect.isgeneratorfunction(handler) or inspect.isasyncgenfunction(handler)


def is_async(handler: Callable[..., Any]) -> bool:
    """Check if a handler is an async function.

    Args:
        handler: The handler function to check.

    Returns:
        True if the handler is async (coroutine or async generator).
    """
    return asyncio.iscoroutinefunction(handler) or inspect.isasyncgenfunction(handler)


def is_async_generator(handler: Callable[..., Any]) -> bool:
    """Check if a handler is an async generator function.

    Args:
        handler: The handler function to check.

    Returns:
        True if the handler is an async generator.
    """
    return inspect.isasyncgenfunction(handler)


def is_sync_generator(handler: Callable[..., Any]) -> bool:
    """Check if a handler is a synchronous generator function.

    Args:
        handler: The handler function to check.

    Returns:
        True if the handler is a synchronous generator.
    """
    return inspect.isgeneratorfunction(handler)


def get_handler_type(handler: Callable[..., Any]) -> str:
    """Get a string description of the handler type.

    Args:
        handler: The handler function to check.

    Returns:
        String describing the handler type.
    """
    if is_async_generator(handler):
        return "async_generator"
    elif is_sync_generator(handler):
        return "sync_generator"
    elif asyncio.iscoroutinefunction(handler):
        return "async"
    else:
        return "sync"
