"""Logging module for the serverless worker."""

import json
import sys
from datetime import datetime, timezone
from enum import IntEnum
from typing import Optional

from wavespeed.config import serverless


class LogLevel(IntEnum):
    """Log level enumeration."""

    NOTSET = 0
    TRACE = 5
    DEBUG = 10
    INFO = 20
    WARN = 30
    ERROR = 40


# Map string names to log levels
_LEVEL_MAP = {
    "NOTSET": LogLevel.NOTSET,
    "TRACE": LogLevel.TRACE,
    "DEBUG": LogLevel.DEBUG,
    "INFO": LogLevel.INFO,
    "WARN": LogLevel.WARN,
    "WARNING": LogLevel.WARN,
    "ERROR": LogLevel.ERROR,
}


class WaverlessLogger:
    """Singleton logger for the serverless worker.

    Outputs logs in JSON format when running in a serverless environment,
    otherwise outputs in a human-readable format.
    """

    _instance: Optional["WaverlessLogger"] = None
    _level: LogLevel
    _endpoint_id: Optional[str]
    _max_message_length: int = 4096

    def __new__(cls) -> "WaverlessLogger":
        """Create or return the singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """Initialize the logger with config settings."""
        level_str = serverless.log_level if serverless.log_level else "INFO"
        self._level = _LEVEL_MAP.get(level_str.upper(), LogLevel.INFO)
        self._endpoint_id = serverless.endpoint_id if serverless.endpoint_id else None

    def set_level(self, level: str | LogLevel) -> None:
        """Set the log level.

        Args:
            level: Log level as string or LogLevel enum.
        """
        if isinstance(level, str):
            self._level = _LEVEL_MAP.get(level.upper(), LogLevel.INFO)
        else:
            self._level = level

    def _should_log(self, level: LogLevel) -> bool:
        """Check if a message at the given level should be logged.

        Args:
            level: The log level to check.

        Returns:
            True if the message should be logged.
        """
        return level >= self._level

    def _truncate(self, message: str) -> str:
        """Truncate message if it exceeds max length.

        Args:
            message: The message to truncate.

        Returns:
            The truncated message.
        """
        if len(message) > self._max_message_length:
            return message[: self._max_message_length - 3] + "..."
        return message

    def _format_message(
        self, level: LogLevel, message: str, job_id: Optional[str] = None
    ) -> str:
        """Format a log message.

        Args:
            level: The log level.
            message: The message to format.
            job_id: Optional job ID to include.

        Returns:
            The formatted message.
        """
        message = self._truncate(str(message))

        # Use JSON format when endpoint_id is set (deployed environment)
        if self._endpoint_id:
            log_data = {
                "requestId": job_id or "N/A",
                "message": message,
                "level": level.name,
                "timestamp": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
            }
            return json.dumps(log_data)
        else:
            # Human-readable format for local development
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            job_str = f" [{job_id}]" if job_id else ""
            return f"{timestamp} | {level.name:5}{job_str} | {message}"

    def _log(self, level: LogLevel, message: str, job_id: Optional[str] = None) -> None:
        """Log a message at the specified level.

        Args:
            level: The log level.
            message: The message to log.
            job_id: Optional job ID.
        """
        if self._should_log(level):
            formatted = self._format_message(level, message, job_id)
            if level >= LogLevel.ERROR:
                print(formatted, file=sys.stderr)
            else:
                print(formatted)
            sys.stdout.flush()

    def trace(self, message: str, job_id: Optional[str] = None) -> None:
        """Log a trace message.

        Args:
            message: The message to log.
            job_id: Optional job ID.
        """
        self._log(LogLevel.TRACE, message, job_id)

    def debug(self, message: str, job_id: Optional[str] = None) -> None:
        """Log a debug message.

        Args:
            message: The message to log.
            job_id: Optional job ID.
        """
        self._log(LogLevel.DEBUG, message, job_id)

    def info(self, message: str, job_id: Optional[str] = None) -> None:
        """Log an info message.

        Args:
            message: The message to log.
            job_id: Optional job ID.
        """
        self._log(LogLevel.INFO, message, job_id)

    def warn(self, message: str, job_id: Optional[str] = None) -> None:
        """Log a warning message.

        Args:
            message: The message to log.
            job_id: Optional job ID.
        """
        self._log(LogLevel.WARN, message, job_id)

    def error(self, message: str, job_id: Optional[str] = None) -> None:
        """Log an error message.

        Args:
            message: The message to log.
            job_id: Optional job ID.
        """
        self._log(LogLevel.ERROR, message, job_id)

    def log(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        job_id: Optional[str] = None,
    ) -> None:
        """Log a message at the specified level.

        Args:
            message: The message to log.
            level: The log level.
            job_id: Optional job ID.
        """
        self._log(level, message, job_id)


# Global logger instance
log = WaverlessLogger()
