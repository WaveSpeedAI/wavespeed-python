"""Lightweight health check HTTP server for worker mode."""

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from .logger import log


class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks."""

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args) -> None:
        """Suppress default logging."""
        pass


class HealthServer:
    """Background health check server."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
        """Initialize the health server.

        Args:
            host: Host to bind to.
            port: Port to bind to.
        """
        self.host = host
        self.port = port
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the health server in a background thread."""
        try:
            self._server = HTTPServer((self.host, self.port), HealthHandler)
            self._thread = threading.Thread(
                target=self._server.serve_forever, daemon=True
            )
            self._thread.start()
            log.info(f"Health server started at http://{self.host}:{self.port}/health")
        except Exception as e:
            log.error(f"Failed to start health server: {e}")

    def stop(self) -> None:
        """Stop the health server."""
        if self._server:
            self._server.shutdown()
            self._server = None
            log.debug("Health server stopped")
