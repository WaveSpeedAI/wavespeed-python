# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WaveSpeed Python SDK - Official Python SDK for the WaveSpeedAI inference platform. It is an API
client only: it submits jobs to the hosted WaveSpeed API, polls for results, and uploads files.

> As of v2.0.0 the SDK no longer ships a serverless worker implementation. WaveSpeed does not offer
> serverless as an external product, so that code was removed rather than left in the public SDK.

## Commands

### Testing
```bash
# Run all tests
python -m pytest

# Run a single test file
python -m pytest tests/test_api.py

# Run a specific test
python -m pytest tests/test_api.py::TestClient::test_run_success -v
```

### Linting
```bash
pre-commit run --all-files
```

### Local Development
```bash
# Install in editable mode
pip install -e .
```

## Architecture

### API client (`src/wavespeed/api/`)

- `client.py` - `Client`, the only transport. Synchronous, built on `requests`. Handles job
  submission, result polling, terminal-status handling, file upload, and the channel-attribution
  headers (`X-Client-Name`, etc.).
- `__init__.py` - module-level convenience wrappers over a lazily created default `Client`:
  `run`, `run_no_throw`, `get_result`, `upload`.

`run` raises on failure; `run_no_throw` returns `{"status", "outputs", "task_id", "error"}` instead,
mirroring the JavaScript SDK. `get_result` recovers a task whose local wait timed out.

### Configuration (`src/wavespeed/config.py`)

A single `api` config class, installed as a config module by `_config_module.py` (the PyTorch-style
`install_config_module` shim). This gives `wavespeed.config.patch(...)` as a context manager /
decorator for tests. `Client` reads its defaults from `wavespeed.config.api`.

`WAVESPEED_API_KEY` seeds `config.api.api_key` at import time.
