# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WaveSpeed Python SDK - Official Python SDK for WaveSpeedAI inference platform. Provides a serverless worker implementation compatible with RunPod infrastructure.

## Commands

### Testing
```bash
# Run all tests
python -m pytest

# Run a single test file
python -m pytest tests/test_serverless/test_modules/test_job.py

# Run a specific test
python -m pytest tests/test_serverless/test_modules/test_job.py::TestRunJob::test_sync_handler_success -v
```

### Local Development
```bash
# Install in editable mode
pip install -e .

# Run handler locally with test input
python handler.py --test_input '{"input": {"message": "hello"}}'

# Run FastAPI development server
python handler.py --waverless_serve_api --waverless_api_port 8000
```

### Docker (test_worker image)
```bash
# Build and test
./images/test_worker/build_and_test.sh
```

## Architecture

### Serverless Worker (`src/wavespeed/serverless/`)

Entry point: `serverless.start({"handler": handler})` in `__init__.py`

Three execution modes:
1. **Local test mode** - `--test_input` flag or `test_input.json` file
2. **API server mode** - `--waverless_serve_api` for FastAPI dev server
3. **Worker mode** - Production mode polling for jobs

Key modules in `modules/`:
- `job.py` - Job execution (`run_job`, `run_job_generator`, `handle_job`)
- `scaler.py` - Concurrent job fetching and processing (`JobScaler`)
- `handler.py` - Handler type detection (sync/async/generator)
- `http.py` - HTTP communication (`fetch_jobs`, `send_result`, `stream_result`)
- `fastapi.py` - Local development API server (`WorkerAPI`)
- `local.py` - Local test execution
- `state.py` - Job and worker state management
- `validator.py` - Input validation (RunPod-compatible)

### Handler Types

Handlers can be sync/async functions or generators:
```python
def sync_handler(job): return {"output": result}
async def async_handler(job): return {"output": result}
def generator_handler(job): yield partial_result
async def async_gen_handler(job): yield partial_result
```

### Validation (RunPod-compatible)

```python
from wavespeed.serverless.utils import validate

schema = {
    "prompt": {"type": str, "required": True},
    "temperature": {"type": float, "required": False, "default": 0.7, "constraints": lambda x: 0 <= x <= 2},
}
result = validate(job_input, schema)  # Returns {"validated_input": ...} or {"errors": [...]}
```

### Environment Detection

Auto-detects RunPod (`RUNPOD_*` env vars) or native Waverless (`WAVERLESS_*` env vars) environments.
