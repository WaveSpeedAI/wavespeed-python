# wavespeed-python

WaveSpeedAI Python Client — Official Python SDK for WaveSpeedAI inference platform. This library provides a clean, unified, and high-performance API and serverless integration layer for your applications.

## Installation

```bash
pip install wavespeed
```

## Serverless Worker

Build serverless workers compatible with RunPod infrastructure.

### Basic Handler

```python
import wavespeed.serverless as serverless

def handler(job):
    job_input = job["input"]
    result = job_input.get("prompt", "").upper()
    return {"output": result}

serverless.start({"handler": handler})
```

### Async Handler

```python
import wavespeed.serverless as serverless

async def handler(job):
    job_input = job["input"]
    result = await process_async(job_input)
    return {"output": result}

serverless.start({"handler": handler})
```

### Generator Handler (Streaming)

```python
import wavespeed.serverless as serverless

def handler(job):
    for i in range(10):
        yield {"progress": i, "partial": f"chunk-{i}"}

serverless.start({"handler": handler})
```

### Input Validation

```python
from wavespeed.serverless.utils import validate

INPUT_SCHEMA = {
    "prompt": {"type": str, "required": True},
    "max_tokens": {"type": int, "required": False, "default": 100},
    "temperature": {
        "type": float,
        "required": False,
        "default": 0.7,
        "constraints": lambda x: 0 <= x <= 2,
    },
}

def handler(job):
    result = validate(job["input"], INPUT_SCHEMA)
    if "errors" in result:
        return {"error": result["errors"]}

    validated = result["validated_input"]
    # process with validated input...
    return {"output": "done"}
```

## Local Development

### Test with JSON Input

```bash
# Using CLI argument
python handler.py --test_input '{"input": {"prompt": "hello"}}'

# Using test_input.json file (auto-detected)
echo '{"input": {"prompt": "hello"}}' > test_input.json
python handler.py
```

### FastAPI Development Server

```bash
python handler.py --waverless_serve_api --waverless_api_port 8000
```

Then use the interactive Swagger UI at `http://localhost:8000/` or make requests:

```bash
# Synchronous execution
curl -X POST http://localhost:8000/runsync \
  -H "Content-Type: application/json" \
  -d '{"input": {"prompt": "hello"}}'

# Async execution
curl -X POST http://localhost:8000/run \
  -H "Content-Type: application/json" \
  -d '{"input": {"prompt": "hello"}}'
```

## CLI Options

| Option | Description |
|--------|-------------|
| `--test_input JSON` | Run locally with JSON test input |
| `--waverless_serve_api` | Start FastAPI development server |
| `--waverless_api_host HOST` | API server host (default: localhost) |
| `--waverless_api_port PORT` | API server port (default: 8000) |
| `--waverless_log_level LEVEL` | Log level (DEBUG, INFO, WARN, ERROR) |

## Environment Variables

The SDK auto-detects RunPod or native Waverless environments:

| Variable | Description |
|----------|-------------|
| `RUNPOD_WEBHOOK_GET_JOB` | Job fetch endpoint |
| `RUNPOD_WEBHOOK_POST_OUTPUT` | Result submission endpoint |
| `RUNPOD_AI_API_KEY` | API authentication key |
| `RUNPOD_POD_ID` | Worker/pod identifier |

## License

MIT
