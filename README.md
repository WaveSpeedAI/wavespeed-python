<div align="center">
  <a href="https://wavespeed.ai" target="_blank" rel="noopener noreferrer">
    <img src="https://raw.githubusercontent.com/WaveSpeedAI/waverless/main/docs/images/wavespeed-dark-logo.png" alt="WaveSpeedAI logo" width="200"/>
  </a>

  <h1>WaveSpeedAI Python SDK</h1>

  <p>
    <strong>Official Python SDK for the WaveSpeedAI inference platform</strong>
  </p>

  <p>
    <a href="https://wavespeed.ai" target="_blank" rel="noopener noreferrer">🌐 Visit wavespeed.ai</a> •
    <a href="https://wavespeed.ai/docs">📖 Documentation</a> •
    <a href="https://github.com/WaveSpeedAI/wavespeed-python/issues">💬 Issues</a>
  </p>
</div>

---

## Installation

```bash
pip install wavespeed
```

## API Client

Run WaveSpeed AI models with a simple API:

```python
import wavespeed

output = wavespeed.run(
    "wavespeed-ai/z-image/turbo",
    {"prompt": "Cat"},
)

print(output["outputs"][0])  # Output URL
```

### Authentication

Set your API key via environment variable (You can get your API key from [https://wavespeed.ai/accesskey](https://wavespeed.ai/accesskey)):

```bash
export WAVESPEED_API_KEY="your-api-key"
```

Or pass it directly:

```python
from wavespeed import Client

client = Client(api_key="your-api-key")
output = client.run("wavespeed-ai/z-image/turbo", {"prompt": "Cat"})
```

### Options

```python
output = wavespeed.run(
    "wavespeed-ai/z-image/turbo",
    {"prompt": "Cat"},
    timeout=36000.0,       # Max wait time in seconds (default: 36000.0)
    poll_interval=1.0,     # Status check interval (default: 1.0)
    enable_sync_mode=False, # Single request mode, no polling (default: False)
)
```

### Sync Mode

Use `enable_sync_mode=True` for a single request that waits for the result (no polling).

> **Note:** Not all models support sync mode. Check the model documentation for availability.

```python
output = wavespeed.run(
    "wavespeed-ai/z-image/turbo",
    {"prompt": "Cat"},
    enable_sync_mode=True,
)
```

### Retry Configuration

Configure retries at the client level:

```python
from wavespeed import Client

client = Client(
    api_key="your-api-key",
    max_retries=0,            # Task-level retries (default: 0)
    max_connection_retries=5, # HTTP connection retries (default: 5)
    retry_interval=1.0,       # Base delay between retries in seconds (default: 1.0)
)
```

### Upload Files

Upload images, videos, or audio files:

```python
import wavespeed

url = wavespeed.upload("/path/to/image.png")
print(url)
```

## Serverless Worker

Build serverless workers for the WaveSpeed platform.

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

### Concurrent Execution

Enable concurrent job processing with `concurrency_modifier`:

```python
import wavespeed.serverless as serverless

def handler(job):
    return {"output": job["input"]["data"]}

def concurrency_modifier(current_concurrency):
    return 2  # Process 2 jobs concurrently

serverless.start({
    "handler": handler,
    "concurrency_modifier": concurrency_modifier
})
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

### Running Tests

```bash
# Run all tests
python -m pytest

# Run a single test file
python -m pytest tests/test_api.py

# Run a specific test
python -m pytest tests/test_api.py::TestClient::test_run_success -v
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

### API Client

| Variable | Description |
|----------|-------------|
| `WAVESPEED_API_KEY` | WaveSpeed API key |

### Serverless Worker

| Variable | Description |
|----------|-------------|
| `WAVERLESS_POD_ID` | Worker/pod identifier |
| `WAVERLESS_API_KEY` | API authentication key |
| `WAVERLESS_WEBHOOK_GET_JOB` | Job fetch endpoint |
| `WAVERLESS_WEBHOOK_POST_OUTPUT` | Result submission endpoint |

## License

MIT
