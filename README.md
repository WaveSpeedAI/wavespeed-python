<div align="center">
  <a href="https://wavespeed.ai" target="_blank" rel="noopener noreferrer">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="assets/wavespeed-logo-dark.svg">
      <img src="assets/wavespeed-logo-light.svg" alt="WaveSpeed" width="342" height="48"/>
    </picture>
  </a>

  <h1>WaveSpeed Python SDK</h1>

  <p>
    <strong>Official Python SDK for the WaveSpeed inference platform</strong>
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
    enable_sync_mode=False, # Best-effort sync result attempt (default: False)
)
```

### Sync Mode

Use `enable_sync_mode=True` to ask the API to wait for the result in the initial
request. If the server-side sync wait times out, the SDK raises an error with
the task ID/result URL; the task continues processing and can be queried later.

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
    max_retries=0,            # Replacement task attempts (default: 0)
    max_connection_retries=5, # Result-query GET retries; POST is never retried
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

## Local Development

### Running Tests

```bash
# Run all tests
python -m pytest

# Run a single test file
python -m pytest tests/test_api.py

# Run a specific test
python -m pytest tests/test_api.py::TestClient::test_run_success -v
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `WAVESPEED_API_KEY` | WaveSpeed API key |
| `WAVESPEED_CLIENT_NAME` | Channel-attribution name sent as the `X-Client-Name` header (overrides the `client_name` parameter; defaults to `wavespeed-python`) |

## License

MIT

---

**[WaveSpeed AI](https://wavespeed.ai/)** — hosted inference for image, video, audio and 3D models.
Try it in the browser: **[Image generator](https://wavespeed.ai/image-generator)** · **[Video generator](https://wavespeed.ai/video-generator)**
