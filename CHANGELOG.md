# Changelog

All notable changes to this project are documented here.

Versions are derived from Git tags via `setuptools_scm` — see [VERSIONING.md](VERSIONING.md).

## 2.0.0

### Removed — BREAKING

**The serverless worker implementation has been removed from the SDK.**

WaveSpeed no longer offers serverless as an external product, so shipping a serverless worker in
the public SDK was misleading: it advertised a capability customers cannot buy. Everything below is
gone as of 2.0.0 and will not be restored.

- `wavespeed.serverless` — the entire package, including `serverless.start()`, the job scaler, the
  handler-type dispatcher, the worker HTTP layer, the FastAPI local dev server, local test mode,
  worker/job state tracking, heartbeat and progress reporting, the S3 (`boto3`) upload helpers, and
  `wavespeed.serverless.utils.validate`.
- `wavespeed.config.serverless` — the serverless config namespace and its import-time environment
  auto-detection (`RUNPOD_*` and `WAVERLESS_*` variables are no longer read by this package).
- `images/` — the `test_worker` Docker image and its build/push scripts, which existed only to
  build and exercise the worker.

Anyone building workers against this package should pin `wavespeed<2` and plan to migrate.

### Fixed

- **Python 3.8 and 3.9 support is now real rather than aspirational.** `api/__init__.py` carried a
  module-level PEP 604 annotation (`Client | None`) with no `__future__` import, so the package
  could not be imported on 3.8 or 3.9 despite `requires-python = ">=3.8"`. CI now runs the suite on
  3.8 through 3.12 so the claim stays honest.
- `typing_extensions` is now a declared dependency. It was imported at module scope but only
  reached the environment transitively through `boto3`/`aiohttp` — removing those would have
  produced an `ImportError` on a clean install.

### Changed

- Dependencies trimmed to what the API client actually needs. `aiohttp`, `aiohttp[speedups]`,
  `aiohttp-retry`, and `boto3` are no longer installed; `typing_extensions` is now declared
  explicitly (it was already imported by `wavespeed._config_module`).
- README and CLAUDE.md rewritten to describe an API-client-only SDK.

### Unchanged

The API client surface is untouched: `wavespeed.run`, `wavespeed.run_no_throw`,
`wavespeed.get_result`, `wavespeed.upload`, `wavespeed.Client`, `wavespeed.config.api`, and the
channel-attribution request headers all behave exactly as they did in 1.0.x.

## 1.0.14 and earlier

See the Git history and GitHub releases.
