# Versioning with setuptools_scm

This project uses [setuptools_scm](https://github.com/pypa/setuptools_scm) for automatic version management based on Git tags.

## How it works

- Version numbers are automatically derived from Git tags
- No manual version management needed in code
- Development versions include commit information
- The version is stored in `src/wavespeed/_version.py` (auto-generated)

## Version Format

- **Tagged release**: `0.1.0` (based on git tag `v0.1.0`)
- **Development**: `0.1.1.dev1+g50ebb7c` (1 commit after `v0.1.0` tag)

## Creating a Release

To create a new release:

1. Commit all your changes
2. Create and push a version tag:
   ```bash
   git tag -a v0.2.0 -m "Release version 0.2.0"
   git push origin v0.2.0
   ```

## Building the Package

```bash
# Install build dependencies
pip install build setuptools_scm

# Build the package
python -m build

# The version will be automatically determined from Git
```

## Checking the Current Version

```bash
# From command line (requires setuptools_scm installed)
python -m setuptools_scm

# From Python code
import wavespeed
print(wavespeed.__version__)
```

## Configuration

The setuptools_scm configuration is in `pyproject.toml`:

```toml
[tool.setuptools_scm]
version_file = "src/wavespeed/_version.py"
```

This automatically generates `src/wavespeed/_version.py` during the build process.
