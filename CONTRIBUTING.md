# Contributing

## Setup

Python 3.11+ with [uv](https://docs.astral.sh/uv/), Node 20+ with npm.

```sh
uv sync --all-extras
cd js && npm ci
pre-commit install   # optional: ruff and basic hooks on commit
```

## Checks

Run these before opening a pull request; CI runs the same.

```sh
uv run ruff check
uv run ruff format --check
uv run mypy
uv run pytest
cd js && npm run build && npm test && npm run typecheck --if-present
```

Add a line to `CHANGELOG.md` under Unreleased for user-visible changes. Keep tests few and meaningful.
