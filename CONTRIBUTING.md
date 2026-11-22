# Contributing

## Setup

Use Python 3.11+ with [uv](https://docs.astral.sh/uv/) and Node.js 22.18+ on Node 22,
24.11+ on Node 24, or 26+ with npm for source builds.

```sh
uv sync --all-extras
(cd js && npm ci)
pre-commit install   # optional: ruff and basic hooks on commit
```

## Checks

Run these before opening a pull request.

```sh
uv run ruff check
uv run ruff format --check
uv run mypy
uv run pytest
(cd js && npm run build && npm run typecheck && npm test && npm run lint:package && npm run test:compat)
```

To try a change in a browser, run the [example](examples/fastapi-assistant-ui/README.md) against the fake Hermes with `make install` followed by `make demo`.

Add a line to `CHANGELOG.md` under the unreleased version for user-visible changes. Keep tests few and meaningful.
