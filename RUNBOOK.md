# Runbook

Procedures for MetricForge. `README.md` describes what each command does; this file says when
to run it and in what order.

## On open

```bash
git status --short
git log -3 --oneline
```

## On close

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -q
```

## Run

Generate the sample data once (README §Quick start), then `uv run mf validate --dir metrics/`
and any `uv run mf query` from the README. `--sql` shows what a query compiles to; use it
before trusting a new metric definition.

## Change the compiler

1. Add the case to `tests/` first; the compiler tests assert on the generated SQL as well as
   the result rows.
2. `uv run pytest`, then `uv run mf show-sql` on the sample metrics to read the SQL by eye.
3. A change in output shape is a release for the consumers: follow §Release.

## Release

The consumers pin this repo by tag, so a release is not done until their pins move.

1. Bump `version` in `pyproject.toml` and `__version__` in `src/metricforge/__init__.py`,
   then `uv lock` (CI installs with `--locked`).
2. Commit `chore: Release vX.Y.Z`, tag `git tag -a vX.Y.Z`, push `main` and the tag.
3. `gh release create vX.Y.Z` with handwritten notes; mark it latest.
4. In `revenue-monitor` and `metric-helpdesk`, bump the `metricforge` tag in `pyproject.toml`,
   `uv lock`, run their tests, commit. Their RUNBOOKs have the steps.

## Restore on a new machine

1. Clone the repo, `uv sync`.
2. Generate the sample data (README §Quick start), then `uv run pytest`. No keys.

## Validate

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src
uv run pytest
uv run mf validate --dir metrics/
```
