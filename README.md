# ais-kadaster-mcp

Small Python repo containing `kadaster.py`, which crawls the Kadaster Labs query catalog, fetches query details, optionally executes the SPARQL, and writes JSON examples to disk.

## Prereqs

- Python (recommended: `3.11`, see `.python-version`)
- `uv` installed: https://docs.astral.sh/uv/

## Setup

Create a local virtualenv and install dependencies:

```bash
uv sync
```

## Run

Run the script directly:

```bash
uv run python kadaster.py
```

Or run via the installed console script:

```bash
uv run kadaster-extract
```

Output is written to `kadaster_dataset3/` (gitignored).

## Configuration

Defaults live at the top of `kadaster.py` (e.g. `OUTPUT_DIR`, `DELAY_BETWEEN_REQUESTS`, `GLOBAL_EXECUTION_ENDPOINT`).

## Dev (optional)

Lint:

```bash
uv run ruff check .
```
