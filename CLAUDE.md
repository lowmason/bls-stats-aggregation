# CLAUDE.md

## Project overview

bls-stats is a toolkit for downloading and processing Bureau of Labor Statistics data. The `bls_stats.qcew` subpackage downloads QCEW (Quarterly Census of Employment and Wages) bulk data and maps it to the CES (Current Employment Statistics) industry hierarchy: sectors → supersectors → domains. Output is a long-format Polars DataFrame with monthly employment levels keyed by geography and industry.

## Repository layout

```
src/bls_stats/            # Installable package (hatchling, src-layout)
  __init__.py               # Top-level re-exports (shared modules)
  download.py               # Bulk ZIP download + CSV filtering (shared)
  geography.py              # State FIPS codes and name mappings (shared)
  http_client.py            # httpx client with BLS-friendly headers + retry (shared)
  qcew/                     # QCEW → CES subpackage
    __init__.py               # QCEW public API re-exports
    mapping.py                # QCEW → CES industry mapping pipeline
    industry.py               # CES hierarchy, crosswalk constants, IndustryEntry
tests/                      # pytest test suite
docs/                       # mkdocs-material + mkdocstrings API docs
data/                       # Output parquet files (gitignored)
scratch.ipynb               # Exploratory notebook
```

## Tech stack

- **Python 3.11+** with `from __future__ import annotations` throughout
- **Polars** (not pandas) for all DataFrames — use `pl.DataFrame`, `pl.LazyFrame`, `pl.col()` idioms
- **httpx** (HTTP/2) for BLS downloads
- **pytest** for tests, **ruff** for linting/formatting
- **hatchling** build backend, installed via `pip install -e .`
- **mkdocs-material** + **mkdocstrings** for docs

## Key conventions

- All source files use `from __future__ import annotations` for modern type hints.
- Module docstrings use Google-style docstrings (configured in mkdocs).
- Industry codes are always strings, never ints (e.g. `'21'`, `'00'`, `'44-45'`).
- Employment data flows through four streams in `qcew/mapping.py`: total, private 2-digit sectors, government by ownership, and 3-digit manufacturing split.
- The CES hierarchy has three levels: **domain** (00, 05, 06, 07, 08), **supersector** (10–90), **sector** (21, 22, 31, 32, etc.). Government sectors 91/92/93 derive from QCEW ownership codes, not industry codes.
- Manufacturing gets a special durable/nondurable split via `NAICS3_TO_MFG_SECTOR` at the 3-digit NAICS level.

## Common commands

```bash
pip install -e ".[dev,docs]"      # Install with all extras
pytest                             # Run tests
ruff check src/ tests/             # Lint
ruff format src/ tests/            # Format
mkdocs serve                       # Local docs preview
mkdocs build                       # Build docs to site/
```

## Environment variables

- `BLS_API_KEY` — optional; appended as `registrationkey` param for higher BLS rate limits.

## Testing

Tests live in `tests/` with one file per module (`test_download.py`, `test_mapping.py`, etc.). Shared fixtures go in `conftest.py`. Tests should be fast and not hit the network — mock `httpx` calls when testing download/HTTP logic.
