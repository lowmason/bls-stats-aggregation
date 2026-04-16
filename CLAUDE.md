# CLAUDE.md

## Project overview

bls-stats-aggregation maps Bureau of Labor Statistics program data to the CES (Current Employment Statistics) industry hierarchy: sectors → supersectors → domains. Data is sourced from Trino data lakes (connectivity implemented in the data lake environment). Output is a long-format Polars DataFrame with employment levels keyed by geography and industry.

## Repository layout

```
src/bls_stats_aggregation/    # Installable package (hatchling, src-layout)
  __init__.py                   # Top-level re-exports (geography constants)
  cli.py                        # Typer CLI entry point
  geography.py                  # State FIPS codes and name mappings
  ces/                          # CES industry standard (canonical for all programs)
    __init__.py                   # CES public API re-exports
    industry.py                   # CES hierarchy, crosswalk constants, IndustryEntry
  qcew/                         # QCEW → CES subpackage
    __init__.py                   # QCEW public API re-exports
    mapping.py                    # QCEW → CES industry mapping pipeline
    industry.py                   # Re-export shim → ces/industry.py
  jolts/                        # JOLTS → CES subpackage
    __init__.py                   # JOLTS public API re-exports
    mapping.py                    # JOLTS → CES industry mapping pipeline
    industry.py                   # JOLTS-to-CES crosswalk and series ID parsing
  sae/                          # SAE → CES subpackage (placeholder)
    __init__.py
    mapping.py
  bed/                          # BED → CES subpackage (placeholder)
    __init__.py
    mapping.py
  data_source/                  # Data lake connectors
    __init__.py
    trino.py                      # Trino placeholder (implemented in data lake env)
tests/                          # pytest test suite
docs/                           # mkdocs-material + mkdocstrings API docs
data/                           # Output parquet files (gitignored)
```

## Tech stack

- **Python 3.11+** with `from __future__ import annotations` throughout
- **Polars** (not pandas) for all DataFrames — use `pl.DataFrame`, `pl.LazyFrame`, `pl.col()` idioms
- **Typer** for CLI
- **pytest** for tests, **ruff** for linting/formatting
- **hatchling** build backend, installed via `pip install -e .`
- **mkdocs-material** + **mkdocstrings** for docs

## Key conventions

- All source files use `from __future__ import annotations` for modern type hints.
- Module docstrings use Google-style docstrings (configured in mkdocs).
- Industry codes are always strings, never ints (e.g. `'21'`, `'00'`, `'44-45'`).
- `ces/` is the canonical industry standard — all other programs import from it.
- Employment data flows through four streams in `qcew/mapping.py`: total, private 2-digit sectors, government by ownership, and 3-digit manufacturing split.
- The CES hierarchy has three levels: **domain** (00, 05, 06, 07, 08), **supersector** (10–90), **sector** (21, 22, 31, 32, etc.). Government sectors 91/92/93 derive from QCEW ownership codes, not industry codes.
- Manufacturing gets a special durable/nondurable split via `NAICS3_TO_MFG_SECTOR` at the 3-digit NAICS level.

## Common commands

```bash
pip install -e ".[dev,docs]"      # Install with all extras
pytest                             # Run tests
ruff check src/ tests/             # Lint
ruff format src/ tests/            # Format
bls-stats --help                   # CLI help
bls-stats ces                      # Display CES hierarchy
mkdocs serve                       # Local docs preview
mkdocs build                       # Build docs to site/
```

## Testing

Tests live in `tests/` with one file per module (`test_mapping.py`, `test_jolts.py`, etc.). Shared fixtures go in `conftest.py`. Tests should be fast and not hit the network.
