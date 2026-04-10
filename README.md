# bls-stats

[![Coverage](https://codecov.io/gh/lowmason/bls-stats/graph/badge.svg)](https://codecov.io/gh/lowmason/bls-stats)

Download QCEW (Quarterly Census of Employment and Wages) data from BLS and map it to CES (Current Employment Statistics) industry groups.

## Install

```bash
pip install -e .
```

Requires Python 3.11+ with `polars` and `httpx`.

## Usage

Downloads yearly ~280 MB singlefile ZIPs from `data.bls.gov/cew/data/files/` (available from 2003 onward), filters to national + state rows, and saves a compact parquet. Then maps to the full CES industry hierarchy including 3-digit manufacturing split into durable/nondurable.

```python
from bls_stats import download_qcew_bulk, map_bulk_to_ces

# Download and filter (writes parquet)
path = download_qcew_bulk(start_year=2020, end_year=2024)

# Map to CES industry groups
ces = map_bulk_to_ces(path)
print(ces)
```

Set `BLS_API_KEY` in your environment for higher BLS rate limits.

## Industry hierarchy

The CES industry structure has three levels:

| Level | Example code | Example name |
|-------|-------------|--------------|
| **Domain** | `00` | Total Non-Farm |
| **Domain** | `05` | Total Private |
| **Supersector** | `30` | Manufacturing |
| **Supersector** | `40` | Trade, Transportation, and Utilities |
| **Sector** | `21` | Mining |
| **Sector** | `44` | Retail Trade |
| **Sector** | `91` | Federal Government |

### Mapping from QCEW

- **By industry**: NAICS 2-digit codes from the bulk singlefiles are mapped to CES sector codes, then aggregated up through supersectors to domains.
- **By ownership**: Government employment uses `own_code` 1/2/3 (Federal/State/Local) on total industry rows, mapped to CES sectors 91/92/93.
- **By state**: Area FIPS codes are mapped to `geographic_type` (national/state) and `geographic_code` (2-digit state FIPS).
- **Manufacturing split**: 3-digit NAICS subsectors are split into durable goods (CES 31) and nondurable goods (CES 32) via `NAICS3_TO_MFG_SECTOR`.

### Key constants

```python
from bls_stats import (
    INDUSTRY_HIERARCHY,           # Polars LazyFrame: sector → supersector → domain
    INDUSTRY_MAP,                 # List[IndustryEntry]: all CES industry codes
    DOMAIN_DEFINITIONS,           # Domain code → name, includes_govt, goods_only
    GOVT_OWNERSHIP_TO_SECTOR,     # {'1': '91', '2': '92', '3': '93'}
    NAICS3_TO_MFG_SECTOR,         # {'311': '32', '321': '31', ...}
    CES_SECTOR_TO_NAICS,          # {'41': '42', '42': '44', '43': '48', ...}
    SINGLE_SECTOR_SUPERSECTORS,   # {'20': '23', '50': '51', '80': '81'}
    get_sector_codes,             # → sorted list of 2-digit sector codes
    get_supersector_codes,        # → sorted list of supersector codes
    get_supersector_components,   # → dict: supersector → list of sector codes
    get_domain_supersectors,      # → list of supersectors composing a domain
)
```
