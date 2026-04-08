# qcew-stats

[![Coverage](https://codecov.io/gh/lowmason/qcew-stats/graph/badge.svg)](https://codecov.io/gh/lowmason/qcew-stats)

Download QCEW (Quarterly Census of Employment and Wages) data from BLS and map it to CES (Current Employment Statistics) industry groups.

## Install

```bash
pip install -e .
```

Requires Python 3.11+ with `polars` and `httpx`.

## Two pipelines

### CSV API pipeline (2014+)

Downloads per-quarter, per-industry CSVs from the BLS QCEW CSV API at `data.bls.gov/cew/data/api/`. Good for recent data, supports filtering by geography and ownership.

```python
from qcew_stats import fetch_qcew_with_geography, map_qcew_to_ces

# Download 2023-2024, private + government, national + all states
raw = fetch_qcew_with_geography(
    years=[2023, 2024],
    ownership_codes=['5', '1', '2', '3'],
    include_national=True,
    include_states=True,
)

# Map to CES industry groups (sectors → supersectors → domains)
ces = map_qcew_to_ces(raw, include_government=True)
print(ces)
```

### Bulk pipeline (2003+)

Downloads yearly ~280 MB singlefile ZIPs from `data.bls.gov/cew/data/files/`, filters to national + state rows, and saves a compact parquet. Includes 3-digit manufacturing split into durable/nondurable.

```python
from qcew_stats import download_qcew_bulk, map_bulk_to_ces

# Download and filter (writes parquet)
path = download_qcew_bulk(start_year=2020, end_year=2024)

# Map to CES industry groups
ces = map_bulk_to_ces(path)
print(ces)
```

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

- **By industry**: QCEW API codes (e.g. `1012` → NAICS 21 → CES sector `21`) and raw NAICS codes are mapped to CES 2-digit sector codes, then aggregated up through supersectors to domains.
- **By ownership**: Government employment uses `own_code` 1/2/3 (Federal/State/Local) on total industry rows, mapped to CES sectors 91/92/93.
- **By state**: Area FIPS codes are mapped to `geographic_type` (national/state) and `geographic_code` (2-digit state FIPS).
- **Manufacturing split**: 3-digit NAICS subsectors are split into durable goods (CES 31) and nondurable goods (CES 32) via `NAICS3_TO_MFG_SECTOR`.

### Key constants

```python
from qcew_stats import (
    INDUSTRY_HIERARCHY,       # Polars LazyFrame: sector → supersector → domain
    INDUSTRY_MAP,             # List[IndustryEntry]: all 35 CES industry codes
    GOVT_OWNERSHIP_TO_SECTOR, # {'1': '91', '2': '92', '3': '93'}
    NAICS3_TO_MFG_SECTOR,     # {'311': '32', '321': '31', ...}
    CES_SECTOR_TO_NAICS,      # {'41': '42', '42': '44', '43': '48', ...}
    qcew_to_sector,           # Returns dict: QCEW API code → NAICS sector
)
```

## Caching

The CSV API client caches responses locally (default `.cache/bls/`, 24-hour TTL). Set `BLS_API_KEY` in your environment for higher rate limits on the JSON API.
