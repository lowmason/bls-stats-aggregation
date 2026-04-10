# User guide

## Install

```bash
pip install -e .
```

Requires Python 3.11+ with `polars` and `httpx`.

## QCEW pipeline

Downloads yearly ~280 MB singlefile ZIPs from `data.bls.gov/cew/data/files/` (available from 2003 onward), filters to national + state rows, and saves a compact parquet. Then maps to the full CES industry hierarchy including 3-digit manufacturing split into durable/nondurable.

```python
from bls_stats import download_qcew_bulk
from bls_stats.qcew import map_bulk_to_ces

# Download and filter (writes parquet)
path = download_qcew_bulk(start_year=2020, end_year=2024)

# Map to CES industry groups
ces = map_bulk_to_ces(path)
print(ces)
```

Output columns: `industry_type`, `industry_code`, `geographic_type`, `geographic_code`, `ref_date`, `employment`.

Set `BLS_API_KEY` in your environment for higher BLS rate limits.

## JOLTS pipeline

Downloads the `jt.data.1.AllItems` tab-separated flat file (~33 MB) from `download.bls.gov`, filters to seasonally adjusted national estimates for private industries, and maps to the CES hierarchy at the domain and supersector levels.

```python
from bls_stats import download_jolts
from bls_stats.jolts import map_jolts_to_ces

# Download and filter (writes parquet)
path = download_jolts()

# Map to CES industry groups
jolts = map_jolts_to_ces(path)
print(jolts)
```

Output columns: `industry_type`, `industry_code`, `rate_or_level`, `data_element`, `ref_date`, `value`.

JOLTS data includes two data elements: **hires** and **total separations**, each reported as both a level (thousands) and a rate (percent). The mapping covers Total Private (domain `05`) and 10 supersectors. Domains `06` (Goods-Producing) and `08` (Private Service-Providing) are derived by aggregating supersector estimates, with rates computed as employment-weighted averages.

## Geography

The `bls_stats.geography` module provides state FIPS codes and Census region/division mappings used by the QCEW pipeline to aggregate state-level data.

```python
from bls_stats import (
    CENSUS_REGIONS,           # {'Northeast': ['09', '23', ...], ...}
    CENSUS_DIVISIONS,         # {'New England': ['09', '23', ...], ...}
    DIVISION_TO_REGION,       # {'New England': 'Northeast', ...}
    STATE_FIPS_TO_DIVISION,   # {'09': 'New England', ...}
    STATE_FIPS_TO_REGION,     # {'09': 'Northeast', ...}
)
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

- **By industry**: NAICS 2-digit codes from the bulk singlefiles are mapped to CES sector codes, then aggregated up through supersectors to domains.
- **By ownership**: Government employment uses `own_code` 1/2/3 (Federal/State/Local) on total industry rows, mapped to CES sectors 91/92/93.
- **By state**: Area FIPS codes are mapped to `geographic_type` (national/state) and `geographic_code` (2-digit state FIPS).
- **Manufacturing split**: 3-digit NAICS subsectors are split into durable goods (CES 31) and nondurable goods (CES 32) via `NAICS3_TO_MFG_SECTOR`.

### Key constants

```python
from bls_stats.qcew import (
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
