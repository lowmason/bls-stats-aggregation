# bls-stats-aggregation

[![Coverage](https://codecov.io/gh/lowmason/bls-stats-aggregation/graph/badge.svg)](https://codecov.io/gh/lowmason/bls-stats-aggregation)

Map Bureau of Labor Statistics program data to the CES (Current Employment Statistics) industry hierarchy. Data for all programs is queried from a Trino data lake.

## Supported programs

| Program | Description |
|---------|-------------|
| **QCEW** | Quarterly Census of Employment and Wages — monthly employment levels by industry and geography |
| **JOLTS** | Job Openings and Labor Turnover Survey — hires and total separations rates and levels by industry |
| **CES** | Current Employment Statistics — industry hierarchy and canonical codes |
| **SAE** | State and Area Employment — state-level employment estimates |
| **BED** | Business Employment Dynamics — establishment births, deaths, expansions, and contractions |

## Install

```bash
pip install -e .
```

Requires Python 3.11+ with `polars`. Install the `trino` extra for data lake connectivity:

```bash
pip install -e ".[trino]"
```

## Usage

All program data is queried from the Trino data lake via `TrinoSource`. The mapping pipelines read from Trino, filter and transform the data, and produce long-format Polars DataFrames keyed by geography and industry.

```python
from bls_stats_aggregation.data_source.trino import TrinoSource
from bls_stats_aggregation.qcew.mapping import map_bulk_to_ces

source = TrinoSource(host="trino.example.com", catalog="hive", schema="bls")

# Query QCEW data from the data lake and map to CES industry groups
raw = source.read_qcew(start_year=2020, end_year=2024)
ces = map_bulk_to_ces(raw)
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

- **By industry**: NAICS 2-digit codes are mapped to CES sector codes, then aggregated up through supersectors to domains.
- **By ownership**: Government employment uses `own_code` 1/2/3 (Federal/State/Local) on total industry rows, mapped to CES sectors 91/92/93.
- **By state**: Area FIPS codes are mapped to `geographic_type` (national/state) and `geographic_code` (2-digit state FIPS).
- **Manufacturing split**: 3-digit NAICS subsectors are split into durable goods (CES 31) and nondurable goods (CES 32) via `NAICS3_TO_MFG_SECTOR`.

### Key constants

```python
from bls_stats_aggregation.ces import (
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
