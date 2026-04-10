# JOLTS Download & CES Mapping — Implementation Plan

## Overview

Add capability to download JOLTS (Job Openings and Labor Turnover Survey) estimates from the BLS public data files at `https://download.bls.gov/pub/time.series/jt/` and map them to the existing CES industry hierarchy at the **domain** and **supersector** levels, **private industries only**.

JOLTS publishes national monthly estimates for job openings, hires, total separations, quits, layoffs & discharges, and other separations — as both levels (thousands) and rates (percent). Data is available from December 2000 to present.

---

## 1. Scope

### Industries included

**Domains** (3):


| JOLTS code | CES code | Name                      | Source                  |
| ---------- | -------- | ------------------------- | ----------------------- |
| `100000`   | `05`     | Total Private             | Direct from JOLTS       |
| —          | `06`     | Goods-Producing           | Derived: `10 + 20 + 30` |
| —          | `08`     | Private Service-Providing | Derived: `05 - 06`      |


**Supersectors** (10 — private only):


| JOLTS code | CES code | Name                                 |
| ---------- | -------- | ------------------------------------ |
| `110099`   | `10`     | Mining and Logging                   |
| `230000`   | `20`     | Construction                         |
| `300000`   | `30`     | Manufacturing                        |
| `400000`   | `40`     | Trade, Transportation, and Utilities |
| `510000`   | `50`     | Information                          |
| `510099`   | `55`     | Financial Activities                 |
| `540099`   | `60`     | Professional and Business Services   |
| `600000`   | `65`     | Education and Health Services        |
| `700000`   | `70`     | Leisure and Hospitality              |
| `810000`   | `80`     | Other Services                       |


### Excluded

- Government industries (domains `00`/`07`, supersector `90`, all govt sectors)
- All sector-level industries
- Regional estimates (Census regions NE/MW/SO/WE)
- Establishment size class breakdowns
- Annual averages (`M13` period)

---

## 2. New module: `src/qcew_stats/jolts.py`

### 2.1 Constants

```python
JOLTS_TO_CES: dict[str, tuple[str, str]] = {
    # jolts_industry_code → (ces_code, industry_type)
    '100000': ('05', 'domain'),
    '110099': ('10', 'supersector'),
    '230000': ('20', 'supersector'),
    '300000': ('30', 'supersector'),
    '400000': ('40', 'supersector'),
    '510000': ('50', 'supersector'),
    '510099': ('55', 'supersector'),
    '540099': ('60', 'supersector'),
    '600000': ('65', 'supersector'),
    '700000': ('70', 'supersector'),
    '810000': ('80', 'supersector'),
}

_GOODS_SUPERSECTORS: list[str] = ['10', '20', '30']

JOLTS_DATA_ELEMENTS: dict[str, str] = {
    'JO': 'job_openings',
    'HI': 'hires',
    'TS': 'total_separations',
    'QU': 'quits',
    'LD': 'layoffs_discharges',
    'OS': 'other_separations',
}
```

### 2.2 Series ID structure

```
JTS000000000000000JOR
││ │      │  │    │ ││
││ │      │  │    │ │└─ ratelevel_code (R=rate, L=level)
││ │      │  │    │ └── dataelement_code (JO, HI, TS, QU, LD, OS)
││ │      │  │    └──── sizeclass_code (00=all)
││ │      │  └───────── area_code (00000=all)
││ │      └──────────── state_code (00=total US)
││ └─────────────────── industry_code (6 digits)
│└───────────────────── seasonal (S/U)
└────────────────────── survey prefix (JT)
```

Parse with string slicing:

- `series_id[2]` → seasonal
- `series_id[3:9]` → industry_code
- `series_id[9:11]` → state_code
- `series_id[11:16]` → area_code
- `series_id[16:18]` → sizeclass_code
- `series_id[18:20]` → dataelement_code
- `series_id[20]` → ratelevel_code

---

## 3. Download function: `download_jolts`

### 3.1 Data source

Download `jt.data.1.AllItems` (~33 MB tab-separated text) from `https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems`. Columns: `series_id`, `year`, `period`, `value`, `footnote_codes`.

### 3.2 Implementation tasks

- Add `download_jolts()` to `src/qcew_stats/jolts.py`.
- Use existing `http_client.create_client` / `get_with_retry` for download.
- Parse tab-separated text into Polars DataFrame.
- Extract components from `series_id` via string slicing (see 2.2).
- Filter to:
  - `seasonal == 'S'` (seasonally adjusted).
  - `state_code == '00'` and `area_code == '00000'` (national only).
  - `sizeclass_code == '00'` (all sizes).
  - `ratelevel_code` in `{'L', 'R'}` (both levels and rates).
  - `industry_code` in `JOLTS_TO_CES` keys.
  - `dataelement_code` in `JOLTS_DATA_ELEMENTS` keys.
  - `period` matching `M01`–`M12` (exclude `M13` annual averages).
- Build `ref_date` from `year` + month extracted from period.
- Save filtered result as parquet.

### 3.3 Function signature

```python
def download_jolts(
    output_path: Path | str = 'data/jolts.parquet',
    *,
    client: httpx.Client | None = None,
) -> Path:
```

---

## 4. Mapping function: `map_jolts_to_ces` 

### 4.1 Implementation tasks

- Add `map_jolts_to_ces()` to `src/qcew_stats/jolts.py`.
- Read the JOLTS parquet from `download_jolts`.
- Map `industry_code` → CES `industry_code` and `industry_type` via `JOLTS_TO_CES`.
- Map `dataelement_code` → human-readable name via `JOLTS_DATA_ELEMENTS`.
- Carry `ratelevel_code` through as `rate_or_level` column (`'level'` / `'rate'`).
- Derive domain `06` (Goods-Producing): for **levels**, sum supersectors `10 + 20 + 30` for each `(data_element, ref_date)`. For **rates**, derive as weighted average or omit (see note below).
- Derive domain `08` (Private Service-Providing): for **levels**, compute `05 - 06`. For **rates**, derive or omit.
- Output long-format DataFrame.

**Note on derived rates**: JOLTS rates are computed as `(element / employment) * 100`. Derived domain rates cannot simply be summed from supersector rates — they require the underlying employment weights. Options:

- Derive levels only for domains `06`/`08` and omit rates.
- Pull CES employment data to compute proper weighted rates.
- Decide approach and implement accordingly.

### 4.2 Output schema

```
┌───────────────┬───────────────┬───────────────┬──────────────────┬────────────┬─────────┐
│ industry_type │ industry_code │ rate_or_level │ data_element     │ ref_date   │ value   │
│ str           │ str           │ str           │ str              │ date       │ f64     │
╞═══════════════╪═══════════════╪═══════════════╪══════════════════╪════════════╪═════════╡
│ domain        │ 05            │ level         │ job_openings     │ 2024-01-01 │ 8813.0  │
│ domain        │ 05            │ rate          │ job_openings     │ 2024-01-01 │ 5.4     │
│ domain        │ 06            │ level         │ job_openings     │ 2024-01-01 │ 1205.0  │
│ domain        │ 08            │ level         │ job_openings     │ 2024-01-01 │ 7608.0  │
│ supersector   │ 30            │ level         │ hires            │ 2024-01-01 │ 382.0   │
│ supersector   │ 30            │ rate          │ hires            │ 2024-01-01 │ 3.0     │
└───────────────┴───────────────┴───────────────┴──────────────────┴────────────┴─────────┘
```

### 4.3 Function signature

```python
def map_jolts_to_ces(
    jolts_path: Path | str = 'data/jolts.parquet',
) -> pl.DataFrame:
```

---

## 5. Deriving domains 06 and 08

Domain `06` (Goods-Producing) and `08` (Private Service-Providing) are not published directly by JOLTS. For **levels**, they are computed from the supersector estimates:

```python
# For each (data_element, ref_date):
domain_06 = supersector_10 + supersector_20 + supersector_30
domain_08 = domain_05 - domain_06
```

This mirrors the approach in `mapping.py` where QCEW domains are aggregated from supersectors via `_DOMAIN_SPECS` and `get_domain_supersectors()`.

- Reuse `get_domain_supersectors('06')` → `['10', '20', '30']` for goods-producing.
- Compute `08` as residual: `05 - 06`.
- Only derive for `rate_or_level == 'level'`. Rates for derived domains require employment weights and should be omitted unless CES employment data is available.

---

## 6. Integration with existing package

### 6.1 Module updates

- Create `src/qcew_stats/jolts.py`.
- Update `src/qcew_stats/__init__.py` — export `download_jolts`, `map_jolts_to_ces`, `JOLTS_TO_CES`, `JOLTS_DATA_ELEMENTS`.
- Update `README.md` with JOLTS usage example.
- Add `docs/api/jolts.md` with `::: qcew_stats.jolts`.
- Add JOLTS entry to `mkdocs.yml` nav under API reference.

### 6.2 Tests: `tests/test_jolts.py`

- Test `JOLTS_TO_CES` covers all 11 expected mappings (1 domain + 10 supersectors).
- Test series_id parsing extracts correct components from a known ID string.
- Test period parsing: `M01` → month 1, `M13` → excluded.
- Test `map_jolts_to_ces` with synthetic parquet data:
  - Verify direct mappings produce correct `industry_code` and `industry_type`.
  - Verify domain `06` level = sum of supersectors `10 + 20 + 30`.
  - Verify domain `08` level = `05 - 06`.
  - Verify rates pass through for direct-mapped industries.
  - Verify no rates produced for derived domains `06`/`08`.
  - Verify output columns and dtypes match schema.
- Test filtering: only `S`, national, `00` sizeclass, exclude `M13`.
- Mock HTTP calls — do not hit BLS in tests.

