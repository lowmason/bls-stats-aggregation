# bls-stats-aggregation

Map Bureau of Labor Statistics program data to the CES (Current Employment Statistics) industry hierarchy. All program data is queried from a Trino data lake.

## Supported programs

- **QCEW** (Quarterly Census of Employment and Wages) — monthly employment levels by industry and geography
- **JOLTS** (Job Openings and Labor Turnover Survey) — hires and total separations rates and levels by industry
- **CES** (Current Employment Statistics) — industry hierarchy and canonical codes
- **SAE** (State and Area Employment) — state-level employment estimates
- **BED** (Business Employment Dynamics) — establishment births, deaths, expansions, and contractions

## Quick links

- [User guide](user-guide.md) — install, pipelines, hierarchy overview
- [API reference](api/package.md) — auto-generated from Google-style docstrings

## Local preview

```bash
pip install '.[docs]'
mkdocs serve
```
