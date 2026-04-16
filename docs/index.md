# bls-stats-aggregation

Download and process Bureau of Labor Statistics data, mapping it to the CES (Current Employment Statistics) industry hierarchy.

Currently supports two BLS programs:

- **QCEW** (Quarterly Census of Employment and Wages) — monthly employment levels by industry and geography
- **JOLTS** (Job Openings and Labor Turnover Survey) — hires and total separations rates and levels by industry

## Quick links

- [User guide](user-guide.md) — install, pipelines, hierarchy overview
- [API reference](api/package.md) — auto-generated from Google-style docstrings

## Local preview

```bash
pip install '.[docs]'
mkdocs serve
```
