"""Map QCEW data to CES industry groups.

Two mapping pipelines:

1. **API mapping** (``map_qcew_to_ces``): operates on raw output from
   ``fetch_qcew`` / ``fetch_qcew_with_geography``. Extracts private-sector
   employment by industry, government employment by ownership, then aggregates
   through sectors → supersectors → domains.

2. **Bulk mapping** (``map_bulk_to_ces``): operates on the parquet file
   produced by ``download_qcew_bulk``. Handles four input streams:
   total (all-ownership), private 2-digit sectors, government by ownership,
   and 3-digit manufacturing (durable/nondurable split). Aggregates through
   the full hierarchy.

Both produce a long-format DataFrame with monthly employment levels keyed by
``(geographic_type, geographic_code, industry_type, industry_code, ref_date)``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from .industry import (
    GOVT_OWNERSHIP_TO_SECTOR,
    NAICS3_TO_MFG_SECTOR,
    get_domain_supersectors,
    get_supersector_components,
    qcew_to_sector,
)


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_QCEW_SECTOR_MAP: dict[str, str] = {
    **qcew_to_sector(),
    '31-33': '31',
    '44-45': '44',
    '48-49': '48',
}

# NAICS 2-digit codes for private sectors, excluding manufacturing (handled
# via the 3-digit durable/nondurable split in bulk processing).
_NAICS_TO_SECTOR: dict[str, str] = {
    k: v
    for k, v in {**qcew_to_sector(), '44-45': '44', '48-49': '48'}.items()
    if v != '31'
}

# Sector → supersector lookup including government and nondurable mfg.
_SECTOR_TO_SS: dict[str, str] = {}
for _ss, _sectors in get_supersector_components().items():
    for _sec in _sectors:
        _SECTOR_TO_SS[_sec] = _ss
_SECTOR_TO_SS['32'] = '30'

_DOMAIN_SPECS: dict[str, list[str]] = {
    '05': get_domain_supersectors('05'),
    '06': get_domain_supersectors('06'),
    '07': get_domain_supersectors('07'),
    '08': get_domain_supersectors('08'),
}


# ---------------------------------------------------------------------------
# API mapping (from fetch_qcew / fetch_qcew_with_geography output)
# ---------------------------------------------------------------------------

def _map_industry_code(code: str) -> str | None:
    """Map a QCEW industry code to a NAICS-based sector code."""
    return _QCEW_SECTOR_MAP.get(code)


def extract_sector_employment(
    raw: pl.DataFrame,
    geographic_type: str = 'national',
    geographic_code: str = '00',
) -> pl.DataFrame:
    """Parse raw QCEW data into sector-level monthly employment rows.

    Returns a DataFrame with columns: ``industry_code``, ``ref_date``,
    ``employment``, ``qtr``, ``geographic_type``, ``geographic_code``.
    """
    required = [
        'industry_code', 'year', 'qtr',
        'month1_emplvl', 'month2_emplvl', 'month3_emplvl',
    ]
    for col in required:
        if col not in raw.columns:
            return pl.DataFrame()

    rows: list[dict] = []
    for row in raw.iter_rows(named=True):
        ind_code = str(row['industry_code'])
        sector = _map_industry_code(ind_code)
        if sector is None:
            continue

        year = int(row['year'])
        qtr = int(row['qtr'])
        geo_t = str(row.get('geographic_type', geographic_type))
        geo_c = str(row.get('geographic_code', geographic_code))

        for m_idx, m_col in enumerate(
            ['month1_emplvl', 'month2_emplvl', 'month3_emplvl'],
            start=1,
        ):
            if row.get(m_col) is None:
                continue
            try:
                emp = int(row[m_col])
            except (ValueError, TypeError):
                continue
            if emp <= 0:
                continue

            month_num = (qtr - 1) * 3 + m_idx
            ref_date = date(year, month_num, 12)
            rows.append({
                'industry_code': sector,
                'ref_date': ref_date,
                'employment': emp,
                'qtr': qtr,
                'geographic_type': geo_t,
                'geographic_code': geo_c,
            })

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def extract_government_employment(raw: pl.DataFrame) -> pl.DataFrame:
    """Extract government employment from QCEW data with ownership codes 1/2/3.

    Government employment uses ``own_code`` (1=Federal, 2=State, 3=Local)
    on the ``industry_code='10'`` (Total) rows.
    """
    required = [
        'own_code', 'industry_code', 'year', 'qtr',
        'month1_emplvl', 'month2_emplvl', 'month3_emplvl',
    ]
    for col in required:
        if col not in raw.columns:
            return pl.DataFrame()

    rows: list[dict] = []
    for row in raw.iter_rows(named=True):
        own = str(row['own_code'])
        govt_sector = GOVT_OWNERSHIP_TO_SECTOR.get(own)
        if govt_sector is None:
            continue
        if str(row['industry_code']) != '10':
            continue

        year = int(row['year'])
        qtr = int(row['qtr'])
        geo_t = str(row.get('geographic_type', 'national'))
        geo_c = str(row.get('geographic_code', '00'))

        for m_idx, m_col in enumerate(
            ['month1_emplvl', 'month2_emplvl', 'month3_emplvl'],
            start=1,
        ):
            if row.get(m_col) is None:
                continue
            try:
                emp = int(row[m_col])
            except (ValueError, TypeError):
                continue
            if emp <= 0:
                continue

            month_num = (qtr - 1) * 3 + m_idx
            ref_date = date(year, month_num, 12)
            rows.append({
                'industry_code': govt_sector,
                'ref_date': ref_date,
                'employment': emp,
                'qtr': qtr,
                'geographic_type': geo_t,
                'geographic_code': geo_c,
            })

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def aggregate_to_hierarchy(sector_df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate sector employment into supersector and domain totals.

    Args:
        sector_df: Sector-level employment with columns ``industry_code``,
            ``ref_date``, ``employment``, ``qtr``, ``geographic_type``,
            ``geographic_code``.

    Returns:
        Combined DataFrame with sector, supersector, and domain rows. Each row has
        an ``industry_type`` column.
    """
    if len(sector_df) == 0:
        return pl.DataFrame()

    result_parts = [
        sector_df.with_columns(pl.lit('sector').alias('industry_type'))
    ]

    group_cols = ['geographic_type', 'geographic_code', 'ref_date', 'qtr']

    # Supersectors: sum component sectors
    ss_components = get_supersector_components()
    for ss_code, component_sectors in ss_components.items():
        ss_df = (
            sector_df
            .filter(pl.col('industry_code').is_in(component_sectors))
            .group_by(group_cols)
            .agg(pl.col('employment').sum())
            .with_columns(
                pl.lit(ss_code).alias('industry_code'),
                pl.lit('supersector').alias('industry_type'),
            )
        )
        if len(ss_df) > 0:
            result_parts.append(ss_df)

    # Domains: sum component supersectors
    ss_rows = [p for p in result_parts if 'industry_type' in p.columns]
    if len(ss_rows) > 1:
        all_ss = pl.concat(
            [p.filter(pl.col('industry_type') == 'supersector') for p in ss_rows],
            how='diagonal_relaxed',
        )
    else:
        all_ss = pl.DataFrame()

    if len(all_ss) > 0:
        for domain_code in ['00', '05', '06', '07', '08']:
            component_ss = get_domain_supersectors(domain_code)
            dom_df = (
                all_ss
                .filter(pl.col('industry_code').is_in(component_ss))
                .group_by(group_cols)
                .agg(pl.col('employment').sum())
                .with_columns(
                    pl.lit(domain_code).alias('industry_code'),
                    pl.lit('domain').alias('industry_type'),
                )
            )
            if len(dom_df) > 0:
                result_parts.append(dom_df)

    if not result_parts:
        return pl.DataFrame()

    return pl.concat(result_parts, how='diagonal_relaxed')


def map_qcew_to_ces(
    raw: pl.DataFrame,
    include_government: bool = True,
) -> pl.DataFrame:
    """Map raw QCEW API data to CES industry groups.

    Extracts private-sector employment by industry, optionally adds
    government employment by ownership, then aggregates through the
    CES hierarchy (sectors → supersectors → domains).

    Args:
        raw: Raw QCEW data from ``fetch_qcew`` or ``fetch_qcew_with_geography``.
        include_government: If ``True``, extract government employment from ownership
            codes 1/2/3 and include sectors 91, 92, 93.

    Returns:
        Employment data with columns ``geographic_type``, ``geographic_code``,
        ``industry_type``, ``industry_code``, ``ref_date``, ``employment``, ``qtr``.
    """
    if len(raw) == 0:
        return pl.DataFrame()

    # Private-sector employment (own_code='5')
    if 'own_code' in raw.columns:
        private_raw = raw.filter(pl.col('own_code') == '5')
    else:
        private_raw = raw
    sector_emp = extract_sector_employment(private_raw)

    # Government employment (own_code 1/2/3 on industry_code='10')
    if include_government and 'own_code' in raw.columns:
        govt_raw = raw.filter(pl.col('own_code').is_in(['1', '2', '3']))
        if len(govt_raw) > 0:
            govt_emp = extract_government_employment(govt_raw)
            if len(govt_emp) > 0:
                sector_emp = (
                    pl.concat([sector_emp, govt_emp], how='diagonal_relaxed')
                    if len(sector_emp) > 0
                    else govt_emp
                )

    if len(sector_emp) == 0:
        return pl.DataFrame()

    return aggregate_to_hierarchy(sector_emp)


# ---------------------------------------------------------------------------
# Bulk mapping (from download_qcew_bulk output)
# ---------------------------------------------------------------------------

def map_bulk_to_ces(bulk_path: Path | str) -> pl.DataFrame:
    """Map bulk QCEW parquet data to CES industry groups.

    Processes four input streams from the bulk data:

    1. **Total** (``own_code='0'``) → total nonfarm (industry_code ``'00'``)
    2. **Private 2-digit sectors** (``own_code='5'``, ``agglvl`` 14/54) —
       all sectors except manufacturing
    3. **Government by ownership** (``own_code`` 1/2/3, ``agglvl`` 11/51) →
       sectors 91/92/93
    4. **Manufacturing 3-digit** (``own_code='5'``, ``agglvl`` 15/55) →
       durable (31) / nondurable (32)

    Then aggregates sectors → supersectors → domains.

    Args:
        bulk_path: Path to the parquet file from ``download_qcew_bulk``.

    Returns:
        Employment data with columns ``geographic_type``, ``geographic_code``,
        ``industry_type``, ``industry_code``, ``ref_date``, ``employment``.
    """
    bulk_path = Path(bulk_path)
    if not bulk_path.exists():
        print(f'  bulk file not found: {bulk_path}')
        return pl.DataFrame()

    df = pl.read_parquet(bulk_path)

    # Unpivot monthly columns into long format
    df = df.with_columns(
        pl.col('year').cast(pl.Int32),
        pl.col('qtr').cast(pl.Int32),
    )
    monthly = df.unpivot(
        ['month1_emplvl', 'month2_emplvl', 'month3_emplvl'],
        index=[
            'area_fips', 'own_code', 'industry_code', 'agglvl_code',
            'year', 'qtr',
        ],
        variable_name='month_col',
        value_name='employment',
    ).with_columns(
        month_offset=(
            pl.col('month_col').str.extract(r'month(\d)').cast(pl.Int32) - 1
        ),
        employment=pl.col('employment').cast(pl.Float64),
    ).filter(
        pl.col('employment').is_not_null() & (pl.col('employment') > 0)
    ).with_columns(
        month=(
            (pl.col('qtr') - 1) * 3 + 1 + pl.col('month_offset')
        ).cast(pl.Int32),
    ).with_columns(
        ref_date=pl.date(pl.col('year'), pl.col('month'), 12),
    ).drop('month_col', 'month_offset', 'month')

    # Map area_fips to geographic_type / geographic_code
    monthly = monthly.with_columns(
        geographic_type=pl.when(pl.col('area_fips') == 'US000')
        .then(pl.lit('national'))
        .otherwise(pl.lit('state')),
        geographic_code=pl.when(pl.col('area_fips') == 'US000')
        .then(pl.lit('00'))
        .otherwise(pl.col('area_fips').str.slice(0, 2)),
    )

    geo_group = ['geographic_type', 'geographic_code', 'ref_date']
    sector_group = [*geo_group, 'industry_type', 'industry_code']

    # Stream 1: total (own_code=0)
    total_rows = monthly.filter(
        (pl.col('own_code') == '0')
        & pl.col('agglvl_code').is_in({'10', '50'})
    ).with_columns(
        industry_type=pl.lit('national'),
        industry_code=pl.lit('00'),
    )

    # Stream 2: private 2-digit sectors (excl manufacturing)
    private_raw = monthly.filter(
        (pl.col('own_code') == '5')
        & pl.col('agglvl_code').is_in({'14', '54'})
    )
    naics_map = pl.DataFrame({
        'industry_code': list(_NAICS_TO_SECTOR.keys()),
        'sector_code': list(_NAICS_TO_SECTOR.values()),
    })
    private_sectors = (
        private_raw.join(naics_map, on='industry_code', how='inner')
        .drop('industry_code')
        .rename({'sector_code': 'industry_code'})
        .with_columns(industry_type=pl.lit('sector'))
        .group_by(sector_group)
        .agg(employment=pl.col('employment').sum())
    )

    # Stream 3: government by ownership (own_code 1/2/3)
    govt_raw = monthly.filter(
        pl.col('own_code').is_in({'1', '2', '3'})
        & pl.col('agglvl_code').is_in({'11', '51'})
        & (pl.col('industry_code') == '10')
    )
    govt_map = pl.DataFrame({
        'own_code': list(GOVT_OWNERSHIP_TO_SECTOR.keys()),
        'sector_code': list(GOVT_OWNERSHIP_TO_SECTOR.values()),
    })
    govt_sectors = (
        govt_raw.join(govt_map, on='own_code', how='inner')
        .with_columns(
            industry_type=pl.lit('sector'),
            industry_code=pl.col('sector_code'),
        )
        .group_by(sector_group)
        .agg(employment=pl.col('employment').sum())
    )

    # Stream 4: manufacturing 3-digit → durable (31) / nondurable (32)
    mfg_raw = monthly.filter(
        (pl.col('own_code') == '5')
        & pl.col('agglvl_code').is_in({'15', '55'})
    )
    mfg_map = pl.DataFrame({
        'industry_code': list(NAICS3_TO_MFG_SECTOR.keys()),
        'sector_code': list(NAICS3_TO_MFG_SECTOR.values()),
    })
    mfg_sectors = (
        mfg_raw.join(mfg_map, on='industry_code', how='inner')
        .drop('industry_code')
        .rename({'sector_code': 'industry_code'})
        .with_columns(industry_type=pl.lit('sector'))
        .group_by(sector_group)
        .agg(employment=pl.col('employment').sum())
    )

    # Combine all sector rows
    keep_cols = [
        'geographic_type', 'geographic_code', 'industry_type',
        'industry_code', 'ref_date', 'employment',
    ]
    all_sectors = pl.concat([
        private_sectors.select(keep_cols),
        govt_sectors.select(keep_cols),
        mfg_sectors.select(keep_cols),
    ])

    # Aggregate sectors → supersectors
    ss_map = pl.DataFrame({
        'industry_code': list(_SECTOR_TO_SS.keys()),
        'supersector_code': list(_SECTOR_TO_SS.values()),
    })
    supersector_rows = (
        all_sectors.join(ss_map, on='industry_code', how='inner')
        .group_by([*geo_group, 'supersector_code'])
        .agg(employment=pl.col('employment').sum())
        .rename({'supersector_code': 'industry_code'})
        .with_columns(industry_type=pl.lit('supersector'))
    )

    # Aggregate supersectors → domains
    domain_frames: list[pl.DataFrame] = []
    for domain_code, ss_list in _DOMAIN_SPECS.items():
        domain_df = (
            supersector_rows
            .filter(pl.col('industry_code').is_in(ss_list))
            .group_by(geo_group)
            .agg(employment=pl.col('employment').sum())
            .with_columns(
                industry_code=pl.lit(domain_code),
                industry_type=pl.lit('domain'),
            )
        )
        domain_frames.append(domain_df)

    # Combine all hierarchy levels
    combined = pl.concat([
        total_rows.select(keep_cols),
        all_sectors.select(keep_cols),
        supersector_rows.select(keep_cols),
        pl.concat(domain_frames).select(keep_cols),
    ])

    # Convert employment from counts to thousands
    combined = combined.with_columns(
        employment=pl.col('employment') / 1000.0,
    )

    print(
        f'  bulk: {combined.height:,} rows across '
        f'{combined["industry_code"].n_unique()} industries'
    )
    return combined
