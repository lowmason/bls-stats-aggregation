"""Map bulk QCEW data to CES industry groups.

Operates on the parquet file produced by ``download_qcew_bulk``.  Handles
four input streams: total (all-ownership), private 2-digit sectors,
government by ownership, and 3-digit manufacturing (durable/nondurable
split).  Aggregates through the full CES hierarchy:

    sectors → supersectors → domains

Produces a long-format DataFrame with monthly employment levels keyed by
``(geographic_type, geographic_code, industry_type, industry_code, ref_date)``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .industry import (
    GOVT_OWNERSHIP_TO_SECTOR,
    NAICS3_TO_MFG_SECTOR,
    get_domain_supersectors,
    get_sector_codes,
    get_supersector_components,
)


# ---------------------------------------------------------------------------
# Mapping constants
# ---------------------------------------------------------------------------

# NAICS 2-digit codes for private sectors, excluding manufacturing (handled
# via the 3-digit durable/nondurable split).  Bulk data uses range codes
# '44-45' and '48-49' for retail and transportation.
_NAICS_TO_SECTOR: dict[str, str] = {
    code: code for code in get_sector_codes() if code != '31'
}
_NAICS_TO_SECTOR['44-45'] = '44'
_NAICS_TO_SECTOR['48-49'] = '48'

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
# Bulk mapping
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
        'ref_date', 
        'geographic_type', 'geographic_code', 
        'industry_type', 'industry_code', 
        'employment',
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
