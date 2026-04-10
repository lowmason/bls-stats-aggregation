"""Download JOLTS flat file and map to CES industry groups.

Downloads the ``jt.data.1.AllItems`` tab-separated file from
``download.bls.gov/pub/time.series/jt/`` and maps series to the CES
industry hierarchy at the domain and supersector levels.
"""

from __future__ import annotations

import io
from pathlib import Path

import httpx
import polars as pl

from ..http_client import create_client, get_with_retry
from .industry import (
    JOLTS_DATA_ELEMENTS,
    JOLTS_TO_CES,
    _GOODS_SUPERSECTORS,
    _parse_series_columns,
)

_JOLTS_URL = 'https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems'


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_jolts(
    output_path: Path | str = 'data/jolts.parquet',
    *,
    client: httpx.Client | None = None,
) -> Path:
    """Download JOLTS flat file and save filtered data as parquet.

    Downloads the ``jt.data.1.AllItems`` tab-separated file (~33 MB),
    filters to seasonally adjusted national estimates for private
    industries, and writes the result as a compact parquet file.

    Args:
        output_path: Path to write the output parquet file.
        client: Optional pre-built ``httpx.Client``.

    Returns:
        Path to the output parquet file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    own_client = client is None
    if client is None:
        client = create_client()

    try:
        print('  downloading JOLTS flat file ...', flush=True)
        r = get_with_retry(client, _JOLTS_URL, timeout=120.0)
        r.raise_for_status()
    finally:
        if own_client:
            client.close()

    df = pl.read_csv(
        io.BytesIO(r.content),
        separator='\t',
        infer_schema_length=0,
        n_threads=1,
    )

    # Strip whitespace from key columns (BLS flat files have trailing spaces)
    df = df.with_columns(
        pl.col('series_id').str.strip_chars(),
        pl.col('value').str.strip_chars(),
        pl.col('year').str.strip_chars(),
        pl.col('period').str.strip_chars(),
    )

    # Filter to well-formed series IDs
    df = df.filter(pl.col('series_id').str.len_chars() == 21)

    # Parse series ID components
    df = _parse_series_columns(df)

    # Build sets for filtering
    industry_codes = set(JOLTS_TO_CES.keys())
    dataelement_codes = set(JOLTS_DATA_ELEMENTS.keys())

    # Apply all filters
    df = df.filter(
        (pl.col('seasonal') == 'S')
        & (pl.col('state_code') == '00')
        & (pl.col('area_code') == '00000')
        & (pl.col('sizeclass_code') == '00')
        & pl.col('industry_code').is_in(industry_codes)
        & pl.col('dataelement_code').is_in(dataelement_codes)
        & pl.col('period').str.slice(1).cast(pl.Int32).is_between(1, 12)
    )

    # Build ref_date and cast value
    df = df.with_columns(
        ref_date=pl.date(
            pl.col('year').cast(pl.Int32),
            pl.col('period').str.slice(1).cast(pl.Int32),
            1,
        ),
        value=pl.col('value').cast(pl.Float64),
    )

    # Select columns for parquet output
    df = df.select(
        'industry_code', 'dataelement_code', 'ratelevel_code',
        'ref_date', 'value',
    )

    df.write_parquet(output_path)
    print(
        f'  wrote {output_path} ({df.height:,} rows)',
        flush=True,
    )
    return output_path


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------

def map_jolts_to_ces(
    jolts_path: Path | str = 'data/jolts.parquet',
) -> pl.DataFrame:
    """Map JOLTS parquet data to CES industry groups.

    Reads the filtered parquet from :func:`download_jolts`, maps JOLTS
    industry codes to CES codes, and derives goods-producing (``06``)
    and private service-providing (``08``) domains from supersector
    estimates.

    Args:
        jolts_path: Path to the parquet file from ``download_jolts``.

    Returns:
        Long-format DataFrame with columns ``industry_type``,
        ``industry_code``, ``rate_or_level``, ``data_element``,
        ``ref_date``, ``value``.
    """
    jolts_path = Path(jolts_path)
    if not jolts_path.exists():
        print(f'  JOLTS file not found: {jolts_path}')
        return pl.DataFrame()

    df = pl.read_parquet(jolts_path)

    # Map industry_code → CES code and industry_type
    ces_map = pl.DataFrame({
        'industry_code': list(JOLTS_TO_CES.keys()),
        'ces_code': [v[0] for v in JOLTS_TO_CES.values()],
        'industry_type': [v[1] for v in JOLTS_TO_CES.values()],
    })
    df = (
        df.join(ces_map, on='industry_code', how='inner')
        .drop('industry_code')
        .rename({'ces_code': 'industry_code'})
    )

    # Map dataelement_code → human-readable name
    de_map = pl.DataFrame({
        'dataelement_code': list(JOLTS_DATA_ELEMENTS.keys()),
        'data_element': list(JOLTS_DATA_ELEMENTS.values()),
    })
    df = (
        df.join(de_map, on='dataelement_code', how='inner')
        .drop('dataelement_code')
    )

    # Map ratelevel_code → rate_or_level
    df = df.with_columns(
        rate_or_level=pl.when(pl.col('ratelevel_code') == 'L')
        .then(pl.lit('level'))
        .otherwise(pl.lit('rate')),
    ).drop('ratelevel_code')

    output_cols = [
        'industry_type', 'industry_code', 'rate_or_level',
        'data_element', 'ref_date', 'value',
    ]
    mapped = df.select(output_cols)

    # ------------------------------------------------------------------
    # Derive employment from supersector levels and rates.
    # JOLTS rate = (level / employment) * 100, so
    # employment = level * 100 / rate.
    # ------------------------------------------------------------------
    ss_levels = mapped.filter(
        (pl.col('industry_type') == 'supersector')
        & (pl.col('rate_or_level') == 'level')
    ).select(
        'industry_code', 'data_element', 'ref_date',
        pl.col('value').alias('level_value'),
    )
    ss_rates = mapped.filter(
        (pl.col('industry_type') == 'supersector')
        & (pl.col('rate_or_level') == 'rate')
    ).select(
        'industry_code', 'data_element', 'ref_date',
        pl.col('value').alias('rate_value'),
    )
    ss_employment = (
        ss_levels.join(
            ss_rates, on=['industry_code', 'data_element', 'ref_date'],
            how='inner',
        )
        .with_columns(
            employment=(pl.col('level_value') * 100.0 / pl.col('rate_value')),
        )
    )

    # ------------------------------------------------------------------
    # Derive domain 06 (Goods-Producing) — levels and rates
    # ------------------------------------------------------------------
    goods_emp = ss_employment.filter(
        pl.col('industry_code').is_in(_GOODS_SUPERSECTORS)
    )

    domain_06_levels = (
        goods_emp.group_by(['data_element', 'ref_date'])
        .agg(value=pl.col('level_value').sum())
        .with_columns(
            industry_type=pl.lit('domain'),
            industry_code=pl.lit('06'),
            rate_or_level=pl.lit('level'),
        )
        .select(output_cols)
    )

    domain_06_rates = (
        goods_emp.group_by(['data_element', 'ref_date'])
        .agg(
            level_sum=pl.col('level_value').sum(),
            emp_sum=pl.col('employment').sum(),
        )
        .with_columns(
            value=(pl.col('level_sum') / pl.col('emp_sum') * 100.0),
        )
        .drop('level_sum', 'emp_sum')
        .with_columns(
            industry_type=pl.lit('domain'),
            industry_code=pl.lit('06'),
            rate_or_level=pl.lit('rate'),
        )
        .select(output_cols)
    )

    # ------------------------------------------------------------------
    # Derive domain 08 (Private Service-Providing) — levels and rates
    # Domain 08 = domain 05 (Total Private) - domain 06 (Goods-Producing)
    # ------------------------------------------------------------------
    # Domain 05 employment: back-calculate from its own level and rate
    d05_levels = mapped.filter(
        (pl.col('industry_code') == '05')
        & (pl.col('rate_or_level') == 'level')
    ).select('data_element', 'ref_date', pl.col('value').alias('d05_level'))

    d05_rates = mapped.filter(
        (pl.col('industry_code') == '05')
        & (pl.col('rate_or_level') == 'rate')
    ).select('data_element', 'ref_date', pl.col('value').alias('d05_rate'))

    d05_emp = (
        d05_levels.join(d05_rates, on=['data_element', 'ref_date'], how='inner')
        .with_columns(
            d05_employment=(pl.col('d05_level') * 100.0 / pl.col('d05_rate')),
        )
    )

    # Domain 06 aggregates for subtraction
    d06_agg = (
        goods_emp.group_by(['data_element', 'ref_date'])
        .agg(
            d06_level=pl.col('level_value').sum(),
            d06_employment=pl.col('employment').sum(),
        )
    )

    d08_calc = (
        d05_emp.join(d06_agg, on=['data_element', 'ref_date'], how='inner')
        .with_columns(
            d08_level=pl.col('d05_level') - pl.col('d06_level'),
            d08_employment=pl.col('d05_employment') - pl.col('d06_employment'),
        )
    )

    domain_08_levels = (
        d08_calc.select(
            'data_element', 'ref_date',
            pl.col('d08_level').alias('value'),
        )
        .with_columns(
            industry_type=pl.lit('domain'),
            industry_code=pl.lit('08'),
            rate_or_level=pl.lit('level'),
        )
        .select(output_cols)
    )

    domain_08_rates = (
        d08_calc.select(
            'data_element', 'ref_date',
            (pl.col('d08_level') / pl.col('d08_employment') * 100.0)
            .alias('value'),
        )
        .with_columns(
            industry_type=pl.lit('domain'),
            industry_code=pl.lit('08'),
            rate_or_level=pl.lit('rate'),
        )
        .select(output_cols)
    )

    combined = pl.concat([
        mapped,
        domain_06_levels, domain_06_rates,
        domain_08_levels, domain_08_rates,
    ])

    print(
        f'  jolts: {combined.height:,} rows across '
        f'{combined["industry_code"].n_unique()} industries',
    )
    return combined
