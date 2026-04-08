"""QCEW data download from BLS.

Two download paths:

1. **CSV API** (``fetch_qcew``, ``fetch_qcew_with_geography``): per-quarter,
   per-industry requests to ``data.bls.gov/cew/data/api/``. Available from
   2014 onward.

2. **Bulk singlefiles** (``download_qcew_bulk``): yearly ~280 MB ZIPs from
   ``data.bls.gov/cew/data/files/``. Available from 2003 onward. Each ZIP
   is downloaded, filtered to national + state rows for the needed aggregation
   levels, and saved as a compact parquet file.
"""

from __future__ import annotations

import io
import logging
import re
import tempfile
import zipfile
from pathlib import Path

import httpx
import polars as pl

from .geography import STATES
from .http_client import BLSHttpClient, create_client, get_with_retry

logger = logging.getLogger(__name__)

_STATE_AREA_RE = re.compile(r'^\d{2}000$')

# ---------------------------------------------------------------------------
# QCEW CSV API industry codes
# ---------------------------------------------------------------------------

QCEW_INDUSTRY_CODES: dict[str, str] = {
    '10': 'Total, all industries',
    '1011': 'NAICS 11 - Agriculture',
    '1012': 'NAICS 21 - Mining',
    '1013': 'NAICS 22 - Utilities',
    '1021': 'NAICS 23 - Construction',
    '1022': 'NAICS 31-33 - Manufacturing',
    '1023': 'NAICS 42 - Wholesale Trade',
    '1024': 'NAICS 44-45 - Retail Trade',
    '1025': 'NAICS 48-49 - Transportation and Warehousing',
    '1026': 'NAICS 51 - Information',
    '1027': 'NAICS 52 - Finance and Insurance',
    '1028': 'NAICS 53 - Real Estate',
    '1029': 'NAICS 54 - Professional and Technical Services',
    '102A': 'NAICS 55 - Management of Companies',
    '102B': 'NAICS 56 - Administrative and Waste Services',
    '102C': 'NAICS 61 - Educational Services',
    '102D': 'NAICS 62 - Health Care and Social Assistance',
    '102E': 'NAICS 71 - Arts, Entertainment, and Recreation',
    '102F': 'NAICS 72 - Accommodation and Food Services',
    '102G': 'NAICS 81 - Other Services',
}

_DEFAULT_INDUSTRIES = [
    '10',
    '1012', '1013', '1021', '1022', '1023', '1024', '1025',
    '1026', '1027', '1028', '1029', '102A', '102B', '102C',
    '102D', '102E', '102F', '102G',
]


# ---------------------------------------------------------------------------
# CSV API downloads
# ---------------------------------------------------------------------------

def fetch_qcew(
    years: list[int],
    quarters: list[int] | None = None,
    industries: list[str] | None = None,
    area_fips: str = 'US000',
    ownership_code: str = '5',
    client: BLSHttpClient | None = None,
) -> pl.DataFrame:
    """Download QCEW data for the specified year/quarter/industry combinations.

    Args:
        years: Reference years (e.g., ``[2020, 2021, 2022]``).
        quarters: Quarters (1-4). Defaults to ``[1, 2, 3, 4]``.
        industries: QCEW API industry codes. If ``None``, defaults to all 2-digit
            private-sector NAICS codes.
        area_fips: Area FIPS code. Defaults to ``'US000'`` (national).
        ownership_code: Ownership filter. Defaults to ``'5'`` (private).
        client: Optional HTTP client. Creates a default if ``None``.

    Returns:
        Raw QCEW data with columns including ``area_fips``, ``own_code``,
        ``industry_code``, ``year``, ``qtr``, ``month1_emplvl``,
        ``month2_emplvl``, ``month3_emplvl``, and others.
    """
    if quarters is None:
        quarters = [1, 2, 3, 4]
    if industries is None:
        industries = _DEFAULT_INDUSTRIES

    own_client = client is None
    if own_client:
        client = BLSHttpClient()

    try:
        frames: list[pl.DataFrame] = []
        first_failure: str | None = None
        for year in years:
            for qtr in quarters:
                for industry in industries:
                    try:
                        df = client.get_qcew_csv(year, qtr, industry)
                    except Exception as e:
                        if first_failure is None:
                            first_failure = (
                                f'{year}Q{qtr} industry={industry}: {e}'
                            )
                        logger.debug(
                            'QCEW fetch failed: %dQ%d industry=%s: %s',
                            year, qtr, industry, e,
                        )
                        continue

                    if len(df) == 0:
                        continue

                    if 'own_code' in df.columns:
                        df = df.with_columns(pl.col('own_code').cast(pl.Utf8))
                    if 'area_fips' in df.columns:
                        df = df.with_columns(pl.col('area_fips').cast(pl.Utf8))

                    df = df.filter(
                        (pl.col('area_fips') == area_fips)
                        & (pl.col('own_code') == ownership_code)
                    )

                    if len(df) > 0:
                        frames.append(df)

        if not frames:
            logger.warning(
                'No QCEW data fetched. The BLS QCEW CSV API only has '
                'data from 2014 onward.'
            )
            if first_failure:
                logger.warning('First failure: %s', first_failure)
            return pl.DataFrame()

        return pl.concat(frames, how='diagonal_relaxed')
    finally:
        if own_client:
            client.close()


def fetch_qcew_with_geography(
    years: list[int],
    quarters: list[int] | None = None,
    industries: list[str] | None = None,
    ownership_codes: list[str] | None = None,
    include_national: bool = True,
    include_states: bool = True,
    state_fips_list: list[str] | None = None,
    client: BLSHttpClient | None = None,
) -> pl.DataFrame:
    """Download QCEW data for national and/or state-level geographies.

    Args:
        years: Reference years.
        quarters: Quarters (1-4). Defaults to all four.
        industries: QCEW API industry codes. Defaults to all private-sector + total.
        ownership_codes: Ownership codes to include. Defaults to ``['5']`` (private
            only). Use ``['5', '1', '2', '3']`` to include government sectors.
        include_national: Include national (US000) rows. Defaults to ``True``.
        include_states: Include state-level rows. Defaults to ``True``.
        state_fips_list: Specific state FIPS to include. If ``None``, uses all 50
            states + DC + PR.
        client: Optional HTTP client.

    Returns:
        Raw QCEW data with added ``geographic_type`` and ``geographic_code`` columns.
    """
    if quarters is None:
        quarters = [1, 2, 3, 4]
    if industries is None:
        industries = _DEFAULT_INDUSTRIES
    if ownership_codes is None:
        ownership_codes = ['5']
    if state_fips_list is None:
        state_fips_list = STATES

    ownership_set = set(ownership_codes)
    state_areas = {f'{fips}000' for fips in state_fips_list}

    own_client = client is None
    if own_client:
        client = BLSHttpClient()

    try:
        frames: list[pl.DataFrame] = []
        first_failure: str | None = None
        for year in years:
            for qtr in quarters:
                for industry in industries:
                    try:
                        df = client.get_qcew_csv(year, qtr, industry)
                    except Exception as e:
                        if first_failure is None:
                            first_failure = (
                                f'{year}Q{qtr} industry={industry}: {e}'
                            )
                        logger.debug(
                            'QCEW fetch failed: %dQ%d industry=%s: %s',
                            year, qtr, industry, e,
                        )
                        continue

                    if len(df) == 0:
                        continue

                    if 'own_code' in df.columns:
                        df = df.with_columns(pl.col('own_code').cast(pl.Utf8))
                    if 'area_fips' in df.columns:
                        df = df.with_columns(pl.col('area_fips').cast(pl.Utf8))

                    df = df.filter(
                        pl.col('own_code').is_in(list(ownership_set))
                    )

                    if len(df) == 0:
                        continue

                    parts: list[pl.DataFrame] = []

                    if include_national:
                        nat = df.filter(pl.col('area_fips') == 'US000')
                        if len(nat) > 0:
                            nat = nat.with_columns(
                                pl.lit('national').alias('geographic_type'),
                                pl.lit('00').alias('geographic_code'),
                            )
                            parts.append(nat)

                    if include_states:
                        state_df = df.filter(
                            pl.col('area_fips').is_in(list(state_areas))
                        )
                        if len(state_df) > 0:
                            state_df = state_df.with_columns(
                                pl.lit('state').alias('geographic_type'),
                                pl.col('area_fips').str.slice(0, 2).alias(
                                    'geographic_code'
                                ),
                            )
                            parts.append(state_df)

                    if parts:
                        combined = pl.concat(parts, how='diagonal_relaxed')
                        frames.append(combined)

        if not frames:
            logger.warning(
                'No QCEW data fetched. The BLS QCEW CSV API only has '
                'data from 2014 onward.'
            )
            if first_failure:
                logger.warning('First failure: %s', first_failure)
            return pl.DataFrame()

        return pl.concat(frames, how='diagonal_relaxed')
    finally:
        if own_client:
            client.close()


# ---------------------------------------------------------------------------
# Bulk singlefile downloads
# ---------------------------------------------------------------------------

BULK_BASE_URL = 'https://data.bls.gov/cew/data/files'

_STATE_AREA_FIPS: frozenset[str] = frozenset(f'{s}000' for s in STATES)
_WANTED_AREAS: frozenset[str] = _STATE_AREA_FIPS | {'US000'}

# Aggregation levels kept from bulk singlefiles:
#   10/50 = national/state total (all ownership)
#   11/51 = national/state by ownership (government extraction)
#   14/54 = national/state by NAICS 2-digit sector
#   15/55 = national/state by NAICS 3-digit subsector (mfg durable/nondurable)
_WANTED_AGGLVL: frozenset[str] = frozenset(
    {'10', '11', '14', '15', '50', '51', '54', '55'}
)

_KEEP_COLUMNS: list[str] = [
    'area_fips', 'own_code', 'industry_code', 'agglvl_code',
    'year', 'qtr',
    'month1_emplvl', 'month2_emplvl', 'month3_emplvl',
]


def _filter_bulk_csv(csv_bytes: bytes) -> pl.DataFrame:
    """Read a QCEW quarterly singlefile CSV and return the filtered subset.

    Keeps rows matching national + state FIPS, relevant aggregation levels,
    and ownership codes 0 (total), 1/2/3 (government), 5 (private).
    """
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        infer_schema_length=0,
        n_threads=1,
    )
    df = df.filter(
        pl.col('area_fips').is_in(_WANTED_AREAS)
        & pl.col('agglvl_code').is_in(_WANTED_AGGLVL)
        & pl.col('own_code').is_in({'0', '1', '2', '3', '5'})
    )
    present = [c for c in _KEEP_COLUMNS if c in df.columns]
    return df.select(present)


def download_qcew_bulk(
    start_year: int = 2003,
    end_year: int = 2025,
    output_path: Path | str = 'data/qcew_bulk.parquet',
    *,
    client: httpx.Client | None = None,
) -> Path:
    """Download QCEW quarterly singlefile ZIPs and extract filtered data.

    For each year, downloads the ~280 MB ZIP, extracts the CSV, filters to
    national + state rows for total and private-sector industries, then
    discards the ZIP.  The compact filtered result is saved as a single
    parquet file.

    Args:
        start_year: First year to download (default 2003).
        end_year: Last year to download inclusive (default 2025).
        output_path: Path to write the output parquet file.
        client: Optional pre-built client.

    Returns:
        Path to the output parquet file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    own_client = client is None
    if client is None:
        client = create_client()

    frames: list[pl.DataFrame] = []
    try:
        for year in range(start_year, end_year + 1):
            url = f'{BULK_BASE_URL}/{year}/csv/{year}_qtrly_singlefile.zip'
            print(f'  downloading {year} quarterly singlefile ...', flush=True)
            r = get_with_retry(client, url, timeout=300.0)
            r.raise_for_status()

            with tempfile.TemporaryDirectory() as tmp:
                zip_path = Path(tmp) / f'{year}.zip'
                zip_path.write_bytes(r.content)

                with zipfile.ZipFile(zip_path) as zf:
                    csv_names = [
                        n for n in zf.namelist() if n.endswith('.csv')
                    ]
                    if not csv_names:
                        print(
                            f'    WARNING: no CSV in {year} ZIP', flush=True,
                        )
                        continue
                    csv_bytes = zf.read(csv_names[0])

                filtered = _filter_bulk_csv(csv_bytes)
                frames.append(filtered)
                print(
                    f'    {year}: kept {filtered.height:,} rows '
                    f'({len(r.content) / 1024 / 1024:.0f} MB downloaded)',
                    flush=True,
                )
    finally:
        if own_client:
            client.close()

    if not frames:
        print('  WARNING: no data collected', flush=True)
        return output_path

    combined = pl.concat(frames, how='diagonal_relaxed')
    combined.write_parquet(output_path)
    print(f'  wrote {output_path} ({combined.height:,} rows)', flush=True)
    return output_path
