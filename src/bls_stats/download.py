"""QCEW bulk data download from BLS.

Downloads yearly ~280 MB quarterly singlefile ZIPs from
``data.bls.gov/cew/data/files/``, available from 2003 onward.  Each ZIP
is downloaded, filtered to national + state rows for the needed aggregation
levels, and saved as a compact parquet file.
"""

from __future__ import annotations

import io
import tempfile
import zipfile
from pathlib import Path

import httpx
import polars as pl

from .geography import STATES
from .http_client import create_client, get_with_retry

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
