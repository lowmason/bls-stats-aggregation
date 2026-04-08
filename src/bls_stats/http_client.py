"""HTTP client for BLS QCEW data access.

Provides ``BLSHttpClient`` for the QCEW CSV API at ``data.bls.gov/cew/data/api/``
and ``get_with_retry`` for bulk file downloads with exponential back-off.
"""

from __future__ import annotations

import io
import logging
import os
import time
from typing import Any

import httpx
import polars as pl

logger = logging.getLogger(__name__)

_USER_AGENT = (
    'bls-stats/0.1.0 '
    '(Python; httpx/{httpx_version})'
).format(httpx_version=httpx.__version__)

_DEFAULT_HEADERS = {
    'User-Agent': _USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-us,en;q=0.5',
}

MAX_RETRIES = 8


class BLSHttpClient:
    """HTTP client for the BLS QCEW CSV API."""

    QCEW_CSV_BASE = 'https://data.bls.gov/cew/data/api'

    def __init__(
        self,
        cache_dir: str = '.cache/bls',
        cache_ttl: int = 86_400,
    ) -> None:
        """Create a client with optional on-disk response cache.

        Args:
            cache_dir: Local directory for cached downloads. Defaults to
                ``'.cache/bls'``.
            cache_ttl: Cache time-to-live in seconds. Defaults to 86400 (24 hours).
        """
        self.cache_dir = cache_dir
        self.cache_ttl = cache_ttl
        self.session = httpx.Client(
            headers={'User-Agent': _USER_AGENT}, timeout=60.0,
        )

    def get_qcew_csv(
        self,
        year: int,
        quarter: int,
        slice_code: str,
        slice_type: str = 'industry',
    ) -> pl.DataFrame:
        """Download QCEW data from the CSV API.

        Args:
            year: Reference year.
            quarter: Reference quarter (1-4).
            slice_code: Industry code (e.g., ``'10'``, ``'1012'``), area code
                (e.g., ``'US000'``), or size code.
            slice_type: One of ``'industry'`` (default), ``'area'``, or ``'size'``.

        Returns:
            Raw QCEW DataFrame parsed from the CSV response.
        """
        if slice_type not in ('industry', 'area', 'size'):
            raise ValueError(
                f'slice_type must be industry, area, or size; got {slice_type!r}'
            )

        cache_key = f'qcew_{year}_{quarter}_{slice_type}_{slice_code}.csv'
        cache_path = self._cache_path(cache_key)

        qcew_text_cols = [
            'area_fips', 'own_code', 'industry_code', 'agglvl_code',
            'size_code', 'year', 'qtr', 'disclosure_code',
            'lq_disclosure_code', 'oty_disclosure_code',
        ]
        schema_overrides = {c: pl.Utf8 for c in qcew_text_cols}

        if self._is_cache_valid(cache_path):
            return pl.read_csv(cache_path, schema_overrides=schema_overrides)

        url = f'{self.QCEW_CSV_BASE}/{year}/{quarter}/{slice_type}/{slice_code}.csv'
        response = self.session.get(url, timeout=60)
        response.raise_for_status()

        os.makedirs(self.cache_dir, exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as fh:
            fh.write(response.text)

        return pl.read_csv(
            io.StringIO(response.text), schema_overrides=schema_overrides,
        )

    def _cache_path(self, filename: str) -> str:
        safe_name = filename.replace('/', '_').replace('\\', '_')
        return os.path.join(self.cache_dir, safe_name)

    def _is_cache_valid(self, path: str) -> bool:
        if not os.path.exists(path):
            return False
        age = time.time() - os.path.getmtime(path)
        return age < self.cache_ttl

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> BLSHttpClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Standalone retry client for bulk downloads
# ---------------------------------------------------------------------------

def create_client(
    *,
    http2: bool = True,
    headers: dict[str, str] | None = None,
    timeout: float = 60.0,
) -> httpx.Client:
    """Build an ``httpx.Client`` with BLS-friendly headers.

    Caller is responsible for closing it.
    """
    merged = {**_DEFAULT_HEADERS}
    if headers:
        merged.update(headers)
    return httpx.Client(http2=http2, headers=merged, timeout=timeout)


def get_with_retry(
    client: httpx.Client,
    url: str,
    *,
    timeout: float = 60.0,
    max_retries: int = MAX_RETRIES,
) -> httpx.Response:
    """GET *url* with exponential back-off on 429 and transient 5xx errors.

    If ``BLS_API_KEY`` is set and the URL contains ``bls.gov``, the key is
    appended as a ``registrationkey`` query parameter.
    """
    params: dict[str, str] = {}
    api_key = os.environ.get('BLS_API_KEY', '')
    if api_key and 'bls.gov' in url:
        params['registrationkey'] = api_key

    for attempt in range(max_retries):
        r = client.get(url, timeout=timeout, params=params)
        if r.status_code == 429 or r.status_code >= 500:
            wait = min(2 ** attempt, 120)
            logger.warning('    [%s] retrying in %ss ...', r.status_code, wait)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r
