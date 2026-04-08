"""HTTP client helpers for BLS bulk downloads.

Provides ``create_client`` for building an ``httpx.Client`` with
BLS-friendly headers and ``get_with_retry`` for downloads with
exponential back-off on transient errors.
"""

from __future__ import annotations

import logging
import os
import time

import httpx

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
