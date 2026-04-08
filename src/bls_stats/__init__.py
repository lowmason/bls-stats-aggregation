"""Download QCEW data from BLS and map to CES industry groups.

Two pipelines:

**CSV API pipeline** (2014+): per-quarter downloads via the QCEW CSV API.

    >>> from bls_stats import fetch_qcew_with_geography, map_qcew_to_ces
    >>> raw = fetch_qcew_with_geography([2024], ownership_codes=['5', '1', '2', '3'])
    >>> ces = map_qcew_to_ces(raw)

**Bulk pipeline** (2003+): yearly singlefile ZIPs, filtered and mapped.

    >>> from bls_stats import download_qcew_bulk, map_bulk_to_ces
    >>> path = download_qcew_bulk(start_year=2020, end_year=2024)
    >>> ces = map_bulk_to_ces(path)
"""

from .download import (
    QCEW_INDUSTRY_CODES,
    download_qcew_bulk,
    fetch_qcew,
    fetch_qcew_with_geography,
)
from .http_client import BLSHttpClient
from .industry import (
    CES_SECTOR_TO_NAICS,
    DOMAIN_DEFINITIONS,
    GOVT_OWNERSHIP_TO_SECTOR,
    INDUSTRY_HIERARCHY,
    INDUSTRY_MAP,
    NAICS3_TO_MFG_SECTOR,
    SINGLE_SECTOR_SUPERSECTORS,
    IndustryEntry,
    get_domain_supersectors,
    get_sector_codes,
    get_supersector_codes,
    get_supersector_components,
    qcew_to_sector,
)
from .mapping import (
    aggregate_to_hierarchy,
    extract_government_employment,
    extract_sector_employment,
    map_bulk_to_ces,
    map_qcew_to_ces,
)

__all__ = [
    # Download
    'BLSHttpClient',
    'QCEW_INDUSTRY_CODES',
    'download_qcew_bulk',
    'fetch_qcew',
    'fetch_qcew_with_geography',
    # Industry hierarchy
    'CES_SECTOR_TO_NAICS',
    'DOMAIN_DEFINITIONS',
    'GOVT_OWNERSHIP_TO_SECTOR',
    'INDUSTRY_HIERARCHY',
    'INDUSTRY_MAP',
    'IndustryEntry',
    'NAICS3_TO_MFG_SECTOR',
    'SINGLE_SECTOR_SUPERSECTORS',
    'get_domain_supersectors',
    'get_sector_codes',
    'get_supersector_codes',
    'get_supersector_components',
    'qcew_to_sector',
    # Mapping
    'aggregate_to_hierarchy',
    'extract_government_employment',
    'extract_sector_employment',
    'map_bulk_to_ces',
    'map_qcew_to_ces',
]
