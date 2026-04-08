"""Download QCEW bulk data from BLS and map to CES industry groups.

    >>> from qcew_stats import download_qcew_bulk, map_bulk_to_ces
    >>> path = download_qcew_bulk(start_year=2020, end_year=2024)
    >>> ces = map_bulk_to_ces(path)
"""

from .download import (
    download_qcew_bulk,
)
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
)
from .mapping import (
    map_bulk_to_ces,
)

__all__ = [
    # Download
    'download_qcew_bulk',
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
    # Mapping
    'map_bulk_to_ces',
]
