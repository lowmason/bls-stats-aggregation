"""BLS statistics aggregation toolkit.

Maps Bureau of Labor Statistics program data to the CES industry hierarchy.
Program-specific modules live in subpackages (e.g. ``bls_stats_aggregation.qcew``).
"""

from .geography import (
    CENSUS_DIVISIONS,
    CENSUS_REGIONS,
    DIVISION_TO_REGION,
    STATE_FIPS_TO_DIVISION,
    STATE_FIPS_TO_REGION,
)

__all__ = [
    # Geography
    'CENSUS_DIVISIONS',
    'CENSUS_REGIONS',
    'DIVISION_TO_REGION',
    'STATE_FIPS_TO_DIVISION',
    'STATE_FIPS_TO_REGION',
]
