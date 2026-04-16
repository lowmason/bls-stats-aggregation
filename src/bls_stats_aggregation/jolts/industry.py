"""JOLTS-to-CES industry crosswalk and series ID parsing.

Maps JOLTS industry codes to the CES industry hierarchy at the domain
and supersector levels.  Provides helpers for parsing the 21-character
JOLTS series ID into its component fields.
"""

from __future__ import annotations

import polars as pl


# ---------------------------------------------------------------------------
# JOLTS → CES industry mapping
# ---------------------------------------------------------------------------
# Each key is a 6-digit JOLTS industry code; value is (ces_code, industry_type).

JOLTS_TO_CES: dict[str, tuple[str, str]] = {
    "000000": ("00", "national"),  # Total Nonfarm
    "100000": ("05", "domain"),  # Total Private
    "110099": ("10", "supersector"),  # Mining and Logging
    "230000": ("20", "supersector"),  # Construction
    "300000": ("30", "supersector"),  # Manufacturing
    "400000": ("40", "supersector"),  # Trade, Transportation, and Utilities
    "510000": ("50", "supersector"),  # Information
    "510099": ("55", "supersector"),  # Financial Activities
    "540099": ("60", "supersector"),  # Professional and Business Services
    "600000": ("65", "supersector"),  # Education and Health Services
    "700000": ("70", "supersector"),  # Leisure and Hospitality
    "810000": ("80", "supersector"),  # Other Services
}

_GOODS_SUPERSECTORS: list[str] = ["10", "20", "30"]

# ---------------------------------------------------------------------------
# Data elements (restricted to hires and total separations)
# ---------------------------------------------------------------------------

JOLTS_DATA_ELEMENTS: dict[str, str] = {
    "HI": "hires",
    "TS": "total_separations",
}


# ---------------------------------------------------------------------------
# Series ID parsing
# ---------------------------------------------------------------------------
# JOLTS series ID structure (21 characters):
#
#   JTS000000000000000JOR
#   ││ │      │  │    │ ││
#   ││ │      │  │    │ │└─ ratelevel_code (R=rate, L=level)
#   ││ │      │  │    │ └── dataelement_code (2 chars)
#   ││ │      │  │    └──── sizeclass_code (2 chars, 00=all)
#   ││ │      │  └───────── area_code (5 chars, 00000=all)
#   ││ │      └──────────── state_code (2 chars, 00=total US)
#   ││ └─────────────────── industry_code (6 chars)
#   │└───────────────────── seasonal (S/U)
#   └────────────────────── survey prefix (JT)


def _parse_series_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Extract component fields from the ``series_id`` column.

    Assumes ``series_id`` has already been whitespace-stripped.

    Args:
        df: DataFrame with a ``series_id`` string column.

    Returns:
        DataFrame with added columns: ``seasonal``, ``industry_code``,
        ``state_code``, ``area_code``, ``sizeclass_code``,
        ``dataelement_code``, ``ratelevel_code``.
    """
    sid = pl.col("series_id")
    return df.with_columns(
        seasonal=sid.str.slice(2, 1),
        industry_code=sid.str.slice(3, 6),
        state_code=sid.str.slice(9, 2),
        area_code=sid.str.slice(11, 5),
        sizeclass_code=sid.str.slice(16, 2),
        dataelement_code=sid.str.slice(18, 2),
        ratelevel_code=sid.str.slice(20, 1),
    )
