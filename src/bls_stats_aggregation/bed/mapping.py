"""Map BED data to CES industry groups.

Placeholder — will be implemented when BED data is available
in the Trino data lake.
"""

from __future__ import annotations

import polars as pl


def map_bed_to_ces() -> pl.DataFrame:
    """Map BED data to CES industry groups.

    Returns:
        Long-format DataFrame with CES-aligned job dynamics data.

    Raises:
        NotImplementedError: BED mapping is not yet implemented.
    """
    raise NotImplementedError("BED mapping is not yet implemented.")
