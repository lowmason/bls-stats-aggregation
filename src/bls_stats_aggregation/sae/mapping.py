"""Map SAE data to CES industry groups.

Placeholder — will be implemented when SAE data is available
in the Trino data lake.
"""

from __future__ import annotations

import polars as pl


def map_sae_to_ces() -> pl.DataFrame:
    """Map SAE data to CES industry groups.

    Returns:
        Long-format DataFrame with CES-aligned employment data.

    Raises:
        NotImplementedError: SAE mapping is not yet implemented.
    """
    raise NotImplementedError("SAE mapping is not yet implemented.")
