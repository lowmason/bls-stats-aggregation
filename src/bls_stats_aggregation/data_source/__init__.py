"""Data source connectors for BLS statistics aggregation.

Provides pluggable data sources for reading BLS program data.
The primary implementation reads from Trino data lakes.
"""

from __future__ import annotations

from .trino import TrinoSource

__all__ = ['TrinoSource']
