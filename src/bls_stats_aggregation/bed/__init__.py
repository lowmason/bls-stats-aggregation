"""BED (Business Employment Dynamics) subpackage.

Maps BED quarterly gross job gains and losses data to the CES industry
hierarchy at the supersector level.
"""

from __future__ import annotations

from .mapping import map_bed_to_ces

__all__ = ['map_bed_to_ces']
