"""SAE (State and Area Employment) subpackage.

Maps SAE nonfarm payroll employment estimates for states and metropolitan
areas to the CES industry hierarchy.  SAE uses the same CES industry codes
at the domain and supersector levels.
"""

from __future__ import annotations

from .mapping import map_sae_to_ces

__all__ = ['map_sae_to_ces']
