"""Tests for bls_stats_aggregation.sae subpackage."""

from __future__ import annotations

import pytest

from bls_stats_aggregation.sae import map_sae_to_ces


class TestSaeMapping:
    def test_importable(self):
        assert callable(map_sae_to_ces)

    def test_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            map_sae_to_ces()
