"""Tests for bls_stats_aggregation.bed subpackage."""

from __future__ import annotations

import pytest

from bls_stats_aggregation.bed import map_bed_to_ces


class TestBedMapping:
    def test_importable(self):
        assert callable(map_bed_to_ces)

    def test_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            map_bed_to_ces()
