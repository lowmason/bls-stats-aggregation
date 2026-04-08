"""Tests for qcew_stats.geography constants."""

from qcew_stats.geography import STATE_FIPS_TO_NAME, STATE_NAME_TO_FIPS, STATES


class TestStates:
    def test_count(self):
        # 50 states + DC + Puerto Rico = 52
        assert len(STATES) == 52

    def test_sorted(self):
        assert STATES == sorted(STATES)

    def test_all_two_digit(self):
        for fips in STATES:
            assert len(fips) == 2
            assert fips.isdigit()

    def test_includes_dc_and_pr(self):
        assert '11' in STATES  # DC
        assert '72' in STATES  # PR


class TestFipsToName:
    def test_count_matches_states(self):
        assert len(STATE_FIPS_TO_NAME) == len(STATES)

    def test_sample_mappings(self):
        assert STATE_FIPS_TO_NAME['06'] == 'California'
        assert STATE_FIPS_TO_NAME['36'] == 'New York'
        assert STATE_FIPS_TO_NAME['11'] == 'District of Columbia'
        assert STATE_FIPS_TO_NAME['72'] == 'Puerto Rico'


class TestNameToFips:
    def test_includes_united_states(self):
        assert STATE_NAME_TO_FIPS['United States'] == '00'

    def test_roundtrip(self):
        for fips, name in STATE_FIPS_TO_NAME.items():
            assert STATE_NAME_TO_FIPS[name] == fips
