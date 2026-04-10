"""Tests for bls_stats.geography constants."""

from bls_stats.geography import (
    CENSUS_DIVISIONS,
    CENSUS_REGIONS,
    DIVISION_TO_REGION,
    STATE_FIPS_TO_DIVISION,
    STATE_FIPS_TO_NAME,
    STATE_FIPS_TO_REGION,
    STATE_NAME_TO_FIPS,
    STATES,
)


class TestStates:
    def test_count(self):
        # 50 states + DC + Puerto Rico + Virgin Islands = 53
        assert len(STATES) == 53

    def test_sorted(self):
        assert STATES == sorted(STATES)

    def test_all_two_digit(self):
        for fips in STATES:
            assert len(fips) == 2
            assert fips.isdigit()

    def test_includes_dc_pr_vi(self):
        assert '11' in STATES  # DC
        assert '72' in STATES  # PR
        assert '78' in STATES  # VI


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


class TestCensusRegions:
    def test_four_regions(self):
        assert len(CENSUS_REGIONS) == 4

    def test_region_names(self):
        assert CENSUS_REGIONS['1'] == 'Northeast'
        assert CENSUS_REGIONS['2'] == 'Midwest'
        assert CENSUS_REGIONS['3'] == 'South'
        assert CENSUS_REGIONS['4'] == 'West'


class TestCensusDivisions:
    def test_nine_divisions(self):
        assert len(CENSUS_DIVISIONS) == 9

    def test_sample_division_names(self):
        assert CENSUS_DIVISIONS['1'] == 'New England'
        assert CENSUS_DIVISIONS['5'] == 'South Atlantic'
        assert CENSUS_DIVISIONS['9'] == 'Pacific'


class TestDivisionToRegion:
    def test_all_divisions_mapped(self):
        assert set(DIVISION_TO_REGION.keys()) == set(CENSUS_DIVISIONS.keys())

    def test_northeast_divisions(self):
        assert DIVISION_TO_REGION['1'] == '1'
        assert DIVISION_TO_REGION['2'] == '1'

    def test_all_values_are_valid_regions(self):
        for region in DIVISION_TO_REGION.values():
            assert region in CENSUS_REGIONS


class TestStateFipsToDivision:
    def test_covers_50_states_plus_dc(self):
        # 50 states + DC = 51 (Puerto Rico excluded)
        assert len(STATE_FIPS_TO_DIVISION) == 51

    def test_territories_excluded(self):
        assert '72' not in STATE_FIPS_TO_DIVISION  # PR
        assert '78' not in STATE_FIPS_TO_DIVISION  # VI

    def test_sample_assignments(self):
        assert STATE_FIPS_TO_DIVISION['06'] == '9'  # California → Pacific
        assert STATE_FIPS_TO_DIVISION['36'] == '2'  # New York → Middle Atlantic
        assert STATE_FIPS_TO_DIVISION['48'] == '7'  # Texas → West South Central
        assert STATE_FIPS_TO_DIVISION['17'] == '3'  # Illinois → East North Central
        assert STATE_FIPS_TO_DIVISION['11'] == '5'  # DC → South Atlantic

    def test_all_values_are_valid_divisions(self):
        for div in STATE_FIPS_TO_DIVISION.values():
            assert div in CENSUS_DIVISIONS

    def test_all_states_except_territories_are_mapped(self):
        territories = {'72', '78'}  # PR, VI
        for fips in STATES:
            if fips in territories:
                continue
            assert fips in STATE_FIPS_TO_DIVISION


class TestStateFipsToRegion:
    def test_same_keys_as_division_map(self):
        assert set(STATE_FIPS_TO_REGION.keys()) == set(STATE_FIPS_TO_DIVISION.keys())

    def test_derived_correctly(self):
        for fips, region in STATE_FIPS_TO_REGION.items():
            assert region == DIVISION_TO_REGION[STATE_FIPS_TO_DIVISION[fips]]

    def test_sample_assignments(self):
        assert STATE_FIPS_TO_REGION['06'] == '4'  # California → West
        assert STATE_FIPS_TO_REGION['36'] == '1'  # New York → Northeast
        assert STATE_FIPS_TO_REGION['48'] == '3'  # Texas → South
