"""Tests for bls_stats_aggregation.qcew.industry hierarchy and mappings."""

import pytest

from bls_stats_aggregation.qcew.industry import (
    CES_SECTOR_TO_NAICS,
    DOMAIN_DEFINITIONS,
    GOVT_OWNERSHIP_TO_SECTOR,
    INDUSTRY_HIERARCHY,
    INDUSTRY_MAP,
    IndustryEntry,
    NAICS3_TO_MFG_SECTOR,
    SINGLE_SECTOR_SUPERSECTORS,
    get_domain_supersectors,
    get_sector_codes,
    get_supersector_codes,
    get_supersector_components,
)


class TestIndustryHierarchy:
    def test_is_lazy_frame(self):
        import polars as pl
        assert isinstance(INDUSTRY_HIERARCHY, pl.LazyFrame)

    def test_collects_without_error(self):
        df = INDUSTRY_HIERARCHY.collect()
        assert len(df) > 0

    def test_expected_columns(self):
        df = INDUSTRY_HIERARCHY.collect()
        expected = {
            'sector_code', 'sector_title', 'supersector_code',
            'supersector_title', 'domain_code', 'domain_title',
        }
        assert set(df.columns) == expected

    def test_all_sectors_have_supersector(self):
        df = INDUSTRY_HIERARCHY.collect()
        assert df['supersector_code'].null_count() == 0

    def test_domain_codes_are_g_or_s(self):
        df = INDUSTRY_HIERARCHY.collect()
        assert set(df['domain_code'].to_list()) == {'G', 'S'}

    def test_manufacturing_is_goods(self):
        df = INDUSTRY_HIERARCHY.collect()
        mfg = df.filter(df['sector_code'] == '31')
        assert len(mfg) == 1
        assert mfg['domain_code'][0] == 'G'
        assert mfg['supersector_code'][0] == '30'


class TestGetSectorCodes:
    def test_returns_list(self):
        codes = get_sector_codes()
        assert isinstance(codes, list)

    def test_sorted(self):
        codes = get_sector_codes()
        assert codes == sorted(codes)

    def test_expected_sectors(self):
        codes = get_sector_codes()
        for expected in ['21', '23', '31', '42', '44', '51', '72', '81']:
            assert expected in codes

    def test_no_government_sectors(self):
        codes = get_sector_codes()
        for govt in ['91', '92', '93']:
            assert govt not in codes


class TestGetSupersectorCodes:
    def test_expected_codes(self):
        codes = get_supersector_codes()
        expected = ['10', '20', '30', '40', '50', '55', '60', '65', '70', '80']
        assert codes == expected


class TestGetSupersectorComponents:
    def test_includes_government(self):
        comps = get_supersector_components()
        assert '90' in comps
        assert comps['90'] == ['91', '92', '93']

    def test_trade_transportation_utilities(self):
        comps = get_supersector_components()
        assert '40' in comps
        assert set(comps['40']) == {'22', '42', '44', '48'}

    def test_manufacturing_single_sector(self):
        comps = get_supersector_components()
        assert comps['30'] == ['31']

    def test_all_values_sorted(self):
        comps = get_supersector_components()
        for sectors in comps.values():
            assert sectors == sorted(sectors)

    def test_keys_sorted(self):
        comps = get_supersector_components()
        keys = list(comps.keys())
        assert keys == sorted(keys)


class TestDomainDefinitions:
    def test_all_domain_codes_present(self):
        assert set(DOMAIN_DEFINITIONS.keys()) == {'00', '05', '06', '07', '08'}

    def test_total_nonfarm_includes_govt(self):
        assert DOMAIN_DEFINITIONS['00']['includes_govt'] is True

    def test_total_private_excludes_govt(self):
        assert DOMAIN_DEFINITIONS['05']['includes_govt'] is False

    def test_goods_producing_is_goods_only(self):
        assert DOMAIN_DEFINITIONS['06']['goods_only'] is True


class TestGetDomainSupersectors:
    def test_total_nonfarm_includes_90(self):
        ss = get_domain_supersectors('00')
        assert '90' in ss

    def test_total_private_excludes_90(self):
        ss = get_domain_supersectors('05')
        assert '90' not in ss

    def test_goods_producing(self):
        ss = get_domain_supersectors('06')
        assert set(ss) == {'10', '20', '30'}

    def test_service_providing_includes_90(self):
        ss = get_domain_supersectors('07')
        assert '90' in ss
        assert '10' not in ss  # no goods

    def test_private_service_providing(self):
        ss = get_domain_supersectors('08')
        assert '90' not in ss
        assert '10' not in ss

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match='Unknown domain code'):
            get_domain_supersectors('99')


class TestGovtOwnershipToSector:
    def test_mapping(self):
        assert GOVT_OWNERSHIP_TO_SECTOR == {'1': '91', '2': '92', '3': '93'}


class TestNaics3ToMfgSector:
    def test_durable_goods(self):
        durables = [k for k, v in NAICS3_TO_MFG_SECTOR.items() if v == '31']
        assert '321' in durables  # Wood
        assert '336' in durables  # Transportation equipment

    def test_nondurable_goods(self):
        nondurables = [k for k, v in NAICS3_TO_MFG_SECTOR.items() if v == '32']
        assert '311' in nondurables  # Food
        assert '325' in nondurables  # Chemicals

    def test_all_values_31_or_32(self):
        for v in NAICS3_TO_MFG_SECTOR.values():
            assert v in ('31', '32')


class TestCesSectorToNaics:
    def test_wholesale_remapping(self):
        assert CES_SECTOR_TO_NAICS['41'] == '42'

    def test_retail_remapping(self):
        assert CES_SECTOR_TO_NAICS['42'] == '44'

    def test_transport_remapping(self):
        assert CES_SECTOR_TO_NAICS['43'] == '48'

    def test_identity_mappings(self):
        for code in ['21', '52', '61', '71']:
            assert CES_SECTOR_TO_NAICS[code] == code


class TestSingleSectorSupersectors:
    def test_expected(self):
        assert SINGLE_SECTOR_SUPERSECTORS == {'20': '23', '50': '51', '80': '81'}


class TestIndustryEntry:
    def test_frozen(self):
        e = IndustryEntry('00', 'domain', 'Total', '000000', '')
        with pytest.raises(AttributeError):
            e.industry_code = '99'

    def test_fields(self):
        e = IndustryEntry('10', 'supersector', 'Mining', '100000', '')
        assert e.industry_code == '10'
        assert e.industry_type == 'supersector'
        assert e.ces_code == '100000'


class TestIndustryMap:
    def test_is_list(self):
        assert isinstance(INDUSTRY_MAP, list)
        assert all(isinstance(e, IndustryEntry) for e in INDUSTRY_MAP)

    def test_has_all_three_types(self):
        types = {e.industry_type for e in INDUSTRY_MAP}
        assert types == {'domain', 'supersector', 'sector'}

    def test_domain_entries(self):
        domains = [e for e in INDUSTRY_MAP if e.industry_type == 'domain']
        assert len(domains) == 5

    def test_supersector_entries(self):
        ss = [e for e in INDUSTRY_MAP if e.industry_type == 'supersector']
        assert len(ss) == 11  # 10 private + government

    def test_sector_qcew_naics_populated(self):
        sectors = [e for e in INDUSTRY_MAP if e.industry_type == 'sector']
        for s in sectors:
            assert s.qcew_naics != '', f'{s.industry_code} missing qcew_naics'
