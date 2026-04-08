"""Tests for bls_stats.mapping — sector extraction and hierarchy aggregation."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from bls_stats.mapping import (
    aggregate_to_hierarchy,
    extract_government_employment,
    extract_sector_employment,
    map_bulk_to_ces,
    map_qcew_to_ces,
)


# ---------------------------------------------------------------------------
# extract_sector_employment
# ---------------------------------------------------------------------------


class TestExtractSectorEmployment:
    def test_basic_extraction(self, qcew_private_raw):
        result = extract_sector_employment(qcew_private_raw)
        assert len(result) > 0
        assert 'industry_code' in result.columns
        assert 'ref_date' in result.columns
        assert 'employment' in result.columns

    def test_maps_industry_codes(self, qcew_private_raw):
        result = extract_sector_employment(qcew_private_raw)
        codes = set(result['industry_code'].to_list())
        # 1021 → 23 (Construction), 1022 → 31 (Mfg), 1027 → 52 (Finance)
        assert codes == {'23', '31', '52'}

    def test_three_months_per_quarter(self, qcew_private_raw):
        result = extract_sector_employment(qcew_private_raw)
        # 3 industries × 3 months = 9 rows
        assert len(result) == 9

    def test_ref_date_calculation(self, qcew_private_raw):
        result = extract_sector_employment(qcew_private_raw)
        dates = sorted(set(result['ref_date'].to_list()))
        assert dates == [date(2024, 1, 12), date(2024, 2, 12), date(2024, 3, 12)]

    def test_q2_dates(self):
        df = pl.DataFrame({
            'industry_code': ['1021'],
            'year': [2024],
            'qtr': [2],
            'month1_emplvl': [100],
            'month2_emplvl': [200],
            'month3_emplvl': [300],
        })
        result = extract_sector_employment(df)
        dates = sorted(set(result['ref_date'].to_list()))
        assert dates == [date(2024, 4, 12), date(2024, 5, 12), date(2024, 6, 12)]

    def test_skips_zero_employment(self):
        df = pl.DataFrame({
            'industry_code': ['1021'],
            'year': [2024],
            'qtr': [1],
            'month1_emplvl': [0],
            'month2_emplvl': [100],
            'month3_emplvl': [-5],
        })
        result = extract_sector_employment(df)
        assert len(result) == 1
        assert result['employment'][0] == 100

    def test_skips_null_employment(self):
        df = pl.DataFrame({
            'industry_code': ['1021'],
            'year': [2024],
            'qtr': [1],
            'month1_emplvl': [None],
            'month2_emplvl': [100],
            'month3_emplvl': [200],
        })
        result = extract_sector_employment(df)
        assert len(result) == 2

    def test_unknown_industry_code_skipped(self):
        df = pl.DataFrame({
            'industry_code': ['UNKNOWN'],
            'year': [2024],
            'qtr': [1],
            'month1_emplvl': [100],
            'month2_emplvl': [200],
            'month3_emplvl': [300],
        })
        result = extract_sector_employment(df)
        assert len(result) == 0

    def test_missing_columns_returns_empty(self):
        df = pl.DataFrame({'foo': [1]})
        result = extract_sector_employment(df)
        assert len(result) == 0

    def test_empty_input_returns_empty(self):
        df = pl.DataFrame({
            'industry_code': [],
            'year': [],
            'qtr': [],
            'month1_emplvl': [],
            'month2_emplvl': [],
            'month3_emplvl': [],
        })
        result = extract_sector_employment(df)
        assert len(result) == 0

    def test_geographic_columns_from_input(self):
        df = pl.DataFrame({
            'industry_code': ['1021'],
            'year': [2024],
            'qtr': [1],
            'month1_emplvl': [100],
            'month2_emplvl': [200],
            'month3_emplvl': [300],
            'geographic_type': ['state'],
            'geographic_code': ['06'],
        })
        result = extract_sector_employment(df)
        assert result['geographic_type'][0] == 'state'
        assert result['geographic_code'][0] == '06'

    def test_geographic_defaults(self):
        df = pl.DataFrame({
            'industry_code': ['1021'],
            'year': [2024],
            'qtr': [1],
            'month1_emplvl': [100],
            'month2_emplvl': [200],
            'month3_emplvl': [300],
        })
        result = extract_sector_employment(df, geographic_type='state', geographic_code='36')
        assert result['geographic_type'][0] == 'state'
        assert result['geographic_code'][0] == '36'


# ---------------------------------------------------------------------------
# extract_government_employment
# ---------------------------------------------------------------------------


class TestExtractGovernmentEmployment:
    def test_basic_extraction(self, qcew_govt_raw):
        result = extract_government_employment(qcew_govt_raw)
        assert len(result) > 0

    def test_maps_ownership_to_sector(self, qcew_govt_raw):
        result = extract_government_employment(qcew_govt_raw)
        codes = set(result['industry_code'].to_list())
        assert codes == {'91', '92', '93'}

    def test_three_months_per_ownership(self, qcew_govt_raw):
        result = extract_government_employment(qcew_govt_raw)
        # 3 ownership codes × 3 months = 9 rows
        assert len(result) == 9

    def test_only_industry_code_10(self):
        df = pl.DataFrame({
            'own_code': ['1', '1'],
            'industry_code': ['10', '1012'],
            'year': [2024, 2024],
            'qtr': [1, 1],
            'month1_emplvl': [100, 999],
            'month2_emplvl': [200, 999],
            'month3_emplvl': [300, 999],
        })
        result = extract_government_employment(df)
        # Only industry_code='10' row should be extracted
        assert len(result) == 3
        assert all(c == '91' for c in result['industry_code'].to_list())

    def test_skips_private_ownership(self):
        df = pl.DataFrame({
            'own_code': ['5'],
            'industry_code': ['10'],
            'year': [2024],
            'qtr': [1],
            'month1_emplvl': [100],
            'month2_emplvl': [200],
            'month3_emplvl': [300],
        })
        result = extract_government_employment(df)
        assert len(result) == 0

    def test_missing_columns_returns_empty(self):
        df = pl.DataFrame({'foo': [1]})
        result = extract_government_employment(df)
        assert len(result) == 0

    def test_ref_dates(self, qcew_govt_raw):
        result = extract_government_employment(qcew_govt_raw)
        dates = sorted(set(result['ref_date'].to_list()))
        assert dates == [date(2024, 1, 12), date(2024, 2, 12), date(2024, 3, 12)]


# ---------------------------------------------------------------------------
# aggregate_to_hierarchy
# ---------------------------------------------------------------------------


class TestAggregateToHierarchy:
    def test_empty_input(self):
        result = aggregate_to_hierarchy(pl.DataFrame())
        assert len(result) == 0

    def test_produces_all_industry_types(self, sector_employment_df):
        result = aggregate_to_hierarchy(sector_employment_df)
        types = set(result['industry_type'].to_list())
        assert types == {'sector', 'supersector', 'domain'}

    def test_sector_rows_preserved(self, sector_employment_df):
        result = aggregate_to_hierarchy(sector_employment_df)
        sectors = result.filter(pl.col('industry_type') == 'sector')
        assert len(sectors) == len(sector_employment_df)

    def test_supersector_aggregation(self, sector_employment_df):
        result = aggregate_to_hierarchy(sector_employment_df)
        ss = result.filter(
            (pl.col('industry_type') == 'supersector')
            & (pl.col('industry_code') == '40')
        )
        # SS 40 = Wholesale(6000) + Retail(15000) + Transport(5500) + Utilities(500) = 27000
        assert len(ss) == 1
        assert ss['employment'][0] == 27_000

    def test_manufacturing_supersector(self, sector_employment_df):
        result = aggregate_to_hierarchy(sector_employment_df)
        ss30 = result.filter(
            (pl.col('industry_type') == 'supersector')
            & (pl.col('industry_code') == '30')
        )
        # SS 30 = Durable(10000) + Nondurable(5000) = 15000
        # But hierarchy only has sector '31' in SS 30, so it depends on components
        assert len(ss30) == 1

    def test_government_supersector(self, sector_employment_df):
        result = aggregate_to_hierarchy(sector_employment_df)
        ss90 = result.filter(
            (pl.col('industry_type') == 'supersector')
            & (pl.col('industry_code') == '90')
        )
        assert len(ss90) == 1
        # 91(2800) + 92(5200) + 93(14000) = 22000
        assert ss90['employment'][0] == 22_000

    def test_domain_total_nonfarm(self, sector_employment_df):
        result = aggregate_to_hierarchy(sector_employment_df)
        d00 = result.filter(
            (pl.col('industry_type') == 'domain')
            & (pl.col('industry_code') == '00')
        )
        assert len(d00) == 1
        # Sum of all supersectors (including govt)
        assert d00['employment'][0] > 0

    def test_domain_goods_producing(self, sector_employment_df):
        result = aggregate_to_hierarchy(sector_employment_df)
        d06 = result.filter(
            (pl.col('industry_type') == 'domain')
            & (pl.col('industry_code') == '06')
        )
        assert len(d06) == 1

    def test_total_private_excludes_govt(self, sector_employment_df):
        result = aggregate_to_hierarchy(sector_employment_df)
        d05 = result.filter(
            (pl.col('industry_type') == 'domain')
            & (pl.col('industry_code') == '05')
        )
        d00 = result.filter(
            (pl.col('industry_type') == 'domain')
            & (pl.col('industry_code') == '00')
        )
        ss90 = result.filter(
            (pl.col('industry_type') == 'supersector')
            & (pl.col('industry_code') == '90')
        )
        # Total nonfarm = Total private + Government
        assert d00['employment'][0] == d05['employment'][0] + ss90['employment'][0]


# ---------------------------------------------------------------------------
# map_qcew_to_ces
# ---------------------------------------------------------------------------


class TestMapQcewToCes:
    def test_empty_input(self):
        result = map_qcew_to_ces(pl.DataFrame())
        assert len(result) == 0

    def test_private_only(self, qcew_private_raw):
        result = map_qcew_to_ces(qcew_private_raw, include_government=False)
        assert len(result) > 0
        types = set(result['industry_type'].to_list())
        assert 'sector' in types

    def test_with_government(self, qcew_combined_raw):
        result = map_qcew_to_ces(qcew_combined_raw, include_government=True)
        codes = set(result['industry_code'].to_list())
        # Should have government sectors
        assert '91' in codes or '92' in codes or '93' in codes

    def test_without_government(self, qcew_combined_raw):
        result = map_qcew_to_ces(qcew_combined_raw, include_government=False)
        codes = set(result['industry_code'].to_list())
        for govt in ['91', '92', '93']:
            assert govt not in codes

    def test_produces_hierarchy(self, qcew_private_raw):
        result = map_qcew_to_ces(qcew_private_raw)
        types = set(result['industry_type'].to_list())
        assert 'sector' in types
        assert 'supersector' in types
        assert 'domain' in types


# ---------------------------------------------------------------------------
# map_bulk_to_ces
# ---------------------------------------------------------------------------


class TestMapBulkToCes:
    def test_missing_file_returns_empty(self, tmp_path):
        result = map_bulk_to_ces(tmp_path / 'nonexistent.parquet')
        assert len(result) == 0

    def test_basic_bulk_mapping(self, tmp_path):
        """Build a minimal bulk parquet and verify the mapping pipeline."""
        rows = []
        # Stream 1: total (own_code=0, agglvl=10)
        rows.append({
            'area_fips': 'US000', 'own_code': '0', 'industry_code': '10',
            'agglvl_code': '10', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '150000', 'month2_emplvl': '151000',
            'month3_emplvl': '152000',
        })
        # Stream 2: private 2-digit (own_code=5, agglvl=14)
        for naics, emp in [('23', 8000), ('42', 6000), ('51', 3000)]:
            rows.append({
                'area_fips': 'US000', 'own_code': '5', 'industry_code': naics,
                'agglvl_code': '14', 'year': '2024', 'qtr': '1',
                'month1_emplvl': str(emp), 'month2_emplvl': str(emp + 100),
                'month3_emplvl': str(emp + 200),
            })
        # Stream 3: government (own_code 1/2/3, agglvl=11, industry_code=10)
        for own, emp in [('1', 2800), ('2', 5200), ('3', 14000)]:
            rows.append({
                'area_fips': 'US000', 'own_code': own, 'industry_code': '10',
                'agglvl_code': '11', 'year': '2024', 'qtr': '1',
                'month1_emplvl': str(emp), 'month2_emplvl': str(emp + 100),
                'month3_emplvl': str(emp + 200),
            })
        # Stream 4: manufacturing 3-digit (own_code=5, agglvl=15)
        for naics3, emp in [('321', 1000), ('311', 2000)]:
            rows.append({
                'area_fips': 'US000', 'own_code': '5', 'industry_code': naics3,
                'agglvl_code': '15', 'year': '2024', 'qtr': '1',
                'month1_emplvl': str(emp), 'month2_emplvl': str(emp + 50),
                'month3_emplvl': str(emp + 100),
            })

        df = pl.DataFrame(rows)
        path = tmp_path / 'bulk.parquet'
        df.write_parquet(path)

        result = map_bulk_to_ces(path)
        assert len(result) > 0
        types = set(result['industry_type'].to_list())
        assert 'sector' in types
        assert 'supersector' in types
        assert 'domain' in types

    def test_bulk_employment_in_thousands(self, tmp_path):
        """Verify that bulk output divides employment by 1000."""
        rows = []
        rows.append({
            'area_fips': 'US000', 'own_code': '5', 'industry_code': '23',
            'agglvl_code': '14', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '100000', 'month2_emplvl': '100000',
            'month3_emplvl': '100000',
        })
        # Need govt too to avoid concat errors
        rows.append({
            'area_fips': 'US000', 'own_code': '1', 'industry_code': '10',
            'agglvl_code': '11', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '50000', 'month2_emplvl': '50000',
            'month3_emplvl': '50000',
        })
        # Manufacturing required for concat
        rows.append({
            'area_fips': 'US000', 'own_code': '5', 'industry_code': '321',
            'agglvl_code': '15', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '10000', 'month2_emplvl': '10000',
            'month3_emplvl': '10000',
        })

        df = pl.DataFrame(rows)
        path = tmp_path / 'bulk.parquet'
        df.write_parquet(path)

        result = map_bulk_to_ces(path)
        construction = result.filter(
            (pl.col('industry_code') == '23') & (pl.col('industry_type') == 'sector')
        )
        assert len(construction) > 0
        # 100000 / 1000 = 100.0
        assert construction['employment'][0] == pytest.approx(100.0)

    def test_bulk_state_level(self, tmp_path):
        """Verify state-level FIPS mapping."""
        rows = []
        rows.append({
            'area_fips': '06000', 'own_code': '5', 'industry_code': '23',
            'agglvl_code': '54', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '50000', 'month2_emplvl': '50000',
            'month3_emplvl': '50000',
        })
        rows.append({
            'area_fips': '06000', 'own_code': '1', 'industry_code': '10',
            'agglvl_code': '51', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '10000', 'month2_emplvl': '10000',
            'month3_emplvl': '10000',
        })
        rows.append({
            'area_fips': '06000', 'own_code': '5', 'industry_code': '321',
            'agglvl_code': '55', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '5000', 'month2_emplvl': '5000',
            'month3_emplvl': '5000',
        })

        df = pl.DataFrame(rows)
        path = tmp_path / 'bulk.parquet'
        df.write_parquet(path)

        result = map_bulk_to_ces(path)
        assert len(result) > 0
        assert 'state' in result['geographic_type'].to_list()
        assert '06' in result['geographic_code'].to_list()
