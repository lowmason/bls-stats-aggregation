"""Tests for bls_stats.qcew.mapping — bulk QCEW-to-CES mapping."""

from __future__ import annotations

import polars as pl
import pytest

from bls_stats.qcew.mapping import map_bulk_to_ces


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
        rows.append({
            'area_fips': 'US000', 'own_code': '1', 'industry_code': '10',
            'agglvl_code': '11', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '50000', 'month2_emplvl': '50000',
            'month3_emplvl': '50000',
        })
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

    def test_bulk_division_and_region_aggregation(self, tmp_path):
        """Verify Census division/region rows are produced from states."""
        rows = []
        # Two states in the same division (Pacific = division 9):
        # California (06) and Oregon (41)
        for fips in ['06000', '41000']:
            rows.append({
                'area_fips': fips, 'own_code': '5', 'industry_code': '23',
                'agglvl_code': '54', 'year': '2024', 'qtr': '1',
                'month1_emplvl': '30000', 'month2_emplvl': '30000',
                'month3_emplvl': '30000',
            })
            rows.append({
                'area_fips': fips, 'own_code': '1', 'industry_code': '10',
                'agglvl_code': '51', 'year': '2024', 'qtr': '1',
                'month1_emplvl': '10000', 'month2_emplvl': '10000',
                'month3_emplvl': '10000',
            })
            rows.append({
                'area_fips': fips, 'own_code': '5', 'industry_code': '321',
                'agglvl_code': '55', 'year': '2024', 'qtr': '1',
                'month1_emplvl': '5000', 'month2_emplvl': '5000',
                'month3_emplvl': '5000',
            })

        df = pl.DataFrame(rows)
        path = tmp_path / 'bulk.parquet'
        df.write_parquet(path)

        result = map_bulk_to_ces(path)
        geo_types = set(result['geographic_type'].to_list())
        assert 'division' in geo_types
        assert 'region' in geo_types

        # Division 9 (Pacific) construction sector should sum the two states
        div9_constr = result.filter(
            (pl.col('geographic_type') == 'division')
            & (pl.col('geographic_code') == '9')
            & (pl.col('industry_code') == '23')
            & (pl.col('industry_type') == 'sector')
        )
        assert len(div9_constr) == 3  # 3 months
        assert div9_constr['employment'][0] == pytest.approx(60.0)

        # Region 4 (West) should equal division 9 here (only Pacific states)
        reg4_constr = result.filter(
            (pl.col('geographic_type') == 'region')
            & (pl.col('geographic_code') == '4')
            & (pl.col('industry_code') == '23')
            & (pl.col('industry_type') == 'sector')
        )
        assert len(reg4_constr) == 3
        assert reg4_constr['employment'][0] == pytest.approx(60.0)

    def test_puerto_rico_excluded_from_divisions(self, tmp_path):
        """Puerto Rico state rows should not appear in divisions or regions."""
        rows = [{
            'area_fips': '72000', 'own_code': '5', 'industry_code': '23',
            'agglvl_code': '54', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '10000', 'month2_emplvl': '10000',
            'month3_emplvl': '10000',
        }]
        df = pl.DataFrame(rows)
        path = tmp_path / 'bulk.parquet'
        df.write_parquet(path)

        result = map_bulk_to_ces(path)
        assert 'state' in result['geographic_type'].to_list()
        div_rows = result.filter(pl.col('geographic_type') == 'division')
        reg_rows = result.filter(pl.col('geographic_type') == 'region')
        assert len(div_rows) == 0
        assert len(reg_rows) == 0
