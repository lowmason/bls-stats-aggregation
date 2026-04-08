"""Tests for qcew_stats.download — bulk CSV filtering."""

from __future__ import annotations

import polars as pl

from qcew_stats.download import _filter_bulk_csv


class TestFilterBulkCsv:
    def _make_csv(self, rows: list[dict]) -> bytes:
        header = ','.join(rows[0].keys())
        lines = [header]
        for r in rows:
            lines.append(','.join(str(v) for v in r.values()))
        return '\n'.join(lines).encode('utf-8')

    def test_keeps_national_rows(self):
        csv = self._make_csv([{
            'area_fips': 'US000', 'own_code': '5', 'industry_code': '23',
            'agglvl_code': '14', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '100', 'month2_emplvl': '200', 'month3_emplvl': '300',
        }])
        df = _filter_bulk_csv(csv)
        assert len(df) == 1

    def test_keeps_state_rows(self):
        csv = self._make_csv([{
            'area_fips': '06000', 'own_code': '5', 'industry_code': '23',
            'agglvl_code': '54', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '100', 'month2_emplvl': '200', 'month3_emplvl': '300',
        }])
        df = _filter_bulk_csv(csv)
        assert len(df) == 1

    def test_filters_county_rows(self):
        csv = self._make_csv([
            {
                'area_fips': 'US000', 'own_code': '5', 'industry_code': '23',
                'agglvl_code': '14', 'year': '2024', 'qtr': '1',
                'month1_emplvl': '100', 'month2_emplvl': '200', 'month3_emplvl': '300',
            },
            {
                'area_fips': '06037', 'own_code': '5', 'industry_code': '23',
                'agglvl_code': '74', 'year': '2024', 'qtr': '1',
                'month1_emplvl': '50', 'month2_emplvl': '60', 'month3_emplvl': '70',
            },
        ])
        df = _filter_bulk_csv(csv)
        assert len(df) == 1
        assert df['area_fips'][0] == 'US000'

    def test_filters_unwanted_agglvl(self):
        csv = self._make_csv([{
            'area_fips': 'US000', 'own_code': '5', 'industry_code': '23',
            'agglvl_code': '99', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '100', 'month2_emplvl': '200', 'month3_emplvl': '300',
        }])
        df = _filter_bulk_csv(csv)
        assert len(df) == 0

    def test_filters_unwanted_ownership(self):
        csv = self._make_csv([{
            'area_fips': 'US000', 'own_code': '9', 'industry_code': '10',
            'agglvl_code': '10', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '100', 'month2_emplvl': '200', 'month3_emplvl': '300',
        }])
        df = _filter_bulk_csv(csv)
        assert len(df) == 0

    def test_keeps_government_ownership(self):
        rows = []
        for own in ['0', '1', '2', '3', '5']:
            rows.append({
                'area_fips': 'US000', 'own_code': own, 'industry_code': '10',
                'agglvl_code': '10', 'year': '2024', 'qtr': '1',
                'month1_emplvl': '100', 'month2_emplvl': '200', 'month3_emplvl': '300',
            })
        csv = self._make_csv(rows)
        df = _filter_bulk_csv(csv)
        assert len(df) == 5

    def test_selects_expected_columns(self):
        csv = self._make_csv([{
            'area_fips': 'US000', 'own_code': '5', 'industry_code': '23',
            'agglvl_code': '14', 'year': '2024', 'qtr': '1',
            'month1_emplvl': '100', 'month2_emplvl': '200', 'month3_emplvl': '300',
            'extra_col': 'ignored',
        }])
        df = _filter_bulk_csv(csv)
        assert 'extra_col' not in df.columns
        assert 'area_fips' in df.columns
        assert 'month1_emplvl' in df.columns
