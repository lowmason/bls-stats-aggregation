"""Tests for bls_stats.download — CSV filtering and fetch functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from bls_stats.download import (
    QCEW_INDUSTRY_CODES,
    _filter_bulk_csv,
    fetch_qcew,
    fetch_qcew_with_geography,
)


class TestQcewIndustryCodes:
    def test_has_total(self):
        assert '10' in QCEW_INDUSTRY_CODES

    def test_has_all_naics_sectors(self):
        expected = [
            '1012', '1013', '1021', '1022', '1023', '1024', '1025',
            '1026', '1027', '1028', '1029', '102A', '102B', '102C',
            '102D', '102E', '102F', '102G',
        ]
        for code in expected:
            assert code in QCEW_INDUSTRY_CODES


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
        # County-level row should be filtered out
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


class TestFetchQcew:
    def test_empty_result_returns_empty_df(self):
        mock_client = MagicMock()
        mock_client.get_qcew_csv.return_value = pl.DataFrame()
        result = fetch_qcew([2024], client=mock_client)
        assert len(result) == 0

    def test_filters_by_area_and_ownership(self):
        mock_client = MagicMock()
        response_df = pl.DataFrame({
            'area_fips': ['US000', 'US000', '06000'],
            'own_code': ['5', '3', '5'],
            'industry_code': ['10', '10', '10'],
            'year': [2024, 2024, 2024],
            'qtr': [1, 1, 1],
            'month1_emplvl': [100, 200, 300],
        })
        mock_client.get_qcew_csv.return_value = response_df
        result = fetch_qcew(
            [2024], quarters=[1], industries=['10'],
            area_fips='US000', ownership_code='5',
            client=mock_client,
        )
        # Should only keep US000 + own_code=5
        assert len(result) == 1

    def test_handles_fetch_errors_gracefully(self):
        mock_client = MagicMock()
        mock_client.get_qcew_csv.side_effect = Exception('network error')
        result = fetch_qcew([2024], quarters=[1], industries=['10'], client=mock_client)
        assert len(result) == 0

    def test_creates_and_closes_client_when_none(self):
        with patch('bls_stats.download.BLSHttpClient') as MockClient:
            mock_instance = MagicMock()
            mock_instance.get_qcew_csv.return_value = pl.DataFrame()
            MockClient.return_value = mock_instance
            fetch_qcew([2024], quarters=[1], industries=['10'])
            mock_instance.close.assert_called_once()

    def test_does_not_close_provided_client(self):
        mock_client = MagicMock()
        mock_client.get_qcew_csv.return_value = pl.DataFrame()
        fetch_qcew([2024], quarters=[1], industries=['10'], client=mock_client)
        mock_client.close.assert_not_called()


class TestFetchQcewWithGeography:
    def test_empty_result(self):
        mock_client = MagicMock()
        mock_client.get_qcew_csv.return_value = pl.DataFrame()
        result = fetch_qcew_with_geography([2024], client=mock_client)
        assert len(result) == 0

    def test_adds_geographic_columns_national(self):
        mock_client = MagicMock()
        response_df = pl.DataFrame({
            'area_fips': ['US000'],
            'own_code': ['5'],
            'industry_code': ['10'],
            'year': [2024],
            'qtr': [1],
            'month1_emplvl': [100],
        })
        mock_client.get_qcew_csv.return_value = response_df
        result = fetch_qcew_with_geography(
            [2024], quarters=[1], industries=['10'],
            include_states=False, client=mock_client,
        )
        assert 'geographic_type' in result.columns
        assert result['geographic_type'][0] == 'national'
        assert result['geographic_code'][0] == '00'

    def test_adds_geographic_columns_state(self):
        mock_client = MagicMock()
        response_df = pl.DataFrame({
            'area_fips': ['06000'],
            'own_code': ['5'],
            'industry_code': ['10'],
            'year': [2024],
            'qtr': [1],
            'month1_emplvl': [100],
        })
        mock_client.get_qcew_csv.return_value = response_df
        result = fetch_qcew_with_geography(
            [2024], quarters=[1], industries=['10'],
            include_national=False, client=mock_client,
        )
        assert 'geographic_type' in result.columns
        assert result['geographic_type'][0] == 'state'
        assert result['geographic_code'][0] == '06'

    def test_filters_by_ownership_codes(self):
        mock_client = MagicMock()
        response_df = pl.DataFrame({
            'area_fips': ['US000', 'US000', 'US000'],
            'own_code': ['5', '1', '9'],
            'industry_code': ['10', '10', '10'],
            'year': [2024, 2024, 2024],
            'qtr': [1, 1, 1],
            'month1_emplvl': [100, 200, 300],
        })
        mock_client.get_qcew_csv.return_value = response_df
        result = fetch_qcew_with_geography(
            [2024], quarters=[1], industries=['10'],
            ownership_codes=['5', '1'],
            include_states=False, client=mock_client,
        )
        own_codes = set(result['own_code'].to_list())
        assert '9' not in own_codes

    def test_creates_and_closes_client_when_none(self):
        with patch('bls_stats.download.BLSHttpClient') as MockClient:
            mock_instance = MagicMock()
            mock_instance.get_qcew_csv.return_value = pl.DataFrame()
            MockClient.return_value = mock_instance
            fetch_qcew_with_geography([2024], quarters=[1], industries=['10'])
            mock_instance.close.assert_called_once()
