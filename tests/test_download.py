"""Tests for bls_stats.download — bulk CSV filtering and download."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl

from bls_stats.download import _filter_bulk_csv, download_qcew_bulk


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


def _make_zip_bytes(csv_content: bytes, csv_name: str = '2024_qtrly_singlefile.csv') -> bytes:
    """Build a ZIP archive in memory containing one CSV."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        zf.writestr(csv_name, csv_content)
    return buf.getvalue()


def _make_csv_bytes() -> bytes:
    """Build a minimal valid CSV for the download pipeline."""
    header = 'area_fips,own_code,industry_code,agglvl_code,year,qtr,month1_emplvl,month2_emplvl,month3_emplvl'
    row = 'US000,5,23,14,2024,1,100,200,300'
    return f'{header}\n{row}\n'.encode('utf-8')


class TestDownloadQcewBulk:
    def test_single_year(self, tmp_path):
        csv_bytes = _make_csv_bytes()
        zip_bytes = _make_zip_bytes(csv_bytes)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes

        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        out = tmp_path / 'out.parquet'
        with patch('bls_stats.download.get_with_retry', return_value=mock_response):
            result = download_qcew_bulk(
                start_year=2024, end_year=2024,
                output_path=out, client=mock_client,
            )

        assert result == out
        assert out.exists()
        df = pl.read_parquet(out)
        assert df.height == 1
        assert df['area_fips'][0] == 'US000'

    def test_multiple_years(self, tmp_path):
        csv_bytes = _make_csv_bytes()
        zip_bytes = _make_zip_bytes(csv_bytes)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes

        out = tmp_path / 'out.parquet'
        with patch('bls_stats.download.get_with_retry', return_value=mock_response):
            result = download_qcew_bulk(
                start_year=2023, end_year=2024,
                output_path=out, client=MagicMock(),
            )

        df = pl.read_parquet(result)
        assert df.height == 2  # one row per year

    def test_empty_zip_skipped(self, tmp_path):
        """A ZIP with no CSV inside should be skipped without error."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('readme.txt', 'no csv here')
        empty_zip_bytes = buf.getvalue()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = empty_zip_bytes

        out = tmp_path / 'out.parquet'
        with patch('bls_stats.download.get_with_retry', return_value=mock_response):
            result = download_qcew_bulk(
                start_year=2024, end_year=2024,
                output_path=out, client=MagicMock(),
            )

        assert result == out
        assert not out.exists()  # no data → no parquet written

    def test_creates_client_when_none(self, tmp_path):
        csv_bytes = _make_csv_bytes()
        zip_bytes = _make_zip_bytes(csv_bytes)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes

        mock_client = MagicMock()
        out = tmp_path / 'out.parquet'
        with (
            patch('bls_stats.download.create_client', return_value=mock_client) as mock_create,
            patch('bls_stats.download.get_with_retry', return_value=mock_response),
        ):
            download_qcew_bulk(
                start_year=2024, end_year=2024,
                output_path=out, client=None,
            )

        mock_create.assert_called_once()
        mock_client.close.assert_called_once()

    def test_does_not_close_provided_client(self, tmp_path):
        csv_bytes = _make_csv_bytes()
        zip_bytes = _make_zip_bytes(csv_bytes)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes

        mock_client = MagicMock()
        out = tmp_path / 'out.parquet'
        with patch('bls_stats.download.get_with_retry', return_value=mock_response):
            download_qcew_bulk(
                start_year=2024, end_year=2024,
                output_path=out, client=mock_client,
            )

        mock_client.close.assert_not_called()
