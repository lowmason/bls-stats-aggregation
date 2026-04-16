"""Tests for the JOLTS subpackage."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from bls_stats_aggregation.jolts.industry import (
    JOLTS_DATA_ELEMENTS,
    JOLTS_TO_CES,
    _parse_series_columns,
)
from bls_stats_aggregation.jolts.mapping import map_jolts_to_ces


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_series_id(
    seasonal: str = "S",
    industry: str = "100000",
    state: str = "00",
    area: str = "00000",
    sizeclass: str = "00",
    dataelement: str = "HI",
    ratelevel: str = "L",
) -> str:
    """Build a 21-character JOLTS series ID from components."""
    return f"JT{seasonal}{industry}{state}{area}{sizeclass}{dataelement}{ratelevel}"


def _make_jolts_parquet(
    path: Path,
    rows: list[dict],
) -> Path:
    """Write a minimal JOLTS parquet file from row dicts.

    Each dict should have keys: state_code, industry_code,
    dataelement_code, ratelevel_code, ref_date, value.
    state_code defaults to '00' (national) if omitted.
    """
    for row in rows:
        row.setdefault("state_code", "00")
    df = pl.DataFrame(
        rows,
        schema={
            "state_code": pl.Utf8,
            "industry_code": pl.Utf8,
            "dataelement_code": pl.Utf8,
            "ratelevel_code": pl.Utf8,
            "ref_date": pl.Date,
            "value": pl.Float64,
        },
    )
    df.write_parquet(path)
    return path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestJoltsConstants:
    def test_jolts_to_ces_has_12_entries(self):
        assert len(JOLTS_TO_CES) == 12

    def test_jolts_to_ces_values_are_tuples(self):
        for key, val in JOLTS_TO_CES.items():
            assert isinstance(val, tuple), f"{key}: expected tuple, got {type(val)}"
            assert len(val) == 2

    def test_jolts_to_ces_industry_types(self):
        types = {v[1] for v in JOLTS_TO_CES.values()}
        assert types == {"domain", "national", "supersector"}

    def test_jolts_to_ces_total_private(self):
        assert JOLTS_TO_CES["100000"] == ("05", "domain")

    def test_jolts_to_ces_has_10_supersectors(self):
        ss = [v for v in JOLTS_TO_CES.values() if v[1] == "supersector"]
        assert len(ss) == 10

    def test_data_elements_has_2_entries(self):
        assert len(JOLTS_DATA_ELEMENTS) == 2

    def test_data_elements_codes(self):
        assert set(JOLTS_DATA_ELEMENTS.keys()) == {"HI", "TS"}

    def test_data_elements_values(self):
        assert JOLTS_DATA_ELEMENTS["HI"] == "hires"
        assert JOLTS_DATA_ELEMENTS["TS"] == "total_separations"


# ---------------------------------------------------------------------------
# Series ID parsing
# ---------------------------------------------------------------------------


class TestParseSeriesColumns:
    def test_parse_extracts_all_fields(self):
        sid = _build_series_id(
            seasonal="S",
            industry="300000",
            state="00",
            area="00000",
            sizeclass="00",
            dataelement="TS",
            ratelevel="R",
        )
        df = pl.DataFrame({"series_id": [sid]})
        result = _parse_series_columns(df)

        row = result.row(0, named=True)
        assert row["seasonal"] == "S"
        assert row["industry_code"] == "300000"
        assert row["state_code"] == "00"
        assert row["area_code"] == "00000"
        assert row["sizeclass_code"] == "00"
        assert row["dataelement_code"] == "TS"
        assert row["ratelevel_code"] == "R"

    def test_parse_with_nondefault_values(self):
        sid = _build_series_id(
            seasonal="U",
            industry="510099",
            state="06",
            area="12345",
            sizeclass="01",
            dataelement="HI",
            ratelevel="L",
        )
        df = pl.DataFrame({"series_id": [sid]})
        result = _parse_series_columns(df)

        row = result.row(0, named=True)
        assert row["seasonal"] == "U"
        assert row["industry_code"] == "510099"
        assert row["state_code"] == "06"
        assert row["area_code"] == "12345"
        assert row["sizeclass_code"] == "01"
        assert row["dataelement_code"] == "HI"
        assert row["ratelevel_code"] == "L"


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


class TestMapJoltsToCes:
    def test_missing_file_returns_empty(self, tmp_path):
        result = map_jolts_to_ces(tmp_path / "nonexistent.parquet")
        assert isinstance(result, pl.DataFrame)
        assert result.height == 0

    def test_basic_mapping(self, tmp_path):
        from datetime import date

        path = _make_jolts_parquet(
            tmp_path / "jolts.parquet",
            [
                {
                    "industry_code": "100000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": date(2024, 1, 1),
                    "value": 5000.0,
                },
            ],
        )
        result = map_jolts_to_ces(path)

        assert result.height >= 1
        row = result.filter(
            (pl.col("industry_code") == "05")
            & (pl.col("rate_level") == "level")
            & (pl.col("data_element") == "entries")
        )
        assert row.height == 1
        assert row["industry_type"][0] == "domain"
        assert row["value"][0] == pytest.approx(5000.0)

    def test_output_schema(self, tmp_path):
        from datetime import date

        path = _make_jolts_parquet(
            tmp_path / "jolts.parquet",
            [
                {
                    "industry_code": "300000",
                    "dataelement_code": "TS",
                    "ratelevel_code": "R",
                    "ref_date": date(2024, 1, 1),
                    "value": 3.5,
                },
            ],
        )
        result = map_jolts_to_ces(path)
        expected_cols = {
            "geographic_type",
            "geographic_code",
            "industry_type",
            "industry_code",
            "rate_level",
            "data_element",
            "ref_date",
            "value",
        }
        assert set(result.columns) == expected_cols

    def test_rate_and_level_mapping(self, tmp_path):
        from datetime import date

        path = _make_jolts_parquet(
            tmp_path / "jolts.parquet",
            [
                {
                    "industry_code": "300000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": date(2024, 1, 1),
                    "value": 400.0,
                },
                {
                    "industry_code": "300000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "R",
                    "ref_date": date(2024, 1, 1),
                    "value": 3.1,
                },
            ],
        )
        result = map_jolts_to_ces(path)
        levels = result.filter(pl.col("rate_level") == "level")
        rates = result.filter(pl.col("rate_level") == "rate")
        assert levels.height >= 1
        assert rates.height >= 1

    def test_dataelement_mapping(self, tmp_path):
        from datetime import date

        path = _make_jolts_parquet(
            tmp_path / "jolts.parquet",
            [
                {
                    "industry_code": "300000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": date(2024, 1, 1),
                    "value": 400.0,
                },
                {
                    "industry_code": "300000",
                    "dataelement_code": "TS",
                    "ratelevel_code": "L",
                    "ref_date": date(2024, 1, 1),
                    "value": 350.0,
                },
            ],
        )
        result = map_jolts_to_ces(path)
        elements = result["data_element"].unique().to_list()
        assert "entries" in elements
        assert "exits" in elements

    def _make_full_domain_fixture(self, tmp_path):
        """Create parquet with levels and rates for domain derivation tests.

        Supersector employment is back-calculated as level * 100 / rate:
          SS 10: emp = 100 * 100 / 2.0 = 5000
          SS 20: emp = 200 * 100 / 4.0 = 5000
          SS 30: emp = 300 * 100 / 3.0 = 10000
          Domain 05: emp = 1000 * 100 / 5.0 = 20000

          Domain 06 level = 100 + 200 + 300 = 600
          Domain 06 emp   = 5000 + 5000 + 10000 = 20000
          Domain 06 rate  = 600 / 20000 * 100 = 3.0

          Domain 08 level = 1000 - 600 = 400
          Domain 08 emp   = 20000 - 20000 = 0  (degenerate, see separate test)
        """
        from datetime import date

        d = date(2024, 1, 1)
        # Use values where domain 05 emp > domain 06 emp
        return _make_jolts_parquet(
            tmp_path / "jolts.parquet",
            [
                # Total Private (domain 05)
                {
                    "industry_code": "100000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": d,
                    "value": 1000.0,
                },
                {
                    "industry_code": "100000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "R",
                    "ref_date": d,
                    "value": 2.5,
                },
                # Mining and Logging (supersector 10)
                {
                    "industry_code": "110099",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": d,
                    "value": 100.0,
                },
                {
                    "industry_code": "110099",
                    "dataelement_code": "HI",
                    "ratelevel_code": "R",
                    "ref_date": d,
                    "value": 2.0,
                },
                # Construction (supersector 20)
                {
                    "industry_code": "230000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": d,
                    "value": 200.0,
                },
                {
                    "industry_code": "230000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "R",
                    "ref_date": d,
                    "value": 4.0,
                },
                # Manufacturing (supersector 30)
                {
                    "industry_code": "300000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": d,
                    "value": 300.0,
                },
                {
                    "industry_code": "300000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "R",
                    "ref_date": d,
                    "value": 3.0,
                },
            ],
        )

    def test_domain_06_level(self, tmp_path):
        """Domain 06 level = sum of supersector levels 10 + 20 + 30."""
        path = self._make_full_domain_fixture(tmp_path)
        result = map_jolts_to_ces(path)
        d06 = result.filter(
            (pl.col("industry_code") == "06")
            & (pl.col("data_element") == "entries")
            & (pl.col("rate_level") == "level")
        )
        assert d06.height == 1
        assert d06["value"][0] == pytest.approx(600.0)
        assert d06["industry_type"][0] == "domain"

    def test_domain_06_rate(self, tmp_path):
        """Domain 06 rate = domain_06_level / domain_06_employment * 100.

        Employment per supersector = level * 100 / rate:
          SS 10: 100 * 100 / 2.0 = 5000
          SS 20: 200 * 100 / 4.0 = 5000
          SS 30: 300 * 100 / 3.0 = 10000
        Domain 06 emp = 20000, level = 600
        Rate = 600 / 20000 * 100 = 3.0
        """
        path = self._make_full_domain_fixture(tmp_path)
        result = map_jolts_to_ces(path)
        d06_rate = result.filter(
            (pl.col("industry_code") == "06")
            & (pl.col("data_element") == "entries")
            & (pl.col("rate_level") == "rate")
        )
        assert d06_rate.height == 1
        assert d06_rate["value"][0] == pytest.approx(3.0)

    def test_domain_08_level(self, tmp_path):
        """Domain 08 level = domain 05 level - domain 06 level."""
        path = self._make_full_domain_fixture(tmp_path)
        result = map_jolts_to_ces(path)
        d08 = result.filter(
            (pl.col("industry_code") == "08")
            & (pl.col("data_element") == "entries")
            & (pl.col("rate_level") == "level")
        )
        assert d08.height == 1
        # 1000 - 600 = 400
        assert d08["value"][0] == pytest.approx(400.0)
        assert d08["industry_type"][0] == "domain"

    def test_domain_08_rate(self, tmp_path):
        """Domain 08 rate = domain_08_level / domain_08_employment * 100.

        Domain 05 emp = 1000 * 100 / 2.5 = 40000
        Domain 06 emp = 5000 + 5000 + 10000 = 20000
        Domain 08 emp = 40000 - 20000 = 20000
        Domain 08 level = 400
        Rate = 400 / 20000 * 100 = 2.0
        """
        path = self._make_full_domain_fixture(tmp_path)
        result = map_jolts_to_ces(path)
        d08_rate = result.filter(
            (pl.col("industry_code") == "08")
            & (pl.col("data_element") == "entries")
            & (pl.col("rate_level") == "rate")
        )
        assert d08_rate.height == 1
        assert d08_rate["value"][0] == pytest.approx(2.0)

    def test_derived_domains_have_both_levels_and_rates(self, tmp_path):
        """Domains 06 and 08 should have both level and rate rows."""
        path = self._make_full_domain_fixture(tmp_path)
        result = map_jolts_to_ces(path)
        for code in ["06", "08"]:
            domain = result.filter(pl.col("industry_code") == code)
            rate_levels = set(domain["rate_level"].to_list())
            assert rate_levels == {"level", "rate"}, (
                f"Domain {code} missing rate_level values"
            )

    def test_state_geography_mapping(self, tmp_path):
        """State rows should get geographic_type='state'."""
        from datetime import date

        path = _make_jolts_parquet(
            tmp_path / "jolts.parquet",
            [
                {
                    "state_code": "00",
                    "industry_code": "100000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": date(2024, 1, 1),
                    "value": 5000.0,
                },
                {
                    "state_code": "06",
                    "industry_code": "100000",
                    "dataelement_code": "HI",
                    "ratelevel_code": "L",
                    "ref_date": date(2024, 1, 1),
                    "value": 800.0,
                },
            ],
        )
        result = map_jolts_to_ces(path)
        nat = result.filter(pl.col("geographic_type") == "national")
        st = result.filter(pl.col("geographic_type") == "state")
        assert nat.height >= 1
        assert nat["geographic_code"][0] == "00"
        assert st.height >= 1
        assert st["geographic_code"][0] == "06"
