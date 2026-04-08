"""Shared fixtures for bls-stats tests."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest


@pytest.fixture()
def qcew_private_raw() -> pl.DataFrame:
    """Minimal QCEW API data for private-sector extraction (own_code='5')."""
    return pl.DataFrame({
        'area_fips': ['US000'] * 3,
        'own_code': ['5'] * 3,
        'industry_code': ['1021', '1022', '1027'],  # Construction, Mfg, Finance
        'year': [2024, 2024, 2024],
        'qtr': [1, 1, 1],
        'month1_emplvl': [100_000, 200_000, 150_000],
        'month2_emplvl': [101_000, 201_000, 151_000],
        'month3_emplvl': [102_000, 202_000, 152_000],
    })


@pytest.fixture()
def qcew_govt_raw() -> pl.DataFrame:
    """Minimal QCEW API data for government extraction (own_code 1/2/3)."""
    return pl.DataFrame({
        'area_fips': ['US000'] * 3,
        'own_code': ['1', '2', '3'],
        'industry_code': ['10', '10', '10'],
        'year': [2024, 2024, 2024],
        'qtr': [1, 1, 1],
        'month1_emplvl': [50_000, 80_000, 120_000],
        'month2_emplvl': [50_500, 80_500, 120_500],
        'month3_emplvl': [51_000, 81_000, 121_000],
    })


@pytest.fixture()
def qcew_combined_raw(qcew_private_raw, qcew_govt_raw) -> pl.DataFrame:
    """Combined private + government raw QCEW data."""
    return pl.concat([qcew_private_raw, qcew_govt_raw], how='diagonal_relaxed')


@pytest.fixture()
def sector_employment_df() -> pl.DataFrame:
    """Sector-level employment DataFrame for aggregation tests.

    Covers enough sectors to test supersector and domain aggregation.
    """
    base_date = date(2024, 1, 12)
    sectors = {
        '21': 500,     # Mining → SS 10
        '23': 8000,    # Construction → SS 20
        '31': 10000,   # Mfg durable → SS 30
        '32': 5000,    # Mfg nondurable → SS 30
        '42': 6000,    # Wholesale → SS 40
        '44': 15000,   # Retail → SS 40
        '48': 5500,    # Transport → SS 40
        '22': 500,     # Utilities → SS 40
        '51': 3000,    # Information → SS 50
        '52': 6500,    # Finance → SS 55
        '53': 2000,    # Real estate → SS 55
        '54': 10000,   # Prof services → SS 60
        '55': 2000,    # Management → SS 60
        '56': 9000,    # Admin → SS 60
        '61': 4000,    # Education → SS 65
        '62': 20000,   # Healthcare → SS 65
        '71': 2500,    # Arts → SS 70
        '72': 16000,   # Hospitality → SS 70
        '81': 5500,    # Other services → SS 80
        '91': 2800,    # Federal govt → SS 90
        '92': 5200,    # State govt → SS 90
        '93': 14000,   # Local govt → SS 90
    }
    return pl.DataFrame({
        'industry_code': list(sectors.keys()),
        'ref_date': [base_date] * len(sectors),
        'employment': list(sectors.values()),
        'qtr': [1] * len(sectors),
        'geographic_type': ['national'] * len(sectors),
        'geographic_code': ['00'] * len(sectors),
    })
