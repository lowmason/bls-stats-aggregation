"""CES industry hierarchy and cross-program industry standard.

Defines the BLS CES industry structure that all program subpackages map to:
  sector (2-digit NAICS) → supersector → domain

Provides ownership-to-sector mapping for government employment and
the 3-digit NAICS split of manufacturing into durable/nondurable goods.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


# ---------------------------------------------------------------------------
# Industry hierarchy
# ---------------------------------------------------------------------------
# Each row maps a 2-digit NAICS sector to its BLS CES supersector and domain.
# Sector codes use simplified forms: '31' (not '31-33'), '44' (not '44-45'),
# '48' (not '48-49').

_HIERARCHY_ROWS = [
    # (sector_code, sector_title, supersector_code, supersector_title, domain_code, domain_title)
    # Goods-producing
    ('21', 'Mining', '10', 'Mining and Logging', 'G', 'Goods-producing'),
    ('23', 'Construction', '20', 'Construction', 'G', 'Goods-producing'),
    ('31', 'Manufacturing', '30', 'Manufacturing', 'G', 'Goods-producing'),
    # Service-providing
    ('42', 'Wholesale Trade', '40', 'Trade, Transportation, and Utilities', 'S', 'Service-providing'),
    ('44', 'Retail Trade', '40', 'Trade, Transportation, and Utilities', 'S', 'Service-providing'),
    ('48', 'Transportation and Warehousing', '40', 'Trade, Transportation, and Utilities', 'S', 'Service-providing'),
    ('22', 'Utilities', '40', 'Trade, Transportation, and Utilities', 'S', 'Service-providing'),
    ('51', 'Information', '50', 'Information', 'S', 'Service-providing'),
    ('52', 'Finance and Insurance', '55', 'Financial Activities', 'S', 'Service-providing'),
    ('53', 'Real Estate', '55', 'Financial Activities', 'S', 'Service-providing'),
    ('54', 'Professional and Technical Services', '60', 'Professional and Business Services', 'S', 'Service-providing'),
    ('55', 'Management of Companies', '60', 'Professional and Business Services', 'S', 'Service-providing'),
    ('56', 'Administrative and Waste Services', '60', 'Professional and Business Services', 'S', 'Service-providing'),
    ('61', 'Educational Services', '65', 'Private Education and Health Services', 'S', 'Service-providing'),
    ('62', 'Health Care and Social Assistance', '65', 'Private Education and Health Services', 'S', 'Service-providing'),
    ('71', 'Arts, Entertainment, and Recreation', '70', 'Leisure and Hospitality', 'S', 'Service-providing'),
    ('72', 'Accommodation and Food Services', '70', 'Leisure and Hospitality', 'S', 'Service-providing'),
    ('81', 'Other Services', '80', 'Other Services', 'S', 'Service-providing'),
]

INDUSTRY_HIERARCHY: pl.LazyFrame = pl.LazyFrame(
    {
        'sector_code': [r[0] for r in _HIERARCHY_ROWS],
        'sector_title': [r[1] for r in _HIERARCHY_ROWS],
        'supersector_code': [r[2] for r in _HIERARCHY_ROWS],
        'supersector_title': [r[3] for r in _HIERARCHY_ROWS],
        'domain_code': [r[4] for r in _HIERARCHY_ROWS],
        'domain_title': [r[5] for r in _HIERARCHY_ROWS],
    },
    schema={
        'sector_code': pl.Utf8,
        'sector_title': pl.Utf8,
        'supersector_code': pl.Utf8,
        'supersector_title': pl.Utf8,
        'domain_code': pl.Utf8,
        'domain_title': pl.Utf8,
    },
)


# ---------------------------------------------------------------------------
# Hierarchy query helpers
# ---------------------------------------------------------------------------

def get_sector_codes() -> list[str]:
    """Return sorted unique 2-digit NAICS-based sector codes."""
    return sorted(set(r[0] for r in _HIERARCHY_ROWS))


def get_supersector_codes() -> list[str]:
    """Return sorted unique supersector codes (10, 20, 30, ..., 80)."""
    return sorted(set(r[2] for r in _HIERARCHY_ROWS))


def get_supersector_components() -> dict[str, list[str]]:
    """Map each supersector to its component NAICS-based sector codes.

    Includes government supersector ``'90'`` with sectors ``'91'``, ``'92'``,
    ``'93'`` (federal, state, local).
    """
    result: dict[str, list[str]] = {}
    for sector_code, _, ss_code, _, _, _ in _HIERARCHY_ROWS:
        result.setdefault(ss_code, []).append(sector_code)
    result['90'] = ['91', '92', '93']
    return {k: sorted(v) for k, v in sorted(result.items())}


_GOODS_SUPERSECTORS = frozenset({'10', '20', '30'})
_ALL_PRIVATE_SUPERSECTORS = frozenset(get_supersector_codes())

DOMAIN_DEFINITIONS: dict[str, dict] = {
    '00': {'name': 'Total Non-Farm', 'includes_govt': True, 'goods_only': False},
    '05': {'name': 'Total Private', 'includes_govt': False, 'goods_only': False},
    '06': {'name': 'Goods-Producing', 'includes_govt': False, 'goods_only': True},
    '07': {'name': 'Service-Providing', 'includes_govt': True, 'goods_only': False},
    '08': {'name': 'Private Service-Providing', 'includes_govt': False, 'goods_only': False},
}


def get_domain_supersectors(domain_code: str) -> list[str]:
    """Return the supersector codes that compose a given domain.

    Args:
        domain_code: One of ``'00'``, ``'05'``, ``'06'``, ``'07'``, ``'08'``.
    """
    all_private = sorted(_ALL_PRIVATE_SUPERSECTORS)
    goods = sorted(_GOODS_SUPERSECTORS)
    services_private = sorted(_ALL_PRIVATE_SUPERSECTORS - _GOODS_SUPERSECTORS)

    if domain_code == '00':
        return all_private + ['90']
    elif domain_code == '05':
        return all_private
    elif domain_code == '06':
        return goods
    elif domain_code == '07':
        return services_private + ['90']
    elif domain_code == '08':
        return services_private
    else:
        raise ValueError(f'Unknown domain code: {domain_code!r}')


# ---------------------------------------------------------------------------
# CES industry codes and cross-mappings
# ---------------------------------------------------------------------------

_CES_DOMAIN = [
    ('000000', '00', 'Total Non-Farm'),
    ('050000', '05', 'Total Private'),
    ('060000', '06', 'Goods-Producing Industries'),
    ('070000', '07', 'Service-Providing Industries'),
    ('080000', '08', 'Private Service-Providing'),
]

_CES_SUPERSECTOR = [
    ('100000', '10', 'Natural Resources and Mining'),
    ('200000', '20', 'Construction'),
    ('300000', '30', 'Manufacturing'),
    ('400000', '40', 'Trade, Transportation, and Utilities'),
    ('500000', '50', 'Information'),
    ('550000', '55', 'Financial Activities'),
    ('600000', '60', 'Professional and Business Services'),
    ('650000', '65', 'Education and Health Services'),
    ('700000', '70', 'Leisure and Hospitality'),
    ('800000', '80', 'Other Services'),
    ('900000', '90', 'Government'),
]

_CES_SECTOR = [
    ('102100', '21', 'Mining, quarrying, and oil and gas extraction'),
    ('310000', '31', 'Durable goods'),
    ('320000', '32', 'Nondurable goods'),
    ('414200', '41', 'Wholesale trade'),
    ('420000', '42', 'Retail trade'),
    ('430000', '43', 'Transportation and warehousing'),
    ('442200', '22', 'Utilities'),
    ('555200', '52', 'Finance and insurance'),
    ('555300', '53', 'Real estate and rental and leasing'),
    ('605400', '54', 'Professional, scientific, and technical services'),
    ('605500', '55', 'Management of companies and enterprises'),
    ('605600', '56', 'Administrative and support and waste management'),
    ('656100', '61', 'Private educational services'),
    ('656200', '62', 'Health care and social assistance'),
    ('707100', '71', 'Arts, entertainment, and recreation'),
    ('707200', '72', 'Accommodation and food services'),
    ('909100', '91', 'Federal'),
    ('909200', '92', 'State government'),
    ('909300', '93', 'Local government'),
]

# CES sector code → NAICS code.  Most are identity; CES uses its own codes
# for Wholesale/Retail/Transportation that differ from NAICS.
CES_SECTOR_TO_NAICS: dict[str, str] = {
    '21': '21', '31': '31', '32': '32',
    '41': '42',  # CES Wholesale → NAICS 42
    '42': '44',  # CES Retail → NAICS 44 (44-45)
    '43': '48',  # CES Transportation → NAICS 48 (48-49)
    '22': '22', '52': '52', '53': '53', '54': '54', '55': '55', '56': '56',
    '61': '61', '62': '62', '71': '71', '72': '72',
    '91': '91', '92': '92', '93': '93',
}

# Government ownership codes (QCEW own_code) → CES sector codes.
GOVT_OWNERSHIP_TO_SECTOR: dict[str, str] = {
    '1': '91',  # Federal
    '2': '92',  # State
    '3': '93',  # Local
}

# NAICS 3-digit manufacturing subsectors → CES durable/nondurable sector code.
# CES sector 31 = Durable goods, sector 32 = Nondurable goods.
NAICS3_TO_MFG_SECTOR: dict[str, str] = {
    # Nondurable goods (CES sector 32)
    '311': '32', '312': '32', '313': '32', '314': '32', '315': '32',
    '316': '32', '322': '32', '323': '32', '324': '32', '325': '32', '326': '32',
    # Durable goods (CES sector 31)
    '321': '31', '327': '31', '331': '31', '332': '31', '333': '31',
    '334': '31', '335': '31', '336': '31', '337': '31', '339': '31',
}

# Supersectors that contain exactly one NAICS sector.
SINGLE_SECTOR_SUPERSECTORS: dict[str, str] = {
    '20': '23',  # Construction
    '50': '51',  # Information
    '80': '81',  # Other Services
}


# ---------------------------------------------------------------------------
# IndustryEntry dataclass & canonical map
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IndustryEntry:
    """Single industry mapping for cross-program consistency.

    Attributes:
        industry_code: Unified 2-digit code (e.g. ``'00'``, ``'10'``).
        industry_type: One of ``'domain'``, ``'supersector'``, ``'sector'``.
        industry_name: Human-readable industry name.
        ces_code: Six-digit CES industry code (e.g. ``'000000'``, ``'100000'``).
        qcew_naics: QCEW NAICS code for the CSV slice API (empty for aggregates).
    """

    industry_code: str
    industry_type: str
    industry_name: str
    ces_code: str
    qcew_naics: str


def _build_industry_map() -> list[IndustryEntry]:
    entries: list[IndustryEntry] = []

    for ces_code, code, name in _CES_DOMAIN:
        entries.append(IndustryEntry(code, 'domain', name, ces_code, ''))

    for ces_code, code, name in _CES_SUPERSECTOR:
        entries.append(IndustryEntry(code, 'supersector', name, ces_code, ''))

    for ces_code, code, name in _CES_SECTOR:
        qcew = CES_SECTOR_TO_NAICS.get(code, code)
        entries.append(IndustryEntry(code, 'sector', name, ces_code, qcew))

    return entries


INDUSTRY_MAP: list[IndustryEntry] = _build_industry_map()
"""Complete industry mapping spanning domain, supersector, and sector levels."""
