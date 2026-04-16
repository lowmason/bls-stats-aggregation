"""State FIPS codes, Census regions/divisions, and name mappings.

Provides the crosswalk from state FIPS → Census division → Census region
based on the U.S. Census Bureau reference:
https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf

Puerto Rico (FIPS 72) and the Virgin Islands (FIPS 78) are included in the
state list but are not assigned to any Census region or division.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# States
# ---------------------------------------------------------------------------

# 50 states + DC + Puerto Rico + Virgin Islands (2-digit FIPS codes)
STATES: list[str] = [
    '01', '02', '04', '05', '06', '08', '09', '10', '11', '12',
    '13', '15', '16', '17', '18', '19', '20', '21', '22', '23',
    '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
    '34', '35', '36', '37', '38', '39', '40', '41', '42', '44',
    '45', '46', '47', '48', '49', '50', '51', '53', '54', '55',
    '56', '72', '78',
]

STATE_FIPS_TO_NAME: dict[str, str] = {
    '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas',
    '06': 'California', '08': 'Colorado', '09': 'Connecticut',
    '10': 'Delaware', '11': 'District of Columbia', '12': 'Florida',
    '13': 'Georgia', '15': 'Hawaii', '16': 'Idaho', '17': 'Illinois',
    '18': 'Indiana', '19': 'Iowa', '20': 'Kansas', '21': 'Kentucky',
    '22': 'Louisiana', '23': 'Maine', '24': 'Maryland',
    '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
    '28': 'Mississippi', '29': 'Missouri', '30': 'Montana',
    '31': 'Nebraska', '32': 'Nevada', '33': 'New Hampshire',
    '34': 'New Jersey', '35': 'New Mexico', '36': 'New York',
    '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio',
    '40': 'Oklahoma', '41': 'Oregon', '42': 'Pennsylvania',
    '44': 'Rhode Island', '45': 'South Carolina', '46': 'South Dakota',
    '47': 'Tennessee', '48': 'Texas', '49': 'Utah', '50': 'Vermont',
    '51': 'Virginia', '53': 'Washington', '54': 'West Virginia',
    '55': 'Wisconsin', '56': 'Wyoming', 
    '72': 'Puerto Rico', '78': 'Virgin Islands'
}

STATE_NAME_TO_FIPS: dict[str, str] = {
    'United States': '00',
    **{name: fips for fips, name in STATE_FIPS_TO_NAME.items()},
}

# ---------------------------------------------------------------------------
# Census regions (4) and divisions (9)
# ---------------------------------------------------------------------------

CENSUS_REGIONS: dict[str, str] = {
    '1': 'Northeast',
    '2': 'Midwest',
    '3': 'South',
    '4': 'West',
}

CENSUS_DIVISIONS: dict[str, str] = {
    '1': 'New England',
    '2': 'Middle Atlantic',
    '3': 'East North Central',
    '4': 'West North Central',
    '5': 'South Atlantic',
    '6': 'East South Central',
    '7': 'West South Central',
    '8': 'Mountain',
    '9': 'Pacific',
}

DIVISION_TO_REGION: dict[str, str] = {
    '1': '1',  # New England → Northeast
    '2': '1',  # Middle Atlantic → Northeast
    '3': '2',  # East North Central → Midwest
    '4': '2',  # West North Central → Midwest
    '5': '3',  # South Atlantic → South
    '6': '3',  # East South Central → South
    '7': '3',  # West South Central → South
    '8': '4',  # Mountain → West
    '9': '4',  # Pacific → West
}

# State FIPS → Census division code (excludes Puerto Rico)
STATE_FIPS_TO_DIVISION: dict[str, str] = {
    # Division 1: New England
    '09': '1', '23': '1', '25': '1', '33': '1', '44': '1', '50': '1',
    # Division 2: Middle Atlantic
    '34': '2', '36': '2', '42': '2',
    # Division 3: East North Central
    '17': '3', '18': '3', '26': '3', '39': '3', '55': '3',
    # Division 4: West North Central
    '19': '4', '20': '4', '27': '4', '29': '4', '31': '4', '38': '4',
    '46': '4',
    # Division 5: South Atlantic
    '10': '5', '11': '5', '12': '5', '13': '5', '24': '5', '37': '5',
    '45': '5', '51': '5', '54': '5',
    # Division 6: East South Central
    '01': '6', '21': '6', '28': '6', '47': '6',
    # Division 7: West South Central
    '05': '7', '22': '7', '40': '7', '48': '7',
    # Division 8: Mountain
    '04': '8', '08': '8', '16': '8', '30': '8', '32': '8', '35': '8',
    '49': '8', '56': '8',
    # Division 9: Pacific
    '02': '9', '06': '9', '15': '9', '41': '9', '53': '9',
}

STATE_FIPS_TO_REGION: dict[str, str] = {
    fips: DIVISION_TO_REGION[div]
    for fips, div in STATE_FIPS_TO_DIVISION.items()
}
