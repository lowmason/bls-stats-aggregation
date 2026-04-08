"""State FIPS codes and name mappings for QCEW geographic filtering."""

from __future__ import annotations

# 50 states + DC + Puerto Rico (2-digit FIPS codes)
STATES: list[str] = [
    '01', '02', '04', '05', '06', '08', '09', '10', '11', '12',
    '13', '15', '16', '17', '18', '19', '20', '21', '22', '23',
    '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
    '34', '35', '36', '37', '38', '39', '40', '41', '42', '44',
    '45', '46', '47', '48', '49', '50', '51', '53', '54', '55',
    '56', '72',
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
    '55': 'Wisconsin', '56': 'Wyoming', '72': 'Puerto Rico',
}

STATE_NAME_TO_FIPS: dict[str, str] = {
    'United States': '00',
    **{name: fips for fips, name in STATE_FIPS_TO_NAME.items()},
}
