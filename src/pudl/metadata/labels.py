"""Descriptive labels for coded field values."""

ESTIMATED_OR_ACTUAL: dict[str, str] = {"E": "estimated", "A": "actual"}
"""Descriptive labels for EIA estimated or actual codes."""

POWER_PURCHASE_TYPES_FERC1: dict[str, str] = {
    "RQ": "requirement",
    "LF": "long_firm",
    "IF": "intermediate_firm",
    "SF": "short_firm",
    "LU": "long_unit",
    "IU": "intermediate_unit",
    "EX": "electricity_exchange",
    "OS": "other_service",
    "AD": "adjustment",
}
"""Descriptive labels for FERC 1 power purchase type codes."""

COALMINE_TYPES_EIA: dict[str, str] = {
    "P": "preparation_plant",
    "S": "surface",
    "U": "underground",
    "US": "underground_and_surface",
    "SU": "surface_and_underground",
}
"""Descriptive labels for coal mine type codes used in EIA 923 reporting.

These codes and descriptions come from Page 7 of the EIA 923.
"""

CENSUS_REGIONS: dict[str, str] = {
    "NEW": "New England",
    "MAT": "Middle Atlantic",
    "SAT": "South Atlantic",
    "ESC": "East South Central",
    "WSC": "West South Central",
    "ENC": "East North Central",
    "WNC": "West North Central",
    "MTN": "Mountain",
    "PACC": "Pacific Contiguous (OR, WA, CA)",
    "PACN": "Pacific Non-Contiguous (AK, HI)",
}
"""Descriptive labels for Census Region codes.

Not currently being used.
"""

RTO_ISO: dict[str, str] = {
    "CAISO": "California ISO",
    "ERCOT": "Electric Reliability Council of Texas",
    "ISONE": "ISO New England",
    "MISO": "Midcontinent ISO",
    "NYISO": "New York ISO",
    "PJM": "PJM Interconnection",
    "SPP": "Southwest Power Pool",
}
"""Descriptive labels for RTO/ISO short codes.

Not currently being used.
"""

FUEL_UNITS_EIA: dict[str, str] = {
    "mcf": "Thousands of cubic feet (for gases)",
    "short_tons": "Short tons (for solids)",
    "barrels": "Barrels (for liquids)",
    "mwh": "Megawatt hours (for electricity)",
}
"""Descriptive labels for the units of measure EIA uses for fuels.

The physical units fuel consumption is reported in.  All consumption is reported
in either short tons for solids, thousands of cubic feet for gases, and barrels
for liquids.

Not currently being used.
"""
