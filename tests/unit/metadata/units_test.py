"""Tests for the PUDL unit registry defined in pudl.metadata.units."""

import pint
import pytest

from pudl.metadata.units import PUDL_UNIT_REGISTRY


@pytest.mark.parametrize(
    "unit_string",
    [
        # Standard pint units used in PUDL field metadata
        "MW",
        "MWh",
        "kW",
        "kWh",
        "hour",
        "count",
        "kg",
        "short_ton",
        "foot",
        "gallon",
        "foot**3",
        "USD",
        "degree",
        "pound",
        # Compound standard units
        "short_ton / hour",
        "gallon / minute",
        "foot**3 / minute",
        "MWh / count",
        "count / km**2",
        "degree**2",
        # PUDL custom energy-industry units
        "MMBtu",
        "MMBTU",
        "Mcf",
        "MMcf",
        "TBtu",
        "VAr",
        # Compound custom units
        "MMBtu / Mcf",
        "MMBtu / MWh",
        "USD / MWh",
        "USD / MMBtu",
        "pound / MMBtu",
        "pound / hour",
        "short_ton / MMBtu",
        "megaVAr",
        "MVAr",
    ],
)
def test_valid_unit_strings(unit_string: str) -> None:
    """Valid unit strings must parse without error."""
    PUDL_UNIT_REGISTRY.parse_units(unit_string)


@pytest.mark.parametrize(
    "unit_string",
    [
        "not_a_unit",
        "megawatts_per_flurble",
        "person",  # removed custom alias — use 'count' instead
        "MWh / person",  # compound using removed alias
        "dollars",  # use 'USD' instead
        "MMBTU / kWh extra garbage",
    ],
)
def test_invalid_unit_strings(unit_string: str) -> None:
    """Invalid unit strings must raise an exception."""
    with pytest.raises(pint.errors.UndefinedUnitError):
        PUDL_UNIT_REGISTRY.parse_units(unit_string)
