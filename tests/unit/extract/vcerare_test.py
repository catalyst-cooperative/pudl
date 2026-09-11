"""Unit tests for :mod:`pudl.extract.vcerare`."""

import pytest

from pudl.extract.vcerare import (
    DATETIME_HOUR_OF_YEAR_START_YEAR,
    _clean_column_name,
    _vcerare_column_types,
)


@pytest.mark.parametrize(
    "raw_col,expected",
    [
        ("Adams_Washington", "adams_washington"),
        ("De.Kalb_Georgia", "dekalb_georgia"),
        ("Baltimore-City_Maryland", "baltimore_city_maryland"),
    ],
)
def test_clean_column_name(raw_col: str, expected: str) -> None:
    assert _clean_column_name(raw_col) == expected


@pytest.mark.parametrize(
    "year,expected_hour_type",
    [
        (DATETIME_HOUR_OF_YEAR_START_YEAR - 1, "BIGINT"),
        (DATETIME_HOUR_OF_YEAR_START_YEAR, "TIMESTAMP"),
        (DATETIME_HOUR_OF_YEAR_START_YEAR + 1, "TIMESTAMP"),
    ],
)
def test_vcerare_column_types_hour_of_year_threshold(
    year: int, expected_hour_type: str
) -> None:
    """The hour_of_year column type should flip at DATETIME_HOUR_OF_YEAR_START_YEAR."""
    column_types = _vcerare_column_types(year)
    # The raw header's first cell is always blank; the other columns exercise the
    # same casing/punctuation cleanup as test_clean_column_name.
    header_row = ["", "Adams_Washington", "De.Kalb-County_Georgia"]

    assert column_types(header_row) == {
        "hour_of_year": expected_hour_type,
        "adams_washington": "DOUBLE",
        "dekalb_county_georgia": "DOUBLE",
    }


def test_vcerare_column_types_ignores_original_first_column_name() -> None:
    """The first column is always mapped to hour_of_year, regardless of its raw name.

    VCE RARE's raw header leaves the first cell blank, but nothing about
    _vcerare_column_types should actually depend on that -- it always treats
    header_row[0] as the hour_of_year column and only cleans/uses the rest.
    """
    column_types = _vcerare_column_types(2020)
    assert column_types(["some_unexpected_label", "Adams_Washington"]) == {
        "hour_of_year": "BIGINT",
        "adams_washington": "DOUBLE",
    }
