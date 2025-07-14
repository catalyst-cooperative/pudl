"""Unit tests for the pudl_null_cols script."""

import io
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from pudl.scripts.pudl_null_cols import (
    compact_row_condition,
    date_column_to_year_expr,
    get_available_years,
    get_null_years,
    infer_row_conditions,
    main,
    max_eia860_year,
)


@pytest.mark.parametrize(
    "input_column,expected_output",
    [
        ("report_year", "report_year"),
        ("year", "year"),
        ("fiscal_year_end", "fiscal_year_end"),
        ("report_date", "EXTRACT(year FROM report_date)"),
        ("start_date", "EXTRACT(year FROM start_date)"),
        ("timestamp", "EXTRACT(year FROM timestamp)"),
    ],
)
def test_date_column_to_year_expr(input_column, expected_output):
    """Test that date_column_to_year_expr uses EXTRACT when appropriate."""
    assert date_column_to_year_expr(input_column) == expected_output


def test_max_eia860_year(mocker):
    """Test that max_eia860_year returns the maximum year from EIA-860 data."""
    # Create mock DataFrame with report_date column
    mock_df = pd.DataFrame(
        {"report_date": pd.to_datetime(["2020-01-01", "2021-01-01", "2022-01-01"])}
    )
    mock_get_parquet_table = mocker.patch(
        "pudl.scripts.pudl_null_cols.get_parquet_table"
    )
    mock_get_parquet_table.return_value = mock_df

    result = max_eia860_year()

    mock_get_parquet_table.assert_called_once_with(
        "core_eia860__scd_ownership",
        columns=["report_date"],
    )
    assert result == 2022


@pytest.mark.parametrize(
    "null_years,available_years,date_column,expected_output",
    [
        # No data years
        (
            [2020, 2021, 2022],
            [2020, 2021, 2022],
            "report_date",
            "ERROR -- Column has no data. Debug the issue or exclude the column.",
        ),
        # Single data year
        (
            [2020, 2022],
            [2020, 2021, 2022],
            "report_date",
            "EXTRACT(year FROM report_date) = 2021",
        ),
        # Contiguous range from start
        (
            [2022, 2023],
            [2020, 2021, 2022, 2023],
            "report_date",
            "EXTRACT(year FROM report_date) <= 2021",
        ),
        # Contiguous range to end
        (
            [2020, 2021],
            [2020, 2021, 2022, 2023],
            "report_date",
            "EXTRACT(year FROM report_date) >= 2022",
        ),
        # Contiguous range in the middle
        (
            [2020, 2023, 2024],
            [2020, 2021, 2022, 2023, 2024],
            "report_date",
            "EXTRACT(year FROM report_date) BETWEEN 2021 AND 2022",
        ),
        # Non-contiguous with fewer data years (use IN)
        (
            [2021, 2023, 2025],
            [2020, 2021, 2022, 2023, 2024, 2025],
            "report_date",
            "EXTRACT(year FROM report_date) IN (2020, 2022, 2024)",
        ),
        # Non-contiguous with more data years (use NOT IN)
        (
            [2021],
            [2020, 2021, 2022, 2023, 2024, 2025],
            "report_date",
            "EXTRACT(year FROM report_date) NOT IN (2021)",
        ),
        # Year column passthrough (no EXTRACT)
        (
            [2020, 2022],
            [2020, 2021, 2022],
            "report_year",
            "report_year = 2021",
        ),
    ],
)
def test_compact_row_condition(
    null_years, available_years, date_column, expected_output
):
    """Test edge cases for compact_row_condition."""
    result = compact_row_condition(
        null_years=null_years,
        available_years=available_years,
        date_column=date_column,
    )
    assert result == expected_output


@pytest.fixture
def parquet_data():
    """Create a temporary parquet file with test data."""
    temp_dir = tempfile.mkdtemp()
    parquet_path = Path(temp_dir) / "test_table.parquet"

    test_data = pd.read_csv(
        io.StringIO(
            """report_date,column_a,column_b,column_c
2020-01-01,1,,7
2020-01-01,2,,8
2021-01-01,,3,9
2021-01-01,,4,10
2022-01-01,5,,11"""
        ),
        dtype_backend="pyarrow",
        parse_dates=["report_date"],
    )

    # Write to parquet
    table = pa.Table.from_pandas(test_data)
    pq.write_table(table, parquet_path)

    return parquet_path


@pytest.mark.parametrize(
    "column,expected_null_years",
    [
        ("column_a", [2021]),  # null in 2021
        ("column_b", [2020, 2022]),  # null in 2020, 2022
        ("column_c", []),  # no null years
    ],
)
def test_get_null_years(mocker, parquet_data, column, expected_null_years):
    """Test that get_null_years really does exclude null years."""
    # Mock PudlPaths to return our test file
    mock_paths = mocker.MagicMock()
    mock_paths.parquet_path.return_value = str(parquet_data)
    mock_pudl_paths = mocker.patch("pudl.scripts.pudl_null_cols.PudlPaths")
    mock_pudl_paths.return_value = mock_paths

    null_years = get_null_years(
        table_name="test_table", column=column, date_column="report_date"
    )
    assert null_years == expected_null_years


def test_get_null_years_with_max_year(mocker, parquet_data):
    """Test that get_null_years obeys the max_year constraint."""
    # Mock PudlPaths to return our test file
    mock_paths = mocker.MagicMock()
    mock_paths.parquet_path.return_value = str(parquet_data)
    mock_pudl_paths = mocker.patch("pudl.scripts.pudl_null_cols.PudlPaths")
    mock_pudl_paths.return_value = mock_paths

    # Test with max_year=2021 (should exclude 2022)
    null_years = get_null_years(
        table_name="test_table",
        column="column_b",
        date_column="report_date",
        max_year=2021,
    )
    assert null_years == [2020]  # 2022 is excluded by max_year


def test_get_available_years(mocker, parquet_data):
    """Test that get_available_years really does list all years present in the date column."""
    # Mock PudlPaths to return our test file
    mock_paths = mocker.MagicMock()
    mock_paths.parquet_path.return_value = str(parquet_data)
    mock_pudl_paths = mocker.patch("pudl.scripts.pudl_null_cols.PudlPaths")
    mock_pudl_paths.return_value = mock_paths

    available_years = get_available_years(
        table_name="test_table", date_column="report_date"
    )
    assert available_years == [2020, 2021, 2022]


def test_get_available_years_with_max_year(mocker, parquet_data):
    """Test that get_available_years obeys the max_year constraint."""
    # Mock PudlPaths to return our test file
    mock_paths = mocker.MagicMock()
    mock_paths.parquet_path.return_value = str(parquet_data)
    mock_pudl_paths = mocker.patch("pudl.scripts.pudl_null_cols.PudlPaths")
    mock_pudl_paths.return_value = mock_paths

    available_years = get_available_years(
        table_name="test_table", date_column="report_date", max_year=2021
    )
    assert available_years == [2020, 2021]


def test_infer_row_conditions(mocker):
    """Test the infer_row_conditions function."""
    # Mock the schema
    mock_schema = mocker.MagicMock()
    mock_schema.names = ["report_date", "column_a", "column_b", "column_c"]
    mock_read_schema = mocker.patch("pudl.scripts.pudl_null_cols.pq.read_schema")
    mock_read_schema.return_value = mock_schema

    # Mock available years
    mock_get_available_years = mocker.patch(
        "pudl.scripts.pudl_null_cols.get_available_years"
    )
    mock_get_available_years.return_value = [2020, 2021, 2022]

    # Mock null years for different columns
    def mock_null_years_side_effect(table_name, column, **kwargs):
        if column == "column_a":
            return [2021]
        if column == "column_b":
            return [2020, 2022]
        # column_c
        return []

    mock_get_null_years = mocker.patch("pudl.scripts.pudl_null_cols.get_null_years")
    mock_get_null_years.side_effect = mock_null_years_side_effect

    # Mock PudlPaths
    mock_pudl_paths = mocker.patch("pudl.scripts.pudl_null_cols.PudlPaths")
    mock_pudl_paths.return_value = mocker.MagicMock()

    result = infer_row_conditions(table_name="test_table", date_column="report_date")

    # Should only include columns with null years
    assert "column_a" in result
    assert "column_b" in result
    assert "column_c" not in result

    # Check the generated conditions
    assert result["column_a"] == "EXTRACT(year FROM report_date) NOT IN (2021)"
    assert result["column_b"] == "EXTRACT(year FROM report_date) = 2021"


def test_main_invalid_table():
    """Test CLI with invalid table name."""
    runner = CliRunner()
    result = runner.invoke(main, ["invalid_table"])

    assert result.exit_code != 0
    assert "Invalid table name" in result.output


def test_main_no_table():
    """Test CLI with no table specified."""
    runner = CliRunner()
    result = runner.invoke(main, [])

    assert result.exit_code != 0  # Click will raise an error for missing argument


def test_main_conflicting_options(mocker):
    """Test CLI with conflicting --ignore-eia860m and --max-year options."""
    # Mock ALL_TABLES to include our test table so we get past the table validation
    mocker.patch("pudl.scripts.pudl_null_cols.ALL_TABLES", ["test_table"])

    runner = CliRunner()
    result = runner.invoke(
        main, ["test_table", "--ignore-eia860m", "--max-year", "2021"]
    )

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


def test_main_yaml_output(mocker):
    """Test that CLI converts row conditions to YAML syntax."""
    mocker.patch("pudl.scripts.pudl_null_cols.ALL_TABLES", ["test_table"])
    mock_infer_row_conditions = mocker.patch(
        "pudl.scripts.pudl_null_cols.infer_row_conditions"
    )
    mock_infer_row_conditions.return_value = {
        "column_a": "EXTRACT(year FROM report_date) = 2020",
        "column_b": "EXTRACT(year FROM report_date) NOT IN (2021)",
    }

    runner = CliRunner()
    result = runner.invoke(main, ["test_table"])

    assert result.exit_code == 0

    # Check YAML structure
    lines = result.output.strip().split("\n")
    assert "column_a: EXTRACT(year FROM report_date) = 2020" in lines
    assert "column_b: EXTRACT(year FROM report_date) NOT IN (2021)" in lines
    mock_infer_row_conditions.assert_called_once_with(
        table_name="test_table", date_column="report_date", max_year=None
    )
