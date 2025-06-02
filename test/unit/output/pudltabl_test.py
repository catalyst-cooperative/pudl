"""Unit tests for PudlTabl class methods."""

from datetime import date, datetime

import pandas as pd
import pyarrow as pa
import pytest

from pudl.output.pudltabl import PudlTabl


class TestPudlTablAggTableName:
    """Test PudlTabl._agg_table_name method."""

    def test_yearly_aggregation(self):
        """Test table name substitution for yearly aggregation."""
        pudl_tabl = PudlTabl(freq="YS")

        result = pudl_tabl._agg_table_name("out_eia923__AGG_generation")
        assert result == "out_eia923__yearly_generation"

    def test_monthly_aggregation(self):
        """Test table name substitution for monthly aggregation."""
        pudl_tabl = PudlTabl(freq="MS")

        result = pudl_tabl._agg_table_name("out_eia923__AGG_fuel_receipts")
        assert result == "out_eia923__monthly_fuel_receipts"

    def test_no_aggregation_removes_agg(self):
        """Test that _AGG is removed when freq is None."""
        pudl_tabl = PudlTabl(freq=None)

        result = pudl_tabl._agg_table_name("out_eia923__AGG_generation")
        assert result == "out_eia923__generation"

    def test_non_aggregated_table_unchanged(self):
        """Test that non-aggregated table names are unchanged."""
        pudl_tabl = PudlTabl(freq="YS")

        result = pudl_tabl._agg_table_name("core_eia860__scd_generators")
        assert result == "core_eia860__scd_generators"


class TestPudlTablInitialization:
    """Test PudlTabl initialization and validation."""

    def test_valid_frequencies(self):
        """Test that valid frequencies are accepted."""
        # These should all work without raising exceptions
        PudlTabl(freq=None)
        PudlTabl(freq="YS")
        PudlTabl(freq="MS")

    def test_invalid_frequency_raises_error(self):
        """Test that invalid frequencies raise ValueError."""
        with pytest.raises(ValueError, match="freq must be one of None, 'MS', or 'YS'"):
            PudlTabl(freq="QS")  # Quarterly not supported

        with pytest.raises(ValueError, match="freq must be one of None, 'MS', or 'YS'"):
            PudlTabl(freq="D")  # Daily not supported

        with pytest.raises(ValueError, match="freq must be one of None, 'MS', or 'YS'"):
            PudlTabl(freq="invalid")

    def test_date_conversion(self):
        """Test that various date formats are properly converted."""
        # String dates
        pudl_tabl = PudlTabl(start_date="2020-01-01", end_date="2022-12-31")
        assert isinstance(pudl_tabl.start_date, pd.Timestamp)
        assert isinstance(pudl_tabl.end_date, pd.Timestamp)

        # Date objects
        pudl_tabl = PudlTabl(start_date=date(2020, 1, 1), end_date=date(2022, 12, 31))
        assert isinstance(pudl_tabl.start_date, pd.Timestamp)
        assert isinstance(pudl_tabl.end_date, pd.Timestamp)

        # Datetime objects
        pudl_tabl = PudlTabl(
            start_date=datetime(2020, 1, 1, 10, 30),
            end_date=datetime(2022, 12, 31, 15, 45),
        )
        assert isinstance(pudl_tabl.start_date, pd.Timestamp)
        assert isinstance(pudl_tabl.end_date, pd.Timestamp)

    def test_invalid_date_format_raises_error(self):
        """Test that invalid date formats raise appropriate errors."""
        with pytest.raises((ValueError, TypeError)):
            PudlTabl(start_date="not-a-date")

        with pytest.raises((ValueError, TypeError)):
            PudlTabl(end_date="2020-13-45")  # Invalid month/day

    def test_fill_net_gen_parameter(self):
        """Test that fill_net_gen parameter is properly set."""
        pudl_tabl_default = PudlTabl()
        assert pudl_tabl_default.fill_net_gen is False

        pudl_tabl_true = PudlTabl(fill_net_gen=True)
        assert pudl_tabl_true.fill_net_gen is True

        pudl_tabl_false = PudlTabl(fill_net_gen=False)
        assert pudl_tabl_false.fill_net_gen is False


class TestPudlTablEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_schema(self):
        """Test behavior with empty PyArrow schema."""
        pudl_tabl = PudlTabl()
        empty_schema = pa.schema([])

        filters = pudl_tabl._build_parquet_date_filters(empty_schema)
        assert filters is None

    def test_schema_with_similar_column_names(self):
        """Test schema with columns that contain but aren't exactly date columns."""
        pudl_tabl = PudlTabl(start_date="2020-01-01", end_date="2022-12-31")
        schema = pa.schema(
            [
                pa.field(
                    "report_date_created", pa.date32()
                ),  # Contains but isn't exact
                pa.field("last_report_year", pa.int64()),  # Contains but isn't exact
                pa.field("some_value", pa.float64()),
            ]
        )

        filters = pudl_tabl._build_parquet_date_filters(schema)
        assert filters is None
