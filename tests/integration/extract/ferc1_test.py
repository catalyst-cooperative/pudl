"""Integration tests for FERC Form 1 extraction functions."""

from typing import Literal

import pandas as pd
import pytest

import pudl
from pudl.settings import Ferc1DataConfig, PudlDataConfig


class TestFerc1ExtractDebugFunctions:
    """Verify the ferc1 extraction debug functions are working properly."""

    @pytest.mark.usefixtures("ferc1_engine_dbf")
    def test_extract_dbf(self):
        """Test extract_dbf."""
        ferc1_dbf_raw_dfs: dict[str, pd.DataFrame] = pudl.extract.ferc1.extract_dbf(
            pudl_data_config=PudlDataConfig(ferc1=Ferc1DataConfig(years=[2020, 2021]))
        )

        for table_name, df in ferc1_dbf_raw_dfs.items():
            assert (df.report_year >= 2020).all() and (df.report_year < 2022).all(), (
                f"Unexpected years found in table: {table_name}"
            )

    @pytest.mark.usefixtures("ferc1_engine_xbrl")
    def test_extract_xbrl(self):
        """Test extract_xbrl."""
        ferc1_xbrl_raw_dfs: dict[
            str, dict[Literal["duration", "instant"], pd.DataFrame]
        ] = pudl.extract.ferc1.extract_xbrl(
            pudl_data_config=PudlDataConfig(ferc1=Ferc1DataConfig(years=[2021]))
        )

        for table_name, xbrl_tables in ferc1_xbrl_raw_dfs.items():
            for table_type, df in xbrl_tables.items():
                # Some raw xbrl tables are empty
                if not df.empty and table_type == "duration":
                    assert (df.report_year >= 2020).all() and (
                        df.report_year < 2022
                    ).all(), f"Unexpected years found in table: {table_name}"
