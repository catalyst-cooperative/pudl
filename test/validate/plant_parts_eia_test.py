"""Validate post-ETL Generators data from EIA 860."""

import logging

import pandas as pd
import pytest

import pudl
from pudl.analysis.plant_parts_eia import (
    IDX_OWN_TO_ADD,
    IDX_TO_ADD,
    PLANT_PARTS,
    SUM_COLS,
    MakeMegaGenTbl,
)

logger = logging.getLogger(__name__)


#################
# Data Validation
#################


def test_run_aggregations(pudl_out_eia, live_dbs):
    """Run a test of the aggregated columns.

    This test will used the plant_parts_eia, re-run groubys and check similarity.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq == "YS":  # Annual only.
        logger.info("Testing ownership fractions for owned records.")

        mcoe = pudl_out_eia.mcoe_generators()
        own_eia860 = pudl_out_eia.own_eia860()
        gens_mega = MakeMegaGenTbl().execute(mcoe, own_eia860)
        plant_parts_eia = pudl_out_eia.plant_parts_eia()

        for part_name in PLANT_PARTS:
            logger.info(f"Begining tests for {part_name}:")
            if part_name == "plant_match_ferc1":
                test_merge = prep_test_merge(
                    part_name,
                    plant_parts_eia,
                    plant_parts_eia.loc[
                        plant_parts_eia.plant_part != "plant_match_ferc1"
                    ],
                )
                # This is a manually generated plant part not present in gens_mega.
                # Test whether aggregating the records flagged as having a ferc1_generator_agg_id
                # produces the same result as the plant_match_ferc1 plant parts.
            else:
                test_merge = prep_test_merge(part_name, plant_parts_eia, gens_mega)
            for test_col in SUM_COLS:
                # Check if test aggregation is the same as generated aggreation
                # Apply a boolean column to the test df.
                test_merge[f"test_{test_col}"] = (
                    (test_merge[f"{test_col}_test"] == test_merge[f"{test_col}"])
                    | (
                        test_merge[f"{test_col}_test"].isnull()
                        & test_merge[f"{test_col}"].isnull()
                    )
                    | (test_merge.ownership_record_type == "total")
                )
                result = list(test_merge[f"test_{test_col}"].unique())
                logger.info(f"  Results for {test_col}: {result}")
                if not all(result):
                    logger.warning(
                        f"{test_col} has {len([val for val in result if val is False])} non-unique values when aggregating for {part_name}."
                    )
                    # raise AssertionError(
                    #    f"{test_col}'s '"
                    # )
    else:
        pytest.skip("Plant part validation only works for annual data.")


def prep_test_merge(part_name, plant_parts_eia, gens_mega):
    """Run the test groupby and merge with the aggregations."""
    id_cols = PLANT_PARTS[part_name]["id_cols"]
    plant_cap = (
        gens_mega[gens_mega.ownership_record_type == "owned"]
        .pipe(pudl.helpers.convert_cols_dtypes, "eia")
        .groupby(by=id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD, observed=True)[SUM_COLS]
        .sum(min_count=1)
        .reset_index()
        .pipe(pudl.helpers.convert_cols_dtypes, "eia")
    )
    test_merge = pd.merge(
        plant_parts_eia[plant_parts_eia.plant_part == part_name],
        plant_cap,
        on=id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD,
        how="outer",
        indicator=True,
        suffixes=("", "_test"),
    )
    return test_merge
