"""Create spreadsheets for manually mapping FERC-EIA records and validate matches.

:mod:`pudl.analysis.record_linkage.eia_ferc1_record_linkage` uses machine learning to link records from FERC Form 1
with records from EIA. While this process is way more efficient and logical
than a human, it requires a set of hand-compiled training data in order to do it's job.

The training data also serve as overrides for otherwise bad AI matches. There are
several examples of plants that require human intuition to make sense of. For instance,
sometimes FERC capacities lag behind by several years or are comprised of two or more
EIA records.

This module creates an output spreadsheet, based on a certain utility, that makes the
matching and machine-matched human validation process much easier. It also contains
functions that will read those new/updated/validated matches from the spreadsheet,
validate them, and incorporate them into the existing training data.
"""

import importlib.resources
import pathlib
from typing import Literal

import numpy as np
import pandas as pd

import pudl

# Create a logger to output any messages we might have...
logger = pudl.logging_helpers.get_logger(__name__)


RENAME_COLS_FERC1_EIA: dict = {
    "new_1": "verified",
    "new_2": "used_match_record",
    "new_3": "signature_1",
    "new_4": "signature_2",
    "new_5": "notes",
    "new_6": "record_id_override_1",
    "new_7": "record_id_override_2",
    "new_8": "record_id_override_3",
    "new_9": "best_match",
    "record_id_ferc1": "record_id_ferc1",
    "record_id_eia": "record_id_eia",
    "true_gran": "true_gran",
    "report_year": "report_year",
    "match_type": "match_type",
    "plant_part": "plant_part",
    "ownership_record_type": "ownership_record_type",
    "utility_id_eia": "utility_id_eia",
    "utility_id_pudl": "utility_id_pudl_ferc1",
    "utility_name_ferc1": "utility_name_ferc1",
    "utility_name_eia": "utility_name_eia",
    "plant_id_pudl": "plant_id_pudl_ferc1",
    "unit_id_pudl": "unit_id_pudl",
    "generator_id": "generator_id",
    "plant_name_ferc1": "plant_name_ferc1",
    "plant_name_ppe": "plant_name_eia",
    "fuel_type_code_pudl_ferc1": "fuel_type_code_pudl_ferc1",
    "fuel_type_code_pudl_eia": "fuel_type_code_pudl_eia",
    "new_10": "fuel_type_code_pudl_diff",
    "net_generation_mwh_ferc1": "net_generation_mwh_ferc1",
    "net_generation_mwh_eia": "net_generation_mwh_eia",
    "new_11": "net_generation_mwh_pct_diff",
    "capacity_mw_ferc1": "capacity_mw_ferc1",
    "capacity_mw_eia": "capacity_mw_eia",
    "new_12": "capacity_mw_pct_diff",
    "capacity_factor_ferc1": "capacity_factor_ferc1",
    "capacity_factor_eia": "capacity_factor_eia",
    "new_13": "capacity_factor_pct_diff",
    "total_fuel_cost_ferc1": "total_fuel_cost_ferc1",
    "total_fuel_cost_eia": "total_fuel_cost_eia",
    "new_14": "total_fuel_cost_pct_diff",
    "total_mmbtu_ferc1": "total_mmbtu_ferc1",
    "total_mmbtu_eia": "total_mmbtu_eia",
    "new_15": "total_mmbtu_pct_diff",
    "fuel_cost_per_mmbtu_ferc1": "fuel_cost_per_mmbtu_ferc1",
    "fuel_cost_per_mmbtu_eia": "fuel_cost_per_mmbtu_eia",
    "new_16": "fuel_cost_per_mmbtu_pct_diff",
    "installation_year_ferc1": "installation_year_ferc1",
    "installation_year_eia": "installation_year_eia",
    "new_17": "installation_year_diff",
}

RELEVANT_COLS_PPE: list = [
    "record_id_eia",
    "report_year",
    "utility_id_pudl",
    "utility_id_eia",
    "utility_name_eia",  # I add this in from the utils_eia860() table
    "operational_status_pudl",
    "true_gran",
    "plant_part",
    "ownership_dupe",
    "fraction_owned",
    "plant_id_eia",
    "plant_id_pudl",
    "plant_name_ppe",
    "generator_id",
    "capacity_mw",
    "capacity_factor",
    "net_generation_mwh",
    "installation_year",
    "fuel_type_code_pudl",
    "total_fuel_cost",
    "total_mmbtu",
    "fuel_cost_per_mmbtu",
    "unit_heat_rate_mmbtu_per_mwh",
]

# --------------------------------------------------------------------------------------
# Create Spreadsheets For Manual FERC1-EIA Mapping
# --------------------------------------------------------------------------------------


def _pct_diff(df, col) -> pd.DataFrame:
    """Calculate percent difference between EIA and FERC versions of a column."""
    # Fed in the _pct_diff column so make sure it is neutral for this analysis
    col = col.replace("_pct_diff", "")
    # Fill in the _pct_diff column with the actual percent difference value
    df.loc[(df[f"{col}_eia"] > 0) & (df[f"{col}_ferc1"] > 0), f"{col}_pct_diff"] = (
        round(((df[f"{col}_ferc1"] - df[f"{col}_eia"]) / df[f"{col}_ferc1"] * 100), 2)
    )

    return df


def _is_best_match(
    df, cap_pct_diff=6, net_gen_pct_diff=6, inst_year_diff=3
) -> pd.DataFrame:
    """Fill the best_match column with strings to show cap, net_gen, inst_year match.

    The process of manually checking all of the FERC-EIA matches made by the machine
    learning algorithm is tedius. This function makes it easier to speed through the
    obviously good matches and pay more attention to those that are more questionable.

    By default, a "best match" is comprised of a FERC-EIA match with a capacity percent
    difference of less than 6%, a net generation percent difference of less than 6%, and
    an installation year difference of less than 3 years.
    """
    best_cap = df.capacity_mw_pct_diff < cap_pct_diff
    best_net_gen = df.net_generation_mwh_pct_diff < net_gen_pct_diff
    best_inst_year = df.installation_year_diff < inst_year_diff

    df.loc[:, "best_match"] = df.best_match.fillna("")
    df.loc[best_cap, "best_match"] = "cap"
    df.loc[best_net_gen, "best_match"] = df.best_match + "_net-gen"
    df.loc[best_inst_year, "best_match"] = df.best_match + "_inst_year"
    df.loc[:, "best_match"] = df.best_match.replace(r"^_", "", regex=True)

    return df


def _prep_eia_ferc1(
    eia_ferc1: pd.DataFrame,
    utils_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Prep FERC-EIA for use in override output sheet pre-utility subgroups.

    Args:
        eia_ferc1: The :ref:`out_pudl__yearly_assn_eia_ferc1_plant_parts` table,
            associating EIA and FERC Form 1 plant records.
        utils_eia860: The :ref:`out_eia__yearly_utilities` table.

    Returns:
        A version of the EIA-FERC1 plant association table that's been modified for the
        purposes of creating an manual mapping spreadsheet.
    """
    logger.debug("Prepping FERC-EIA table")
    # Only want to keep the plant_name_ppe field which replaces plant_name_eia
    eia_ferc1_prep = eia_ferc1.copy().drop(columns="plant_name_eia")
    # Add utility_name_eia - this must happen before renaming the cols or else there
    # will be duplicate utility_name_eia columns.
    utils_eia860.loc[:, "report_year"] = utils_eia860.report_date.dt.year
    eia_ferc1_prep = pd.merge(
        eia_ferc1_prep,
        utils_eia860[["utility_id_eia", "utility_name_eia", "report_year"]].copy(),
        on=["utility_id_eia", "report_year"],
        how="left",
        validate="m:1",
    )

    # Add the new columns to the df
    for new_col in [x for x in RENAME_COLS_FERC1_EIA if "new_" in x]:
        eia_ferc1_prep.loc[:, new_col] = pd.NA

    # Rename the columns, and remove unwanted columns from ferc-eia table
    eia_ferc1_prep = eia_ferc1_prep.rename(columns=RENAME_COLS_FERC1_EIA)[
        list(RENAME_COLS_FERC1_EIA.values())
    ]
    # Add in pct diff values
    for pct_diff_col in [x for x in RENAME_COLS_FERC1_EIA.values() if "_pct_diff" in x]:
        eia_ferc1_prep = _pct_diff(eia_ferc1_prep, pct_diff_col)
    # Add in fuel_type_code_pudl diff (qualitative bool)
    eia_ferc1_prep["fuel_type_code_pudl_diff"] = False
    eia_ferc1_prep_nona = eia_ferc1_prep[
        eia_ferc1_prep.fuel_type_code_pudl_eia.notna()
        & eia_ferc1_prep.fuel_type_code_pudl_ferc1.notna()
    ].copy()
    eia_ferc1_prep_nona["fuel_type_code_pudl_diff"] = (
        eia_ferc1_prep_nona.fuel_type_code_pudl_eia
        == eia_ferc1_prep_nona.fuel_type_code_pudl_ferc1
    )
    eia_ferc1_prep.update(eia_ferc1_prep_nona)

    # Add in installation_year diff (diff vs. pct_diff)
    eia_ferc1_prep.loc[
        eia_ferc1_prep.installation_year_ferc1.notna(), "installation_year_ferc1"
    ] = eia_ferc1_prep.installation_year_ferc1.astype("Int64")

    eia_ferc1_prep.loc[
        eia_ferc1_prep.installation_year_eia.notna()
        & eia_ferc1_prep.installation_year_ferc1.notna(),
        "installation_year_diff",
    ] = eia_ferc1_prep.installation_year_eia - eia_ferc1_prep.installation_year_ferc1

    # Add best match col
    eia_ferc1_prep = _is_best_match(eia_ferc1_prep)
    return eia_ferc1_prep


def _prep_deprish(deprish, utils_eia860) -> pd.DataFrame:
    """Prep depreciation data for use in override output sheet pre-utility subgroups."""
    # Not using this function ATM.
    logger.debug("Prepping Deprish Data")

    # Get utility_id_eia from EIA
    util_df = utils_eia860()[["utility_id_pudl", "utility_id_eia"]].drop_duplicates()
    deprish.loc[:, "report_year"] = deprish.report_date.dt.year.astype("Int64")
    deprish = deprish.merge(util_df, on=["utility_id_pudl"], how="left")

    return deprish


def _get_util_year_subsets(inputs_dict, util_id_eia_list, years) -> dict:
    """Get utility and year subsets for each of the input dfs.

    After generating the dictionary with all of the inputs tables loaded, we'll want to
    create subsets of each of those tables based on the utility and year inputs we're
    given. This function takes the input dict generated in _generate_input_dfs() and
    outputs an updated version with df values pertaining to the utilities in
    util_id_eia_list and years in years.

    Args:
        inputs_dict (dict): The output of running _generation_input_dfs()
        util_id_eia_list (list): A list of the utility_id_eia values you want to
            include in a single spreadsheet output. Generally this is a list of the
            subsidiaries that pertain to a single parent company.
        years (list): A list of the years you'd like to add to the override sheets.

    Returns:
        dict: A subset of the inputs_dict that contains versions of the value dfs that
            pertain only to the utilities and years specified in util_id_eia_list and
            years.
    """
    util_year_subset_dict = {}
    logger.debug("Getting utility-year subsets")
    for df_name, df in inputs_dict.items():
        subset_df = df[
            df["report_year"].isin(years) & df["utility_id_eia"].isin(util_id_eia_list)
        ].copy()
        # Make sure dfs aren't too big...
        if len(subset_df) > 500000:
            raise AssertionError(
                "Your subset is more than 500,000 rows...this \
                is going to make excel reaaalllllyyy slow. Try entering a smaller utility \
                or year subset"
            )

        if df_name == "eia_ferc1":
            # Add column with excel formula to check if the override record id is the
            # same as the AI assigned id. Doing this here instead of prep_eia_ferc1
            # because it is based on row index number which is changes when you take a
            # subset of the data.
            subset_df = subset_df.reset_index(drop=True)
            override_col_index = subset_df.columns.get_loc("record_id_override_1")
            record_link_col_index = subset_df.columns.get_loc("record_id_eia")
            subset_df["used_match_record"] = (
                "="
                + chr(ord("a") + override_col_index)
                + (subset_df.index + 2).astype(str)
                + "="
                + chr(ord("a") + record_link_col_index)
                + (subset_df.index + 2).astype(str)
            )

        util_year_subset_dict[f"{df_name}_util_year_subset"] = subset_df

    return util_year_subset_dict


def _output_override_spreadsheet(
    util_year_subset_dict, util_name, output_dir_path
) -> None:
    """Output spreadsheet with tabs for ferc-eia, ppe, deprish for one utility.

    Args:
        util_year_subset_dict (dict): The output from _get_util_year_subsets()
        util_name (str): A string indicating the name of the utility that you are
            creating an override sheet for. The string will be used as the suffix for
            the name of the excel file. Ex: for util_name = "BHE", the file name will be
            BHE_fix_FERC-EIA_overrides.xlsx.
        output_dir_path (str): The relative path to the folder where you'd like to
            output the override spreadsheets that this function creates.
    """
    # Enable unique file names and put all files in directory called overrides
    new_output_path = f"{output_dir_path}/{util_name}_fix_FERC-EIA_overrides.xlsx"
    # Output file to a folder called overrides
    logger.info(f"Outputting {util_name} subsets to tabs\n")
    with pd.ExcelWriter(new_output_path) as writer:
        # writer = pd.ExcelWriter(new_output_path, engine="xlsxwriter")
        for df_name, df in util_year_subset_dict.items():
            df.to_excel(writer, sheet_name=df_name, index=False)


def generate_all_override_spreadsheets(
    eia_ferc1: pd.DataFrame,
    ppe: pd.DataFrame,
    utils_eia860: pd.DataFrame,
    util_dict: dict[str, list[int]],
    years: list[int],
    output_dir_path: str,
) -> None:
    """Output override spreadsheets for all specified utilities and years.

    These manual override files will be output to a folder called "overrides" in the
    output directory.

    Args:
        eia_ferc1: The :ref:`out_pudl__yearly_assn_eia_ferc1_plant_parts` table as a
            dataframe, associating EIA and FERC Form 1 plant records.
        ppe: The :ref:`out_eia__yearly_plant_parts` table as a dataframe.
        utils_eia860: The :ref:`out_eia__yearly_utilities` table as a dataframe.
        util_dict: A dictionary with keys that are the names of utility
            parent companies and values that are lists of subsidiary utility_id_eia
            values. EIA values are used instead of PUDL in this case because PUDL values
            are subject to change.
        years: A list of the years you'd like to add to the override sheets.
        output_dir_path: The relative path to the folder where you'd like to output the
            override spreadsheets that this function creates.
    """
    inputs_dict = {
        "eia_ferc1": _prep_eia_ferc1(eia_ferc1, utils_eia860),
        "ppe": ppe.reset_index()[RELEVANT_COLS_PPE],
    }

    # For each utility, make an override sheet with the correct input table slices
    for util_name, util_id_eia_list in util_dict.items():
        logger.info(f"Developing outputs for {util_name}")
        util_year_subset_dict = _get_util_year_subsets(
            inputs_dict, util_id_eia_list, years
        )
        _output_override_spreadsheet(util_year_subset_dict, util_name, output_dir_path)


# --------------------------------------------------------------------------------------
# Validate Overrides and Merge With Existing Training Data
# --------------------------------------------------------------------------------------


def _check_id_consistency(
    id_col: Literal["record_id_eia_override_1", "record_id_ferc1"],
    df,
    actual_ids,
    error_message,
) -> None:
    """Check for rogue FERC or EIA ids that don't exist.

    Args:
        id_col (str): The name of either the ferc record id column: record_id_ferc1 or
            the eia record override column: record_id_eia_override_1.
        df (pd.DataFrame): A dataframe of intended overrides.
        actual_ids (list): A list of the ferc or eia ids that are valid and come from
            either the ppe or official ferc-eia record linkage.
        error_message (str): A short string to indicate the type of error you're
            checking for. This could be looking for values that aren't in the official
            list or values that are already in the training data.
    """
    logger.debug(f"Checking {id_col} consistency for {error_message}")

    assert len(bad_ids := df[~df[id_col].isin(actual_ids)][id_col].to_list()) == 0, (
        f"{id_col} {error_message}: {bad_ids}"
    )


def check_if_already_in_training(training_data, validated_connections):
    """Check whether any manually mapped records aren't yet in the training data.

    This function is useful for instances where you've started the manual mapping
    process, taken an extended break, and need to check whether the data you've mapped
    has been integrated into the training data or not.
    """
    logger.info(
        """Checking to see if any of the manual matches aren't in the training data."""
    )
    training_vals = training_data.record_id_eia.dropna().unique().tolist()
    val_vals = validated_connections.record_id_eia_override_1.dropna().unique().tolist()

    not_in_training = [x for x in val_vals if x not in training_vals]
    print(f"Total records: {len(val_vals)}")
    print(f"Number of records not in training data: {len(not_in_training)}")
    print(
        f"Number of records already in training data: {len(val_vals - not_in_training)}"
    )


def validate_override_fixes(
    validated_connections: pd.DataFrame,
    ppe: pd.DataFrame,
    eia_ferc1: pd.DataFrame,
    training_data: pd.DataFrame,
    expect_override_overrides: bool = False,
    allow_mismatched_utilities: bool = True,
) -> pd.DataFrame:
    """Process the verified and/or fixed matches and look for human error.

    Args:
        validated_connections: A dataframe in the add_to_training directory that is
            ready to be added to be validated and subsumed into the training data.
        ppe: The :ref:`out_eia__yearly_plant_parts` table as a dataframe.
        eia_ferc1: The :ref:`out_pudl__yearly_assn_eia_ferc1_plant_parts` table as a
            dataframe, associating EIA and FERC Form 1 plant records.
        training_data: The current FERC-EIA training data
        expect_override_overrides: Whether you expect the tables to have
            overridden matches already in the training data.
        allow_mismatched_utilities: Whether you want to allow FERC and EIA
            record ids to come from different utilities.

    Raises:
        AssertionError: If there are EIA override id records that aren't in the original
            FERC-EIA connection.
        AssertionError: If there are FERC record ids that aren't in the original
            FERC-EIA connection.
        AssertionError: If there are EIA override ids that are duplicated throughout the
            override document.
        AssertionError: If the utility id in the EIA override id doesn't match the pudl
            id corresponding with the FERC record.
        AssertionError: If there are EIA override id records that don't correspond to
            the correct report year.
        AssertionError: If you didn't expect to override overrides but the new training
            data implies an override to the existing training data.

    Returns:
        The validated FERC-EIA dataframe you're trying to add to the training data.
    """
    logger.info("Validating overrides")
    # When there are NA values in the verified column in the excel doc, it seems that
    # the TRUE values become 1.0 and the column becomes a type float. Let's replace
    # those here and make it a boolean.
    validated_connections["verified"] = validated_connections["verified"].replace(
        {1: True, np.nan: False}
    )
    # Make sure the verified column doesn't contain non-boolean outliers. This will fail
    # if there are bad values.
    validated_connections = validated_connections.astype(
        {"verified": pd.BooleanDtype()}
    )

    # From validated records, get only records with an override
    only_overrides = (
        validated_connections[validated_connections["verified"]]
        .dropna(subset=["record_id_eia_override_1"])
        .reset_index()
        .copy()
    )

    # Make sure that the override EIA ids actually match those in the original FERC-EIA
    # record linkage.
    actual_eia_ids = ppe.record_id_eia.unique()
    _check_id_consistency(
        "record_id_eia_override_1",
        only_overrides,
        actual_eia_ids,
        "values that don't exist",
    )

    # It's unlikely that this changed, but check FERC id too just in case!
    actual_ferc_ids = eia_ferc1.record_id_ferc1.unique()
    _check_id_consistency(
        "record_id_ferc1", only_overrides, actual_ferc_ids, "values that don't exist"
    )

    # Make sure there are no duplicate EIA id overrides
    logger.debug("Checking for duplicate override ids")
    assert (
        len(
            override_dups := only_overrides[
                only_overrides["record_id_eia_override_1"].duplicated(keep=False)
            ]
        )
        == 0
    ), (
        f"Found record_id_eia_override_1 duplicates: \
    {override_dups.record_id_eia_override_1.unique()}"
    )

    # Make sure there are no duplicate FERC1 id overrides
    logger.debug("Checking for duplicate FERC1 override ids")
    assert (
        len(
            ferc_override_dups := only_overrides[
                only_overrides["record_id_ferc1"].duplicated(keep=False)
            ]
        )
        == 0
    ), (
        f"Found record_id_ferc1 duplicates: \
    {ferc_override_dups.record_id_ferc1.unique()}"
    )

    if not allow_mismatched_utilities:
        # Make sure the EIA utility id from the override matches the PUDL id from the
        # FERC record. Start by mapping utility_id_pudl from PPE onto each
        # record_id_eia_override_1 and then compare to the existing
        # utility_id_pudl_ferc1 record.
        logger.debug("Checking for mismatched utility ids")
        only_overrides = only_overrides.merge(
            ppe[["record_id_eia", "utility_id_pudl"]].drop_duplicates(),
            left_on="record_id_eia_override_1",
            right_on="record_id_eia",
            how="left",
        )
        # Now we compare the two utility_id_pudl columns
        if (
            len(
                bad_utils := only_overrides["utility_id_pudl"].compare(
                    only_overrides["utility_id_pudl_ferc1"]
                )
            )
            > 0
        ):
            raise AssertionError(f"Found mismatched utilities: {bad_utils}")

    # Make sure the year in the EIA id overrides match the year in the report_year
    # column (which comes from FERC)
    logger.debug("Checking that year in override id matches report year")
    only_overrides = only_overrides.merge(
        ppe[["record_id_eia", "report_year"]].drop_duplicates(),
        left_on="record_id_eia_override_1",
        right_on="record_id_eia",
        how="left",
        suffixes=("", "_ppe"),
    )
    if (
        len(
            bad_eia_year := only_overrides["report_year"].compare(
                only_overrides["report_year_ppe"]
            )
        )
        > 0
    ):
        raise AssertionError(
            f"Found record_id_eia_override_1 values that don't correspond to the right \
            report year:\
            {[only_overrides.iloc[x].record_id_eia_override_1 for x in bad_eia_year.index]}"
        )

    # If you don't expect to override values that have already been overridden, make
    # sure the ids you fixed aren't already in the training data.
    if not expect_override_overrides:
        existing_training_eia_ids = training_data.record_id_eia.dropna().unique()
        new_training_eia_ids = only_overrides["record_id_eia_override_1"].unique()
        if eia_overrides := [
            x for x in new_training_eia_ids if x in existing_training_eia_ids
        ]:
            raise AssertionError(
                f"""The following EIA records are already in the training data: {eia_overrides}"""
            )
        existing_training_ferc1_ids = training_data.record_id_ferc1.dropna().unique()
        new_training_ferc1_ids = only_overrides["record_id_ferc1"].unique()
        if ferc_overrides := [
            x for x in new_training_ferc1_ids if x in existing_training_ferc1_ids
        ]:
            raise AssertionError(
                f"""The following FERC 1 records are already in the training data: {ferc_overrides}"""
            )

    # Only return the results that have been verified
    verified_connections = validated_connections[
        validated_connections["verified"]
    ].copy()

    return verified_connections


def get_multi_match_df(training_data: pd.DataFrame) -> pd.DataFrame:
    """Process the verified and/or fixed matches and generate a list of 1:m matches.

    Filter the dataframe to only include FERC records with more than one EIA match.
    Melt this dataframe to report all matched EIA records in the
    ``record_id_eia_override_1`` column.

    Args:
        training_data: A dataframe in the add_to_training directory that is ready to be
            validated and subsumed into the training data.

    Returns:
        A dataframe of one_to_many matches formatted to fit into the existing validation
        framework.
    """
    match_cols = [
        col for col in training_data.columns if "record_id_eia_override_" in col
    ]
    multi_match_cols = [col for col in match_cols if col != "record_id_eia_override_1"]

    id_cols = [col for col in training_data.columns if col not in match_cols]
    multimatch_df = training_data[training_data[multi_match_cols].notnull().any(axis=1)]
    multimatch_df = multimatch_df.melt(
        id_vars=id_cols, var_name="match_number", value_name="record_id_eia_override"
    ).dropna(subset=["record_id_eia_override"])
    multimatch_df = (
        multimatch_df.rename(  # To match existing validation function format
            columns={"record_id_eia_override": "record_id_eia_override_1"}
        )
    )
    return multimatch_df


def _add_to_training(new_overrides, path_to_current_training) -> None:
    """Add the new overrides to the old override sheet."""
    logger.info("Combining all new overrides with existing training data")
    current_training_df = pd.read_csv(path_to_current_training)
    new_training = (
        new_overrides[
            ["record_id_eia", "record_id_ferc1", "signature_1", "signature_2", "notes"]
        ]
        .copy()
        .drop_duplicates(subset=["record_id_eia", "record_id_ferc1"])
    )
    logger.info(f"Found {len(new_training)} new overrides")
    # Combine new and old training data; drop old data in favor or new overrides
    training_data_out = pd.concat([current_training_df, new_training]).drop_duplicates(
        subset=["record_id_ferc1"], keep="last"
    )
    # Output combined training data
    training_data_out.to_csv(path_to_current_training, index=False)


def _add_to_null_overrides(null_matches, current_null_overrides_path) -> None:
    """Take record_id_ferc1 values verified to have no EIA match and add them to csv."""
    logger.info("Adding record_id_ferc1 values with no EIA match to null_overrides csv")
    # Get new null matches
    new_null_matches = null_matches[["record_id_ferc1"]].copy()
    logger.info(f"Found {len(new_null_matches)} new null matches")
    # Get current null matches
    current_null_matches = pd.read_csv(current_null_overrides_path)
    # Combine new and current record_id_ferc1 values that have no EIA match
    out_null_matches = pd.concat(
        [current_null_matches, new_null_matches]
    ).drop_duplicates()
    # Write the combined values out to the same location as before
    out_null_matches.to_csv(current_null_overrides_path, index=False)


def _add_to_one_to_many_overrides(one_to_many, current_one_to_many_path) -> None:
    """Add record_id_ferc1 values verified to have multiple EIA matches to csv."""
    logger.info(
        "Adding record_id_ferc1 values with multiple EIA matches to one_to_many_overrides csv"
    )
    current_one_to_many = pd.read_csv(current_one_to_many_path)
    new_one_to_many = (
        one_to_many[
            ["record_id_eia", "record_id_ferc1", "signature_1", "signature_2", "notes"]
        ]
        .copy()
        .drop_duplicates(subset=["record_id_eia", "record_id_ferc1"])
    )
    logger.debug(
        f"Found {len(new_one_to_many.record_id_ferc1.unique())} new FERC1 records with multiple EIA matches."
    )
    # Combine new and old training data; drop old data in favor or new overrides
    one_to_many = pd.concat([current_one_to_many, new_one_to_many]).drop_duplicates(
        subset=["record_id_ferc1", "record_id_eia"], keep="last"
    )
    # Write the combined values out to the same location as before
    one_to_many.to_csv(current_one_to_many_path, index=False)


def validate_and_add_to_training(
    ppe: pd.DataFrame,
    eia_ferc1: pd.DataFrame,
    input_dir_path: str,
    expect_override_overrides: bool = False,
    allow_mismatched_utilities: bool = True,
    one_to_many: bool = True,
) -> None:
    """Validate, combine, and add overrides to the training data.

    Validating and combinging the records so you only have to loop through the files
    once. Runs :func:`validate_override_fixes` and :func:`_add_to_training`.

    Args:
        input_dir_path: The path to the place where the matched files that you want to
            validate or integrate are.
        expect_override_overrides: This value is explicitly assigned at the top of the
            notebook.
        allow_mismatched_utilities: Whether you are allowed to have FERC-EIA matches
            from different utilities.
        one_to_many: If True, will also validate and save a CSV of one_to_many matches.
    """
    glue_resource_path = importlib.resources.files("pudl.package_data.glue")
    path_to_current_training = glue_resource_path / "eia_ferc1_train.csv"
    path_to_new_training = input_dir_path
    current_training_df = pd.read_csv(path_to_current_training)
    path_to_null_overrides = glue_resource_path / "eia_ferc1_null.csv"
    if one_to_many:
        path_to_one_to_many = glue_resource_path / "eia_ferc1_one_to_many.csv"
    override_cols = [
        "record_id_eia",
        "record_id_ferc1",
        "signature_1",
        "signature_2",
        "notes",
    ]
    null_match_cols = ["record_id_ferc1"]
    all_overrides_list = []
    all_null_matches_list = []
    if one_to_many:
        all_multi_matches_list = []

    # Loop through all the files, validate, and combine them.
    all_files = pathlib.Path(path_to_new_training).iterdir()
    excel_files = [file for file in all_files if file.endswith(".xlsx")]
    if not excel_files:
        raise AssertionError("Found no override files in the add_to_training directory")
    for file in excel_files:
        logger.info(f"Processing fixes in {file}")
        file_raw = pd.read_excel(path_to_new_training + file, engine="calamine")
        file_df = file_raw.pipe(
            validate_override_fixes,
            ppe,
            eia_ferc1,
            current_training_df,
            expect_override_overrides=expect_override_overrides,
            allow_mismatched_utilities=allow_mismatched_utilities,
        ).rename(
            columns={
                "record_id_eia": "record_id_eia_old",
                "record_id_eia_override_1": "record_id_eia",
            }
        )
        # Get just the overrides and combine them to full list of overrides
        only_overrides = file_df[file_df["record_id_eia"].notna()][override_cols].copy()
        all_overrides_list.append(only_overrides)
        # Get just the null matches and combine them to full list of overrides
        only_null_matches = file_df[file_df["record_id_eia"].isna()][
            null_match_cols
        ].copy()
        all_null_matches_list.append(only_null_matches)

        if one_to_many:
            multi_df = file_raw.pipe(get_multi_match_df)
            multi_file_df = multi_df.pipe(
                validate_override_fixes,
                ppe=ppe,
                eia_ferc1=eia_ferc1,
                training_data=current_training_df,
                expect_override_overrides=expect_override_overrides,
                allow_mismatched_utilities=allow_mismatched_utilities,
            ).rename(
                columns={
                    "record_id_eia": "record_id_eia_old",
                    "record_id_eia_override_1": "record_id_eia",
                }
            )

            only_multi = multi_file_df[multi_file_df["record_id_eia"].notna()][
                override_cols
            ].copy()
            all_multi_matches_list.append(only_multi)

    # Combine all training data and null matches
    all_overrides_df = pd.concat(all_overrides_list)
    all_null_matches_df = pd.concat(all_null_matches_list)

    # Add the records to the training data and null overrides
    _add_to_training(all_overrides_df, path_to_current_training)
    _add_to_null_overrides(all_null_matches_df, path_to_null_overrides)

    if one_to_many:
        all_multi_matches_df = pd.concat(all_multi_matches_list)
        _add_to_one_to_many_overrides(all_multi_matches_df, path_to_one_to_many)
