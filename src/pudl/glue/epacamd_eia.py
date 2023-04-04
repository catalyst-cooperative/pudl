"""Extract, clean, and normalize the EPACAMD-EIA crosswalk.

This module defines functions that read the raw EPACAMD-EIA crosswalk file and clean
up the column names.

The crosswalk file was a joint effort on behalf on EPA and EIA and is published on the
EPA's github account at www.github.com/USEPA/camd-eia-crosswalk". It's a work in
progress and worth noting that, at present, only pulls from 2018 data.
"""
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

import pudl.logging_helpers
from pudl.helpers import remove_leading_zeros_from_numeric_strings, simplify_columns
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.output.epacems import year_state_filter
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


def extract(ds: Datastore) -> pd.DataFrame:
    """Extract the EPACAMD-EIA Crosswalk from the Datastore."""
    logger.info("Extracting the EPACAMD-EIA crosswalk from Zenodo")
    with ds.get_zipfile_resource("epacamd_eia", name="epacamd_eia.zip").open(
        "camd-eia-crosswalk-master/epa_eia_crosswalk.csv"
    ) as f:
        return pd.read_csv(f)


def transform(
    epacamd_eia: pd.DataFrame,
    generators_entity_eia: pd.DataFrame,
    boilers_entity_eia: pd.DataFrame,
    processing_all_eia_years: bool,
) -> dict[str, pd.DataFrame]:
    """Clean up the EPACAMD-EIA Crosswalk file.

    In its raw form, the crosswalk contains many fields. The transform process removes
    descriptive fields like state, location, facility name, capacity, operating status,
    and fuel type that can be found by linking this dataset to others already in the
    database. We're primarily concerned with linking ids in this table, not including
    any other plant information.

    The raw file contains several fields with the prefix ``MOD``. These fields are used
    to create a single ID for matches with different spelling. For example,
    ``EIA_PLANT_ID`` (``plant_id_eia`` in PUDL) has the generator ID ``CTG5`` in EPA and
    ``GT5`` in EIA. The ``MOD`` columns for both of these generators
    (``MOD_CAMD_GENERATOR_ID``, ``MOD_EIA_GENERATOR_ID_GEN``) converts that value to 5.
    The ``MATCH_TYPE_GEN``, ``MATCH_TYPE_BOILER``, and ``PLANT_ID_CHANGE_FLAG`` fields
    indicate whether these ``MOD`` fields contain new information. Because we're not
    concerned with creating a new, modified ID field for either EPA or EIA data, we
    don't need these ``MOD`` or ``MATCH_TYPE`` columns in our final output. We just care
    which EPA value maps to which EIA value.

    In terms of cleaning, we implement the standard column name changes: lower-case, no
    special characters, and underscores instead of spaces. We also rename some of the
    columns for clarity and to match how they appear in the tables you will merge with.
    Besides standardizing datatypes (again for merge compatability) the only meaningful
    data alteration we employ here is removing leading zeros from numeric strings on
    the ``generator_id`` and ``emissions_unit_id_epa`` fields. This is because the same
    function is used to clean those same fields in all the other tables in which they
    appear. In order to merge properly, we need to clean the values in the crosswalk the
    same way. Lastly, we drop all rows without ``EIA_PLANT_ID`` (``plant_id_eia``)
    values because that means that they are unmatched and do not provide any useful
    information to users.

    It's important to note that the crosswalk is kept intact (and not seperated into
    smaller reference tables) because the relationship between the ids is not 1:1. For
    example, you can't isolate the plant_id fields, drop duplicates, and call it a day.
    The plant discrepancies depend on which generator ids it's referring to. This goes
    for all fields. Be careful, and do some due diligence before eliminating columns.

    We talk more about the complexities regarding EPA "units" in our :doc:`Data Source
    documentation page for EPACEMS </data_sources/epacems>`.

    It's also important to note that the crosswalk is a static file: there is no year
    field. The plant_id_eia and generator_id fields, however, are foreign keys from an
    annualized table. If the fast ETL is run (on one year of data) the test will break
    because the crosswalk tables with ``plant_id_eia`` and ``generator_id`` contain
    values from various years. To keep the crosswalk in alignment with the available eia
    data, we'll restrict it based on the generator entity table which has
    ``plant_id_eia`` and ``generator_id`` so long as it's not using the full suite of
    avilable years. If it is, we don't want to restrict the crosswalk so we can get
    warnings and errors from any foreign key discrepancies. This isn't an ideal
    solution, but it works for now.

    Args:
        epacamd_eia: The result of running this module's extract() function.
        generators_entity_eia: The generators_entity_eia table.
        boilers_entity_eia: The boilers_entitiy_eia table.
        processing_all_years: A boolean indicating whether the years from the
            Eia860Settings object match the EIA860 working partitions. This indicates
            whether or not to restrict the crosswalk data so the tests don't fail on
            foreign key restraints.

    Returns:
        A dictionary containing the cleaned EPACAMD-EIA crosswalk DataFrame.
    """
    logger.info("Transforming the EPACAMD-EIA crosswalk")

    column_rename = {
        "camd_plant_id": "plant_id_epa",
        "camd_unit_id": "emissions_unit_id_epa",
        "camd_generator_id": "generator_id_epa",
        "eia_plant_id": "plant_id_eia",
        "eia_boiler_id": "boiler_id",  # Eventually change to boiler_id_eia
        "eia_generator_id": "generator_id",  # Eventually change to generator_id_eia
    }

    # Basic column rename, selection, and dtype alignment.
    crosswalk_clean = (
        epacamd_eia.pipe(simplify_columns)
        .rename(columns=column_rename)
        .filter(list(column_rename.values()))
        .pipe(remove_leading_zeros_from_numeric_strings, col_name="generator_id")
        .pipe(remove_leading_zeros_from_numeric_strings, col_name="boiler_id")
        .pipe(
            remove_leading_zeros_from_numeric_strings, col_name="emissions_unit_id_epa"
        )
        .pipe(manually_update_subplant_id)
        .pipe(apply_pudl_dtypes, "eia")
        .dropna(subset=["plant_id_eia"])
    )

    # Restrict crosswalk for tests if running fast etl
    if not processing_all_eia_years:
        logger.info(
            "Selected subset of avilable EIA years--restricting EPACAMD-EIA Crosswalk \
            to chosen subset of EIA years"
        )
        crosswalk_clean = pd.merge(
            crosswalk_clean,
            generators_entity_eia[["plant_id_eia", "generator_id"]],
            on=["plant_id_eia", "generator_id"],
            how="inner",
        )
        crosswalk_clean = pd.merge(
            crosswalk_clean,
            boilers_entity_eia[["plant_id_eia", "boiler_id"]],
            on=["plant_id_eia", "boiler_id"],
            how="inner",
        )
    # TODO: Add mannual crosswalk
    return {"epacamd_eia": crosswalk_clean}


##################
# Add Sub-Plant ID
##################


def identify_subplants_epacamd_eia(
    crosswalk_clean: pd.DataFrame, years=None, states=None
) -> pd.DataFrame:
    """Identify sub-plant IDs, GTN regressions, and GTN ratios."""
    cems_ids = unique_plant_ids_epacems(years, states)
    # add subplant ids to the data
    crosswalk_w_subplant_ids = generate_subplant_ids(years, cems_ids, crosswalk_clean)
    return crosswalk_w_subplant_ids


def unique_plant_ids_epacems(
    epacems_path: Path | None = None,
    years: list[int] | None = None,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """Make unique annual plant_id_eia and emissions_unit_id_epa.

    Returns:
        dataframe with unique set of: "plant_id_eia", "report_year" and
        "emissions_unit_id_epa"
    """
    logger.info("loading CEMS ids")
    if epacems_path is None:
        pudl_settings = pudl.workspace.setup.get_defaults()
        epacems_path = (
            Path(pudl_settings["parquet_dir"]) / "hourly_emissions_epacems.parquet"
        )
    annual_id_cols = ["plant_id_eia", "report_year", "emissions_unit_id_epa"]
    epacems_dd = dd.read_parquet(
        epacems_path,
        use_nullable_dtypes=True,
        engine="pyarrow",
        index=False,
        split_row_groups=True,
        filters=year_state_filter(years, states),
        columns=annual_id_cols,
    )
    epacems_df = epacems_dd.compute().drop_duplicates()

    return epacems_df


def augement_crosswalk_with_eia860(
    crosswalk_clean: pd.DataFrame, generators_eia860: pd.DataFrame
) -> pd.DataFrame:
    """Merge any plants that are missing from the EPA crosswalk but appear in EIA-860.

    Args:
        crosswalk_clean: transformed EPA CEMS-EIA crosswalk
        generators_eia860: EIA860 generators table
    """
    crosswalk_clean = crosswalk_clean.merge(
        generators_eia860[["plant_id_eia", "generator_id", "report_date"]],
        how="outer",
        on=["plant_id_eia", "generator_id", "report_date"],
        validate="m:1",
    )
    crosswalk_clean["plant_id_epa"] = crosswalk_clean["plant_id_epa"].fillna(
        crosswalk_clean["plant_id_eia"]
    )
    return crosswalk_clean


def generate_subplant_ids(cems_ids, crosswalk_clean, generators_eia860):
    """Groups units and generators into unique subplant groups.

    This function consists of three primary parts:
    1. Identify a list of all unique plant-units that exist in the CEMS data
        for the years in question. This will be used to filter the crosswalk.
    2. Load the EPA-EIA crosswalk and filter it based on the units that exist
        in the CEMS data for the years in question
    3. Use graph analysis to identify distinct groupings of EPA units and EIA
        generators based on 1:1, 1:m, m:1, or m:m relationships.

    Returns:
        exports the subplant crosswalk to a csv file
        cems_ids and gen_fuel_allocated with subplant_id added
    """
    logger.info("Identify unique subplants within the EPA CEMS crosswalk")

    crosswalk_clean = augement_crosswalk_with_eia860(crosswalk_clean, generators_eia860)
    # filter the crosswalk to drop any units that don't exist in CEMS
    filtered_crosswalk = pudl.analysis.epacamd.filter_crosswalk(
        crosswalk_clean, cems_ids
    )
    # use graph analysis to identify subplants
    crosswalk_with_subplant_ids = pudl.analysis.epacamd.make_subplant_ids(
        filtered_crosswalk
    )

    # change the eia plant id to int
    crosswalk_with_subplant_ids["plant_id_eia"] = crosswalk_with_subplant_ids[
        "plant_id_eia"
    ].astype("Int64")

    # change the order of the columns
    crosswalk_with_subplant_ids = crosswalk_with_subplant_ids[
        [
            "plant_id_epa",
            "emissions_unit_id_epa",
            "plant_id_eia",
            "generator_id",
            "subplant_id",
        ]
    ]

    # update the subplant_crosswalk to ensure completeness
    # prepare the subplant crosswalk by adding a complete list of generators and adding the unit_id_pudl column

    # TODO: Is this actually the right table here? Should we migrate the unit_id_pudl
    # complete fleshing out from outputs -> transform? Is the unit_id_pudl annually varying?
    complete_generator_ids = generators_eia860[
        ["plant_id_eia", "generator_id", "unit_id_pudl", "report_date"]
    ].drop_duplicates()
    subplant_crosswalk_complete = crosswalk_with_subplant_ids.merge(
        complete_generator_ids,
        how="outer",
        on=["plant_id_eia", "generator_id", "report_date"],
        validate="m:1",
    )
    # also add a complete list of cems emissions_unit_id_epa
    subplant_crosswalk_complete = subplant_crosswalk_complete.merge(
        cems_ids[
            ["plant_id_eia", "report_date", "emissions_unit_id_epa"]
        ].drop_duplicates(),
        how="outer",
        on=["plant_id_eia", "report_date", "emissions_unit_id_epa"],
        validate="m:1",
    )
    # update the subplant ids for each plant
    subplant_crosswalk_complete = subplant_crosswalk_complete.groupby(
        "plant_id_eia", "report_date"
    ).apply(update_subplant_ids)

    # remove the intermediate columns created by update_subplant_ids
    subplant_crosswalk_complete["subplant_id"].update(
        subplant_crosswalk_complete["new_subplant"]
    )
    subplant_crosswalk_complete = subplant_crosswalk_complete.reset_index(drop=True)[
        [
            "plant_id_epa",
            "emissions_unit_id_epa",
            "plant_id_eia",
            "generator_id",
            "subplant_id",
            "unit_id_pudl",
        ]
    ]

    subplant_crosswalk_complete = manually_update_subplant_id(
        subplant_crosswalk_complete
    )

    subplant_crosswalk_complete = subplant_crosswalk_complete.drop_duplicates(
        subset=[
            "plant_id_epa",
            "emissions_unit_id_epa",
            "plant_id_eia",
            "generator_id",
            "subplant_id",
        ],
        keep="last",
    )

    # add proposed operating dates and retirements to the subplant id crosswalk
    crosswalk_with_subplant_ids = add_operating_and_retirement_dates(
        subplant_crosswalk_complete, generators_eia860
    )
    return crosswalk_with_subplant_ids


def manually_update_subplant_id(subplant_crosswalk):
    """Correct subplant mappings not caught by update_subplant_id.

    This is temporary until the pudl subplant crosswalk includes boiler-generator id
    matches.
    """
    # set all generators in plant 1391 to the same subplant
    subplant_crosswalk.loc[
        subplant_crosswalk["plant_id_eia"] == 1391, "subplant_id"
    ] = 0

    return subplant_crosswalk


def update_subplant_ids(subplant_crosswalk):
    """Ensure a complete and accurate subplant_id mapping for all generators.

    This function is meant to be applied using a .groupby("plant_id_eia").apply()
    function. This function will only properly work when applied to a single
    ``plant_id_eia`` at a time.

    Data Preparation
        Because the existing subplant_id crosswalk was only meant to map CAMD units to
        EIA generators, it is missing a large number of subplant_ids for generators that
        do not report to CEMS. Before applying this function to the subplant crosswalk,
        the crosswalk must be completed with all generators by outer merging in the
        complete list of generators from EIA-860 (specifically the gens_eia860 table
        from pudl). This dataframe also contains the complete list of ``unit_id_pudl``
        mappings that will be necessary.

    High-level overview of method:
        1. Use the PUDL subplant_id if available. In the case where a unit_id_pudl
           groups several subplants, we overwrite these multiple existing subplant_id
           with a single subplant_id.
        2. Where there is no PUDL subplant_id, we use the unit_id_pudl to assign a
           unique subplant_id
        3. Where there is neither a pudl subplant_id nor unit_id_pudl, we use the
           generator ID to assign a unique subplant_id
        4. All of the new unique ids are renumbered in consecutive ascending order

    Detailed explanation of steps:
        1. Because the current subplant_id code does not take boiler-generator associations into account,
           there may be instances where the code assigns generators to different subplants when in fact, according
           to the boiler-generator association table, these generators are grouped into a single unit based on their
           boiler associations. The first step of this function is thus to identify if multiple subplant_id have
           been assigned to a single unit_id_pudl. If so, we replace the existing subplant_ids with a single subplant_id.
           For example, if a generator A was assigned subplant_id 0 and generator B was assigned subplant_id 1, but
           both generators A and B are part of unit_id_pudl 1, we would re-assign the subplant_id to both generators to
           0 (we always use the lowest number subplant_id in each unit_id_pudl group). This may result in some subplant_id
           being skipped, but this is okay because we will later renumber all subplant ids (i.e. if there were also a
           generator C with subplant_id 2, there would no be no subplant_id 1 at the plant)
           Likewise, sometimes multiple unit_id_pudl are connected to a single subplant_id, so we also correct the
           unit_id_pudl basedon these connections.
        2. The second issue is that there are many NA subplant_id that we should fill. To do this, we first look at
           unit_id_pudl. If a group of generators are assigned a unit_id_pudl but have NA subplant_ids, we assign a single
           new subplant_id to this group of generators. If there are still generators at a plant that have both NA subplant_id
           and NA unit_id_pudl, we for now assume that each of these generators consitutes its own subplant. We thus assign a unique
           subplant_id to each generator that is unique from any existing subplant_id already at the plant.
           In the case that there are multiple emissions_unit_id_epa at a plant that are not matched to any other identifiers (generator_id,
           unit_id_pudl, or subplant_id), as is the case when there are units that report to CEMS but which do not exist in the EIA
           data, we assign these units to a single subplant.

    Args:
        subplant_crosswalk: a dataframe containing the output of
            :func:`pudl.analysis.epacamd.make_subplant_ids`
    """
    # Step 1: Create corrected versions of subplant_id and unit_id_pudl
    # if multiple unit_id_pudl are connected by a single subplant_id, unit_id_pudl_connected groups these unit_id_pudl together
    subplant_crosswalk = connect_ids(
        subplant_crosswalk, id_to_update="unit_id_pudl", connecting_id="subplant_id"
    )
    # if multiple subplant_id are connected by a single unit_id_pudl, group these subplant_id together
    subplant_crosswalk = connect_ids(
        subplant_crosswalk, id_to_update="subplant_id", connecting_id="unit_id_pudl"
    )

    # Step 2: Fill missing subplant_id
    # We will use unit_id_pudl to fill missing subplant ids, so first we need to fill any missing unit_id_pudl
    # We do this by assigning a new unit_id_pudl to each generator that isn't already grouped into a unit

    # create a numeric version of each generator_id
    # ngroup() creates a unique number for each element in the group
    subplant_crosswalk["numeric_generator_id"] = subplant_crosswalk.groupby(
        ["plant_id_eia", "report_date" "generator_id"], dropna=False
    ).ngroup()
    # when filling in missing unit_id_pudl, we don't want these numeric_generator_id to overlap existing unit_id
    # to ensure this, we will add 1000 to each of these numeric generator ids to ensure they are unique
    # 1000 was chosen as an arbitrarily high number, since the largest unit_id_pudl is ~ 10.
    subplant_crosswalk["numeric_generator_id"] = (
        subplant_crosswalk["numeric_generator_id"] + 1000
    )
    # fill any missing unit_id_pudl with a number for each unique generator
    subplant_crosswalk["unit_id_pudl_filled"] = (
        subplant_crosswalk["unit_id_pudl_connected"]
        .fillna(subplant_crosswalk["subplant_id_connected"] + 100)
        .fillna(subplant_crosswalk["numeric_generator_id"])
    )
    # create a new unique subplant_id based on the connected subplant ids and the filled unit_id
    subplant_crosswalk["new_subplant"] = subplant_crosswalk.groupby(
        ["plant_id_eia", "report_date", "subplant_id_connected", "unit_id_pudl_filled"],
        dropna=False,
    ).ngroup()

    return subplant_crosswalk


def connect_ids(df, id_to_update, connecting_id):
    """Corrects an id value if it is connected by an id value in another column.

    If multiple subplant_id are connected by a single unit_id_pudl, this groups these
    subplant_id together. If multiple unit_id_pudl are connected by a single subplant_id,
    this groups these unit_id_pudl together.

    Args:
        df: dataframe containing columns with id_to_update and connecting_id columns
        subplant_unit_pairs
        id_to_update: List of ID columns
        connecting_id: ID column
    """
    # get a table with all unique subplant to unit pairs
    subplant_unit_pairs = df[
        ["plant_id_eia", "subplant_id", "unit_id_pudl"]
    ].drop_duplicates()

    # identify if any non-NA id_to_update are duplicated, indicated that it is associated with multiple connecting_id
    duplicates = subplant_unit_pairs[
        (subplant_unit_pairs.duplicated(subset=id_to_update, keep=False))
        & (~subplant_unit_pairs[id_to_update].isna())
    ].copy()

    # if there are any duplicate units, indicating an incorrect id_to_update, fix the id_to_update
    df[f"{connecting_id}_connected"] = df[connecting_id]
    if len(duplicates) > 0:
        # find the lowest number subplant id associated with each duplicated unit_id_pudl
        duplicates.loc[:, f"{connecting_id}_to_replace"] = (
            duplicates.groupby(["plant_id_eia", id_to_update])[connecting_id]
            .min()
            .iloc[0]
        )
        # merge this replacement subplant_id into the dataframe and use it to update the existing subplant id
        df = df.merge(
            duplicates,
            how="left",
            on=["plant_id_eia", id_to_update, connecting_id],
            validate="m:1",
        )
        df[f"{connecting_id}_connected"].update(df[f"{connecting_id}_to_replace"])
    return df


def add_operating_and_retirement_dates(df, generators_eia860):
    """Adds generators' planned operating date and/or retirement date."""
    generator_status = generators_eia860.loc[
        :,
        [
            "plant_id_eia",
            "generator_id",
            "report_date",
            "operational_status",
            "current_planned_operating_date",
            "retirement_date",
        ],
    ]
    # only keep values that have a planned operating date or retirement date
    generator_status = generator_status[
        (~generator_status["current_planned_operating_date"].isna())
        | (~generator_status["retirement_date"].isna())
    ]
    # drop any duplicate entries
    logger.info(f"TEMP: len of generator_status pre dupe drop: {len(generator_status)}")
    generator_status = generator_status.sort_values(
        by=["plant_id_eia", "generator_id", "report_date"]
    ).drop_duplicates(
        subset=[
            "plant_id_eia",
            "generator_id",
            "report_date",  # added by cbz
            "current_planned_operating_date",
            "retirement_date",
        ],
        keep="last",
    )
    logger.info(
        f"TEMP: len of generator_status post dupe drop: {len(generator_status)}"
    )

    # merge the dates into the crosswalk
    df = df.merge(
        generator_status[
            [
                "plant_id_eia",
                "generator_id",
                "report_date",
                "current_planned_operating_date",
                "retirement_date",
            ]
        ],
        how="left",
        on=["plant_id_eia", "generator_id", "report_date"],
        validate="m:1",
    )

    return df
