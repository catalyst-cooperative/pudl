"""EPA CEMS Hourly Emissions assets.

The :func:`hourly_emissions_epacems` asset defined in this module uses a dagster pattern
that is unique from other PUDL assets. The underlying architecture uses ops to create a
dynamic graph
which is wrapped by a special asset called a graph backed asset that creates an asset
from a graph of ops. The dynamic graph will allow dagster to dynamically generate an op
for processing each year of EPA CEMS data and execute these ops in parallel. For more information
see: https://docs.dagster.io/concepts/ops-jobs-graphs/dynamic-graphs and https://docs.dagster.io/concepts/assets/graph-backed-assets.
"""
from collections import namedtuple
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetIn, DynamicOut, DynamicOutput, Field, asset, graph_asset, op

import pudl
from pudl.helpers import EnvVar
from pudl.metadata.classes import Resource

logger = pudl.logging_helpers.get_logger(__name__)


YearPartitions = namedtuple("YearPartitions", ["year", "states"])


@op(
    out=DynamicOut(),
    required_resource_keys={"dataset_settings"},
)
def get_years_from_settings(context):
    """Return set of years in settings.

    These will be used to kick off worker processes to process each year of data in
    parallel.
    """
    epacems_settings = context.resources.dataset_settings.epacems
    for year in epacems_settings.years:
        yield DynamicOutput(year, mapping_key=str(year))


@op(
    required_resource_keys={"datastore", "dataset_settings"},
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    },
)
def process_single_year(
    context,
    year,
    clean_epacamd_eia: pd.DataFrame,
    plants_entity_eia: pd.DataFrame,
) -> YearPartitions:
    """Process a single year of EPA CEMS data.

    Args:
        context: dagster keyword that provides access to resources and config.
        year: Year of data to process.
        clean_epacamd_eia: The EPA EIA crosswalk table used for harmonizing the
                     ORISPL code with EIA.
        plants_entity_eia: The EIA Plant entities used for aligning timezones.
    """
    ds = context.resources.datastore
    epacems_settings = context.resources.dataset_settings.epacems

    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()
    partitioned_path = (
        Path(context.op_config["pudl_output_path"]) / "hourly_emissions_epacems"
    )
    partitioned_path.mkdir(exist_ok=True)

    for state in epacems_settings.states:
        logger.info(f"Processing EPA CEMS hourly data for {year}-{state}")
        df = pudl.extract.epacems.extract(year=year, state=state, ds=ds)
        df = pudl.transform.epacems.transform(df, clean_epacamd_eia, plants_entity_eia)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # Write to a directory of partitioned parquet files
        with pq.ParquetWriter(
            where=partitioned_path / f"epacems-{year}-{state}.parquet",
            schema=schema,
            compression="snappy",
            version="2.6",
        ) as partitioned_writer:
            partitioned_writer.write_table(table)

    return YearPartitions(year, epacems_settings.states)


@op(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    },
)
def consolidate_partitions(context, partitions: list[YearPartitions]) -> None:
    """Read partitions into memory and write to a single monolithic output.

    Args:
        context: dagster keyword that provides access to resources and config.
        partitions: Year and state combinations in the output database.
    """
    partitioned_path = (
        Path(context.op_config["pudl_output_path"]) / "hourly_emissions_epacems"
    )
    monolithic_path = (
        Path(context.op_config["pudl_output_path"]) / "hourly_emissions_epacems.parquet"
    )
    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()

    with pq.ParquetWriter(
        where=monolithic_path, schema=schema, compression="snappy", version="2.6"
    ) as monolithic_writer:
        for year, states in partitions:
            for state in states:
                monolithic_writer.write_table(
                    pq.read_table(
                        source=partitioned_path / f"epacems-{year}-{state}.parquet",
                        schema=schema,
                    )
                )


@graph_asset
def hourly_emissions_epacems(
    clean_epacamd_eia: pd.DataFrame, plants_entity_eia: pd.DataFrame
) -> None:
    """Extract, transform and load CSVs for EPA CEMS.

    This asset creates a dynamic graph of ops to process EPA CEMS data in parallel. It
    will create both a partitioned and single monolithic parquet output. For more
    information see: https://docs.dagster.io/concepts/ops-jobs-graphs/dynamic-graphs.
    """
    years = get_years_from_settings()
    partitions = years.map(
        lambda year: process_single_year(
            year,
            clean_epacamd_eia,
            plants_entity_eia,
        )
    )
    return consolidate_partitions(partitions.collect())


#####################
# EPA - EIA Crosswalk
#####################


@asset(required_resource_keys={"datastore"})
def raw_epacamd_eia(context) -> pd.DataFrame:
    """Extract the EPACAMD-EIA Crosswalk from the Datastore."""
    logger.info("Extracting the EPACAMD-EIA crosswalk from Zenodo")
    ds = context.resources.datastore
    with ds.get_zipfile_resource("epacamd_eia", name="epacamd_eia.zip").open(
        "camd-eia-crosswalk-master/epa_eia_crosswalk.csv"
    ) as f:
        return pd.read_csv(f)


@asset(
    required_resource_keys={"dataset_settings"},
)
def clean_epacamd_eia(
    context,
    raw_epacamd_eia: pd.DataFrame,
    generators_entity_eia: pd.DataFrame,
    boilers_entity_eia: pd.DataFrame,
) -> pd.DataFrame:
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
        context: dagster keyword that provides access to resources and config. For this
            asset, this determines whether the years from the Eia860Settings object
            match the EIA860 working partitions. This indicates whether or not to
            restrict the crosswalk data so the tests don't fail on foreign key
            restraints.
        raw_epacamd_eia: The result of running this module's extract() function.
        generators_entity_eia: The generators_entity_eia table.
        boilers_entity_eia: The boilers_entitiy_eia table.

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
        raw_epacamd_eia.pipe(pudl.helpers.simplify_columns)
        .rename(columns=column_rename)
        .filter(list(column_rename.values()))
        .pipe(
            pudl.helpers.remove_leading_zeros_from_numeric_strings,
            col_name="generator_id",
        )
        .pipe(
            pudl.helpers.remove_leading_zeros_from_numeric_strings, col_name="boiler_id"
        )
        .pipe(
            pudl.helpers.remove_leading_zeros_from_numeric_strings,
            col_name="emissions_unit_id_epa",
        )
        .pipe(pudl.metadata.fields.apply_pudl_dtypes, "eia")
        .dropna(subset=["plant_id_eia"])
        .pipe(correct_epa_eia_plant_id_mapping)
    )
    dataset_settings = context.resources.dataset_settings
    processing_all_eia_years = (
        dataset_settings.eia.eia860.years
        == dataset_settings.eia.eia860.data_source.working_partitions["years"]
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
    # TODO: Add manual crosswalk cleanup from @grgmiller
    return crosswalk_clean


def correct_epa_eia_plant_id_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Mannually correct one plant ID.

    The EPA's power sector data crosswalk incorrectly maps plant_id_epa 55248 to
    plant_id_eia 55248, when it should be mapped to id 2847.
    """
    df.loc[df["plant_id_eia"] == 55248, "plant_id_eia"] = 2847

    return df


##################
# Add Sub-Plant ID
##################


@asset(
    ins={
        "hourly_emissions_epacems": AssetIn(input_manager_key="epacems_io_manager"),
    }
)
def emissions_unit_ids_epacems(
    hourly_emissions_epacems: dd.DataFrame,
) -> pd.DataFrame:
    """Make unique annual plant_id_eia and emissions_unit_id_epa.

    Returns:
        dataframe with unique set of: "plant_id_eia", "year" and "emissions_unit_id_epa"
    """
    epacems_ids = (
        hourly_emissions_epacems[["plant_id_eia", "year", "emissions_unit_id_epa"]]
        .drop_duplicates()
        .compute()
    )

    return epacems_ids


@asset(io_manager_key="pudl_sqlite_io_manager")
def epacamd_eia_subplant_ids(
    clean_epacamd_eia: pd.DataFrame,
    generators_eia860: pd.DataFrame,
    emissions_unit_ids_epacems: pd.DataFrame,
    boiler_generator_assn_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Groups units and generators into unique subplant groups.

    This function consists of three primary parts:

    #.  Augement the EPA CAMD:EIA crosswalk with all IDs from EIA and EPA CAMD. Fill in
        key IDs when possible. Because the published crosswalk was only meant to map
        CAMD units to EIA generators, it is missing a large number of subplant_ids for
        generators that do not report to CEMS. Before applying this function to the
        subplant crosswalk, the crosswalk must be completed with all generators by outer
        merging in the complete list of generators from EIA-860. This dataframe also
        contains the complete list of ``unit_id_pudl`` mappings that will be necessary.
    #.  :ref:`make_subplant_ids`: Use graph analysis to identify distinct groupings of
        EPA units and EIA generators based on 1:1, 1:m, m:1, or m:m relationships.
    #.  :func:`update_subplant_ids`: Augment the ``subplant_id`` with the
        ``unit_id_pudl`` and ``generator_id``.

    Returns:
        table of cems_ids and with subplant_id added
    """
    # Ensure ALL relevant IDs are present. Basically just merge in all the IDs
    # Later note: As of April 2023, there is an experimental augmentation of the
    # unit_id_pudl living in pudl.output.eia860.assign_unit_ids. It is currently non-
    # functioning (#2535) but when it is, ensure that it gets plugged into the dag
    # BEFORE this step so the subplant IDs can benefit from the more fleshed out units
    clean_epacamd_eia = (
        augement_crosswalk_with_generators_eia860(clean_epacamd_eia, generators_eia860)
        .pipe(augement_crosswalk_with_epacamd_ids, emissions_unit_ids_epacems)
        .pipe(augement_crosswalk_with_bga_eia860, boiler_generator_assn_eia860)
    )
    # use graph analysis to identify subplants
    subplant_crosswalk_complete = pudl.analysis.epacamd_eia.make_subplant_ids(
        clean_epacamd_eia
    ).pipe(pudl.metadata.fields.apply_pudl_dtypes, "eia")

    # update the subplant ids for each plant
    subplant_crosswalk_complete = subplant_crosswalk_complete.groupby(
        by=["plant_id_eia"], group_keys=False
    ).apply(update_subplant_ids)
    # remove the intermediate columns created by update_subplant_ids
    subplant_crosswalk_complete["subplant_id"].update(
        subplant_crosswalk_complete["new_subplant"]
    )
    subplant_crosswalk_complete = manually_update_subplant_id(
        subplant_crosswalk_complete
    )

    # check for duplicates in sudo-PKs. These are not the actual PKs because there are
    # some nulls in generator_id, so this won't be checked during the db construction
    if (
        epacamd_eia_dupe_mask := subplant_crosswalk_complete.duplicated(
            subset=[
                "plant_id_epa",
                "emissions_unit_id_epa",
                "plant_id_eia",
                "generator_id",
                "subplant_id",
            ]
        )
    ).any():
        raise AssertionError(
            "Duplicates found in sudo primary keys of EPA CAMD/EIA subplant ID table "
            "when none are expected. Duplicates found: \n"
            f"{subplant_crosswalk_complete[epacamd_eia_dupe_mask]}"
        )
    return subplant_crosswalk_complete.reset_index(drop=True)


def augement_crosswalk_with_generators_eia860(
    crosswalk_clean: pd.DataFrame, generators_eia860: pd.DataFrame
) -> pd.DataFrame:
    """Merge any plants that are missing from the EPA crosswalk but appear in EIA-860.

    Args:
        crosswalk_clean: transformed EPA CEMS-EIA crosswalk.
        generators_eia860: EIA860 generators table.
    """
    crosswalk_clean = crosswalk_clean.merge(
        generators_eia860[["plant_id_eia", "generator_id"]].drop_duplicates(),
        how="outer",
        on=["plant_id_eia", "generator_id"],
        validate="m:1",
    )
    crosswalk_clean["plant_id_epa"] = crosswalk_clean["plant_id_epa"].fillna(
        crosswalk_clean["plant_id_eia"]
    )
    return crosswalk_clean


def augement_crosswalk_with_epacamd_ids(
    crosswalk_clean: pd.DataFrame, emissions_unit_ids_epacems: pd.DataFrame
) -> pd.DataFrame:
    """Merge all EPA CAMD IDs into the crosswalk."""
    return crosswalk_clean.assign(
        emissions_unit_id_epa=lambda x: x.emissions_unit_id_epa.fillna(x.generator_id)
    ).merge(
        emissions_unit_ids_epacems[
            ["plant_id_eia", "emissions_unit_id_epa"]
        ].drop_duplicates(),
        how="outer",
        on=["plant_id_eia", "emissions_unit_id_epa"],
        validate="m:m",
    )


def augement_crosswalk_with_bga_eia860(
    crosswalk_clean: pd.DataFrame, boiler_generator_assn_eia860: pd.DataFrame
) -> pd.DataFrame:
    """Merge all EIA Unit IDs into the crosswalk."""
    return crosswalk_clean.merge(
        boiler_generator_assn_eia860[
            ["plant_id_eia", "generator_id", "unit_id_pudl"]
        ].drop_duplicates(),
        how="outer",
        on=["plant_id_eia", "generator_id"],
        validate="m:1",
    )


def update_subplant_ids(subplant_crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Ensure a complete and accurate subplant_id mapping for all generators.

    This function is meant to be applied using a ``.groupby("plant_id_eia").apply()``
    function. This function will only properly work when applied to a single
    ``plant_id_eia`` at a time.

    High-level overview of method:
    ==============================

    #.  Use the ``subplant_id`` derived from :ref:`make_subplant_ids` if available. In
        the case where a ``unit_id_pudl`` groups several subplants, we overwrite these
        multiple existing subplant_id with a single ``subplant_id``.
    #.  Where there is no ``subplant_id`` derived from :ref:`make_subplant_ids`, we use
        the ``unit_id_pudl`` to assign a unique ``subplant_id``
    #.  Where there is neither a pudl ``subplant_id`` nor ``unit_id_pudl``, we use the
        ``generator_id`` to assign a unique ``subplant_id``.
    #.  All of the new unique ids are renumbered in consecutive ascending order

    Detailed explanation of steps:
    ==============================

    #.  Because the current subplant_id code does not take boiler-generator associations
        into account, there may be instances where the code assigns generators to
        different subplants when in fact, according to the boiler-generator association
        table, these generators are grouped into a single unit based on their boiler
        associations. The first step of this function is thus to identify if multiple
        subplant_id have been assigned to a single unit_id_pudl. If so, we replace the
        existing subplant_ids with a single subplant_id. For example, if a generator A
        was assigned subplant_id 0 and generator B was assigned subplant_id 1, but both
        generators A and B are part of unit_id_pudl 1, we would re-assign the subplant_id
        to both generators to 0 (we always use the lowest number subplant_id in each
        unit_id_pudl group). This may result in some subplant_id being skipped, but this
        is okay because we will later renumber all subplant ids (i.e. if there were also
        a generator C with subplant_id 2, there would no be no subplant_id 1 at the
        plant). Likewise, sometimes multiple unit_id_pudl are connected to a single
        subplant_id, so we also correct the unit_id_pudl basedon these connections.
    #.  The second issue is that there are many NA subplant_id that we should fill. To
        do this, we first look at unit_id_pudl. If a group of generators are assigned a
        unit_id_pudl but have NA subplant_ids, we assign a single new subplant_id to this
        group of generators. If there are still generators at a plant that have both NA
        subplant_id and NA unit_id_pudl, we for now assume that each of these generators
        consitutes its own subplant. We thus assign a unique subplant_id to each
        generator that is unique from any existing subplant_id already at the plant. In
        the case that there are multiple emissions_unit_id_epa at a plant that are not
        matched to any other identifiers (generator_id, unit_id_pudl, or subplant_id), as
        is the case when there are units that report to CEMS but which do not exist in
        the EIA data, we assign these units to a single subplant.

    Args:
        subplant_crosswalk: a dataframe containing the output of
            :func:`pudl.analysis.epacamd_eia.make_subplant_ids`
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
    # when filling in missing unit_id_pudl, we don't want these numeric_generator_id to
    # overlap existing unit_id
    plant_gen_gb = subplant_crosswalk.groupby(
        ["plant_id_eia", "generator_id"], dropna=False
    )
    plant_gb = subplant_crosswalk.groupby(["plant_id_eia"], dropna=False)
    subplant_crosswalk = subplant_crosswalk.assign(
        numeric_generator_id=(
            plant_gen_gb.ngroup() + plant_gb.unit_id_pudl.transform(max)
        ),
        unit_id_pudl_filled=(
            lambda x: x.unit_id_pudl_connected.fillna(
                x.subplant_id_connected + plant_gb.unit_id_pudl_connected.transform(max)
            ).fillna(x.numeric_generator_id)
        ),
    )

    # create a new unique subplant_id based on the connected subplant ids and the filled unit_id
    subplant_crosswalk["new_subplant"] = subplant_crosswalk.groupby(
        ["plant_id_eia", "subplant_id_connected", "unit_id_pudl_filled"],
        dropna=False,
    ).ngroup()

    return subplant_crosswalk


def connect_ids(
    subplant_crosswalk: pd.DataFrame, id_to_update: str, connecting_id: str
) -> pd.DataFrame:
    """Corrects an id value if it is connected by an id value in another column.

    If multiple subplant_id are connected by a single unit_id_pudl, this groups these
    subplant_id together. If multiple unit_id_pudl are connected by a single subplant_id,
    this groups these unit_id_pudl together.

    Args:
        subplant_crosswalk: dataframe containing columns of id_to_update andconnecting_id
        id_to_update: List of ID columns
        connecting_id: ID column
    """
    # get a table with all unique subplant to unit pairs
    subplant_unit_pairs = subplant_crosswalk[
        ["plant_id_eia", "subplant_id", "unit_id_pudl"]
    ].drop_duplicates()

    # identify if any non-NA id_to_update are duplicated, indicated that it is associated with multiple connecting_id
    duplicates = subplant_unit_pairs[
        (subplant_unit_pairs.duplicated(subset=id_to_update, keep=False))
        & (~subplant_unit_pairs[id_to_update].isna())
    ].copy()

    # if there are any duplicate units, indicating an incorrect id_to_update, fix the id_to_update
    subplant_crosswalk[f"{connecting_id}_connected"] = subplant_crosswalk[connecting_id]
    if len(duplicates) > 0:
        # find the lowest number subplant id associated with each duplicated unit_id_pudl
        duplicates.loc[:, f"{connecting_id}_to_replace"] = (
            duplicates.groupby(["plant_id_eia", id_to_update])[connecting_id]
            .min()
            .iloc[0]
        )
        # merge this replacement subplant_id into the dataframe and use it to update the existing subplant id
        subplant_crosswalk = subplant_crosswalk.merge(
            duplicates,
            how="left",
            on=["plant_id_eia", id_to_update, connecting_id],
            validate="m:1",
        )
        subplant_crosswalk[f"{connecting_id}_connected"].update(
            subplant_crosswalk[f"{connecting_id}_to_replace"]
        )
    return subplant_crosswalk


def manually_update_subplant_id(subplant_crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Mannually update the subplant_id for ``plant_id_eia`` 1391.

    This function lumps all records within ``plant_id_eia`` 1391 into the same
    ``subplant_id`` group. See `comment<https://github.com/singularity-energy/open-grid-emissions/pull/142#issuecomment-1186579260>_`
    for expanation of why.
    """
    # set all generators in plant 1391 to the same subplant
    subplant_crosswalk.loc[
        subplant_crosswalk["plant_id_eia"] == 1391, "subplant_id"
    ] = 0

    return subplant_crosswalk
