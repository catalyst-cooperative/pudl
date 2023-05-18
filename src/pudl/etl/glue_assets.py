"""FERC and EIA and EPA CAMD glue assets."""
import networkx as nx
import pandas as pd
from dagster import AssetOut, Output, asset, multi_asset

import pudl
from pudl.metadata.classes import Package

logger = pudl.logging_helpers.get_logger(__name__)


# TODO (bendnorman): Currently loading all glue tables. Could potentially allow users
# to load subsets of the glue tables, see: https://docs.dagster.io/concepts/assets/multi-assets#subsetting-multi-assets
# Could split out different types of glue tables into different assets. For example the cross walk table could be a separate asset
# that way dagster doesn't think all glue tables depend on generators_entity_eia, boilers_entity_eia.


@multi_asset(
    outs={
        table_name: AssetOut(io_manager_key="pudl_sqlite_io_manager")
        for table_name in Package.get_etl_group_tables("glue")
        #  do not load epacamd_eia glue assets bc they are stand-alone assets below.
        if "epacamd_eia" not in table_name
    },
    required_resource_keys={"datastore", "dataset_settings"},
)
def create_glue_tables(context):
    """Extract, transform and load CSVs for the FERC-EIA Glue tables.

    Args:
        context: dagster keyword that provides access to resources and config.
        generators_entity_eia: Static generator attributes compiled from across the EIA-860 and EIA-923 data.
        boilers_entity_eia: boilers_entity_eia.

    Returns:
        A dictionary of DataFrames whose keys are the names of the corresponding
        database table.
    """
    dataset_settings = context.resources.dataset_settings
    # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(
        ferc1=dataset_settings.glue.ferc1,
        eia=dataset_settings.glue.eia,
    )

    # Ensure they are sorted so they match up with the asset outs
    glue_dfs = dict(sorted(glue_dfs.items()))

    return (
        Output(output_name=table_name, value=df) for table_name, df in glue_dfs.items()
    )


#####################
# EPA - EIA Crosswalk
#####################


@asset(required_resource_keys={"datastore"})
def raw_epacamd_eia(context) -> pd.DataFrame:
    """Extract the EPACAMD-EIA Crosswalk from the Datastore."""
    logger.info("Extracting the EPACAMD-EIA crosswalk from Zenodo")
    csv_map = {
        2018: "camd-eia-crosswalk-master/epa_eia_crosswalk.csv",
        2021: "camd-eia-crosswalk-2021-main/epa_eia_crosswalk.csv",
    }

    ds = context.resources.datastore
    year_matches = []
    for year, csv_path in csv_map.items():
        with ds.get_zipfile_resource("epacamd_eia", year=year).open(csv_path) as f:
            df = pd.read_csv(f)
            df["report_year"] = year
            year_matches.append(df)

    return pd.concat(year_matches, ignore_index=True)


@asset(
    required_resource_keys={"dataset_settings"}, io_manager_key="pudl_sqlite_io_manager"
)
def epacamd_eia(
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
        "report_year": "report_year",
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


##############################
# Add EPACAMD-EIA Sub-Plant ID
##############################


@asset(io_manager_key="pudl_sqlite_io_manager")
def epacamd_eia_subplant_ids(
    epacamd_eia: pd.DataFrame,
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
    #.  :func:`make_subplant_ids`: Use graph analysis to identify distinct groupings of
        EPA units and EIA generators based on 1:1, 1:m, m:1, or m:m relationships.
    #.  :func:`update_subplant_ids`: Augment the ``subplant_id`` with the
        ``unit_id_pudl`` and ``generator_id``.

    Returns:
        table of cems_ids and with subplant_id added
    """
    # epacamd_eia contains matches found from multiple years of data.
    # Many of these matches will be duplicated between years, so these are dropped.
    epacamd_eia = epacamd_eia.drop(["report_year"], axis=1).drop_duplicates()

    # Ensure ALL relevant IDs are present. Basically just merge in all the IDs
    # Later note: As of April 2023, there is an experimental augmentation of the
    # unit_id_pudl living in pudl.output.eia860.assign_unit_ids. It is currently non-
    # functioning (#2535) but when it is, ensure that it gets plugged into the dag
    # BEFORE this step so the subplant IDs can benefit from the more fleshed out units
    epacamd_eia_complete = (
        augement_crosswalk_with_generators_eia860(epacamd_eia, generators_eia860)
        .pipe(augement_crosswalk_with_epacamd_ids, emissions_unit_ids_epacems)
        .pipe(augement_crosswalk_with_bga_eia860, boiler_generator_assn_eia860)
    )
    # use graph analysis to identify subplants
    subplant_ids = make_subplant_ids(epacamd_eia_complete).pipe(
        pudl.metadata.fields.apply_pudl_dtypes, "glue"
    )
    logger.info(
        "After making the networkx generateds subplant_ids, "
        f"{subplant_ids.subplant_id.notnull().sum() / len(subplant_ids):.1%} of"
        " records have a subplant_id."
    )
    # update the subplant ids for each plant
    subplant_ids_updated = subplant_ids.groupby(
        by=["plant_id_eia"], group_keys=False
    ).apply(update_subplant_ids)
    # log differences between updated ids
    subplant_id_diff = subplant_ids_updated[
        subplant_ids_updated.subplant_id != subplant_ids_updated.subplant_id_updated
    ]
    logger.info(
        "Edited subplant_ids after update_subplant_ids: "
        f"{len(subplant_id_diff)/len(subplant_ids_updated):.1}%"
    )
    # overwrite the subplant ids and apply mannual update
    subplant_ids_updated = (
        subplant_ids_updated.assign(subplant_id=lambda x: x.subplant_id_updated)
        .reset_index(drop=True)
        .pipe(manually_update_subplant_id)
    )
    # check for duplicates in sudo-PKs. These are not the actual PKs because there are
    # some nulls in generator_id, so this won't be checked during the db construction
    if (
        epacamd_eia_dupe_mask := subplant_ids_updated.duplicated(
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
            f"{subplant_ids_updated[epacamd_eia_dupe_mask]}"
        )
    return subplant_ids_updated


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


######################
# Nexworkx subplant_id
######################


def _prep_for_networkx(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Make surrogate keys for combustors and generators.

    Args:
        crosswalk (pd.DataFrame): epacamd_eia crosswalk

    Returns:
        pd.DataFrame: copy of epacamd_eia crosswalk with new surrogate ID columns
            'combustor_id' and 'generator_id'
    """
    prepped = crosswalk.copy()
    # networkx can't handle composite keys, so make surrogates
    prepped["combustor_id"] = prepped.groupby(
        by=["plant_id_eia", "emissions_unit_id_epa"]
    ).ngroup()
    # node IDs can't overlap so add (max + 1)
    prepped["generator_id_unique"] = (
        prepped.groupby(by=["plant_id_eia", "generator_id"]).ngroup()
        + prepped["combustor_id"].max()
        + 1
    )
    return prepped


def _subplant_ids_from_prepped_crosswalk(prepped: pd.DataFrame) -> pd.DataFrame:
    """Use networkx graph analysis to create subplant IDs from crosswalk edge list.

    Args:
        prepped (pd.DataFrame): epacamd_eia crosswalked passed through
            _prep_for_networkx()

    Returns:
        pd.DataFrame: copy of epacamd_eia crosswalk plus new column 'global_subplant_id'
    """
    graph = nx.from_pandas_edgelist(
        prepped,
        source="combustor_id",
        target="generator_id_unique",
        edge_attr=True,
    )
    for i, node_set in enumerate(nx.connected_components(graph)):
        subgraph = graph.subgraph(node_set)
        assert nx.algorithms.bipartite.is_bipartite(
            subgraph
        ), f"non-bipartite: i={i}, node_set={node_set}"
        nx.set_edge_attributes(subgraph, name="global_subplant_id", values=i)
    return nx.to_pandas_edgelist(graph)


def _convert_global_id_to_composite_id(
    crosswalk_with_ids: pd.DataFrame,
) -> pd.DataFrame:
    """Convert global_subplant_id to a composite key (plant_id_eia, subplant_id).

    The composite key will be much more stable (though not fully stable!) in time.
    The global ID changes if ANY unit or generator changes, whereas the
    compound key only changes if units/generators change within that specific plant.

    A global ID could also tempt users into using it as a crutch, even though it isn't
    stable. A compound key should discourage that behavior.

    Args:
        crosswalk_with_ids (pd.DataFrame): crosswalk with global_subplant_id, as from
            _subplant_ids_from_prepped_crosswalk()

    Raises:
        ValueError: if crosswalk_with_ids has a MultiIndex

    Returns:
        pd.DataFrame: copy of crosswalk_with_ids with an added column: 'subplant_id'
    """
    if isinstance(crosswalk_with_ids.index, pd.MultiIndex):
        raise ValueError(
            f"Input crosswalk must have single level index. Given levels: {crosswalk_with_ids.index.names}"
        )

    reindexed = crosswalk_with_ids.reset_index()  # copy
    idx_name = crosswalk_with_ids.index.name
    if idx_name is None:
        # Indices with no name (None) are set to a pandas default name ('index'), which
        # could (though probably won't) change.
        idx_col = reindexed.columns.symmetric_difference(crosswalk_with_ids.columns)[
            0
        ]  # get index name
    else:
        idx_col = idx_name

    composite_key: pd.Series = reindexed.groupby("plant_id_eia", as_index=False).apply(
        lambda x: x.groupby("global_subplant_id").ngroup()
    )

    # Recombine. Could use index join but I chose to reindex, sort and assign.
    # Errors like mismatched length will raise exceptions, which is good.

    # drop the outer group, leave the reindexed row index
    composite_key.reset_index(level=0, drop=True, inplace=True)
    composite_key.sort_index(inplace=True)  # put back in same order as reindexed
    reindexed["subplant_id"] = composite_key
    # restore original index
    reindexed.set_index(idx_col, inplace=True)  # restore values
    reindexed.index.rename(idx_name, inplace=True)  # restore original name
    return reindexed


def make_subplant_ids(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Identify sub-plants in the EPA/EIA crosswalk graph.

    In graph analysis terminology, the crosswalk is a list of edges between nodes
    (combustors and generators) in a bipartite graph. The networkx python package
    provides functions to analyze this edge list and extract disjoint subgraphs (groups
    of combustors and generators that are connected to each other).  These are the
    distinct power plants. To avoid a name collision with plant_id, we term these
    collections 'subplants', and identify them with a subplant_id that is unique within
    each plant_id. Subplants are thus identified with the composite key (plant_id,
    subplant_id).

    Through this analysis, we found that 56% of plant_ids contain multiple distinct
    subplants, and 11% contain subplants with different technology types, such as a gas
    boiler and gas turbine (not in a combined cycle).

    Any row filtering should be done before this step if desired.

    Usage Example:

    epacems = pudl.output.epacems.epacems(states=['ID']) # small subset for quick test
    epacamd_eia = pudl_out.epacamd_eia()
    filtered_crosswalk = pudl.analysis.epacamd_eia.filter_crosswalk(epacamd_eia, epacems)
    crosswalk_with_subplant_ids = make_subplant_ids(filtered_crosswalk)

    Note that sub-plant ids should be used in conjunction with `plant_id_eia` vs.
    `plant_id_epa` because the former is more granular and integrated into CEMS during
    the transform process.

    Args:
        crosswalk (pd.DataFrame): The epacamd_eia crosswalk

    Returns:
        pd.DataFrame: An edge list connecting EPA units to EIA generators, with
            connected pieces issued a subplant_id
    """
    edge_list = _prep_for_networkx(crosswalk)
    edge_list = _subplant_ids_from_prepped_crosswalk(edge_list)
    edge_list = _convert_global_id_to_composite_id(edge_list)
    return edge_list


#############################
# Augmentation of subplant_id
#############################


def update_subplant_ids(subplant_crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Ensure a complete and accurate subplant_id mapping for all generators.

    This function is meant to be applied using a ``.groupby("plant_id_eia").apply()``
    function. This function will only properly work when applied to a single
    ``plant_id_eia`` at a time.

    High-level overview of method:
    ==============================

    #.  Use the ``subplant_id`` derived from :func:`make_subplant_ids` if available. In
        the case where a ``unit_id_pudl`` groups several subplants, we overwrite these
        multiple existing subplant_id with a single ``subplant_id``.
    #.  All of the new unique ids are renumbered in consecutive ascending order

    Args:
        subplant_crosswalk: a dataframe containing the output of :func:`make_subplant_ids`
    """
    # Step 1: Create corrected versions of subplant_id and unit_id_pudl
    # if multiple unit_id_pudl are connected by a single subplant_id,
    # unit_id_pudl_connected groups these unit_id_pudl together
    subplant_crosswalk = connect_ids(
        subplant_crosswalk, id_to_update="unit_id_pudl", connecting_id="subplant_id"
    )
    # if multiple subplant_id are connected by a single unit_id_pudl, group these
    # subplant_id together
    subplant_crosswalk = connect_ids(
        subplant_crosswalk, id_to_update="subplant_id", connecting_id="unit_id_pudl"
    )

    # Step 2: Update the subplant ID based on these now known unit/subplant overlaps
    subplant_crosswalk = subplant_crosswalk.assign(
        unit_id_pudl_filled=(
            lambda x: x.unit_id_pudl_connected.fillna(
                x.subplant_id_connected
                + x.groupby(
                    ["plant_id_eia"], dropna=False
                ).unit_id_pudl_connected.transform(max)
            )
        ),
        # create a new unique subplant_id based on the connected subplant ids and the
        # filled unit_id
        subplant_id_updated=(
            lambda x: x.groupby(
                ["plant_id_eia", "subplant_id_connected", "unit_id_pudl_filled"],
                dropna=False,
            ).ngroup()
        ),
    )

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
