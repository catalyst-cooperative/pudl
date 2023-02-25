"""Extract and transform glue tables between FERC Form 1 and EIA 860/923.

FERC1 and EIA report on many of the same plants and utilities, but have no embedded
connection. We have combed through the FERC and EIA plants and utilities to generate
id's which can connect these datasets. The resulting fields in the PUDL tables are
``plant_id_pudl`` and ``utility_id_pudl``, respectively. This was done by hand in a
spreadsheet which is in the ``package_data/glue`` directory. When mapping plants, we
considered a plant a co-located collection of electricity generation equipment. If a
coal plant was converted to a natural gas unit, our aim was to consider this the same
plant.  This module simply reads in the mapping spreadsheet and converts it to a
dictionary of dataframes.

Because these mappings were done by hand and for every one of FERC Form 1's thousands of
reported plants, we know there are probably some incorrect or incomplete mappings. If
you see a ``plant_id_pudl`` or ``utility_id_pudl`` mapping that you think is incorrect,
please open an issue on our Github!

Note that the PUDL IDs may change over time. They are not guaranteed to be stable. If
you need to find a particular plant or utility reliably, you should use its
``plant_id_eia``, ``utility_id_eia``, or ``utility_id_ferc1``.

Another note about these id's: these id's map our definition of plants, which is not the
most granular level of plant unit. The generators are typically the smaller, more
interesting unit. FERC does not typically report in units (although it sometimes does),
but it does often break up gas units from coal units. EIA reports on the generator and
boiler level. When trying to use these PUDL id's, consider the granularity that you
desire and the potential implications of using a co-located set of plant infrastructure
as an id.
"""

import importlib

import pandas as pd
import sqlalchemy as sa
from dagster import AssetIn, Definitions, JobDefinition, asset, define_asset_job

import pudl
from pudl.extract.ferc1 import raw_ferc1_assets, xbrl_metadata_json
from pudl.io_managers import ferc1_dbf_sqlite_io_manager, ferc1_xbrl_sqlite_io_manager
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.resources import dataset_settings
from pudl.transform.classes import StringNormalization, normalize_strings_multicol
from pudl.transform.ferc1 import (
    Ferc1AbstractTableTransformer,
    TableIdFerc1,
    ferc1_transform_asset_factory,
)
from pudl.transform.params.ferc1 import FERC1_STRING_NORM

logger = pudl.logging_helpers.get_logger(__name__)

PUDL_ID_MAP_XLSX = importlib.resources.open_binary(
    "pudl.package_data.glue", "pudl_id_mapping.xlsx"
)
"""Path to the PUDL ID mapping sheet with the plant map."""

UTIL_ID_PUDL_MAP_CSV = importlib.resources.open_text(
    "pudl.package_data.glue", "utility_id_pudl.csv"
)
"""Path to the PUDL utility ID mapping CSV."""

UTIL_ID_FERC_MAP_CSV = importlib.resources.open_text(
    "pudl.package_data.glue", "utility_id_ferc1.csv"
)
"""Path to the PUDL-assign FERC1 utility ID mapping CSV."""

MIN_PLANT_CAPACITY_MW: float = 5.0
MAX_LOST_PLANTS_EIA: int = 50
MAX_LOST_UTILS_EIA: int = 10


#####################################
# Stored Maps of plants and utilities
#####################################
def get_plant_map() -> pd.DataFrame:
    """Read in the manual FERC to EIA plant mapping data."""
    return pd.read_excel(
        PUDL_ID_MAP_XLSX,
        sheet_name="plants_output",
        na_values="",
        keep_default_na=False,
        converters={
            "plant_id_pudl": int,
            "plant_name_pudl": str,
            "utility_id_ferc1": int,
            "utility_name_ferc1": str,
            "plant_name_ferc1": str,
            "plant_id_eia": int,
            "plant_name_eia": str,
            "utility_name_eia": str,
            "utility_id_eia": int,
        },
    )


def get_utility_map_pudl() -> pd.DataFrame:
    """Read in the manual FERC to EIA utility mapping data."""
    return (
        pd.read_csv(UTIL_ID_PUDL_MAP_CSV.name)
        .convert_dtypes()
        .assign(
            utility_name_pudl=lambda x: x.utility_name_eia.fillna(x.utility_name_ferc1)
        )
    )


def get_utility_map_ferc1() -> pd.DataFrame:
    """Read in the manual XBRL to DBF FERC1 utility mapping data."""
    return pd.read_csv(UTIL_ID_FERC_MAP_CSV.name).convert_dtypes()


def get_mapped_plants_eia():
    """Get a list of all EIA plants that have been assigned PUDL Plant IDs.

    Read in the list of already mapped EIA plants from the FERC 1 / EIA plant
    and utility mapping spreadsheet kept in the package_data.

    Args:
        None

    Returns:
        pandas.DataFrame: A DataFrame listing the plant_id_eia and
        plant_name_eia values for every EIA plant which has already been
        assigned a PUDL Plant ID.
    """
    mapped_plants_eia = (
        get_plant_map()
        .loc[:, ["plant_id_eia", "plant_name_eia"]]
        .dropna(subset=["plant_id_eia"])
        .pipe(pudl.helpers.simplify_strings, columns=["plant_name_eia"])
        .astype({"plant_id_eia": int})
        .drop_duplicates("plant_id_eia")
        .sort_values("plant_id_eia")
    )
    return mapped_plants_eia


##########################
# Raw Plants and Utilities
##########################
def get_util_ids_ferc1_raw_xbrl(ferc1_engine_xbrl: sa.engine.Engine) -> pd.DataFrame:
    """Grab the utility ids (reported as `entity_id`) in the FERC1 XBRL database."""
    all_utils_ferc1_xbrl = (
        pd.read_sql(
            "SELECT entity_id, respondent_legal_name FROM identification_001_duration",
            ferc1_engine_xbrl,
        )
        .rename(
            columns={
                "entity_id": "utility_id_ferc1_xbrl",
                "respondent_legal_name": "utility_name_ferc1",
            }
        )
        .pipe(
            normalize_strings_multicol,
            {"utility_name_ferc1": StringNormalization(**FERC1_STRING_NORM)},
        )
        .drop_duplicates(subset=["utility_id_ferc1_xbrl"])
    )
    return all_utils_ferc1_xbrl


def get_util_ids_ferc1_raw_dbf(ferc1_engine_dbf: sa.engine.Engine) -> pd.DataFrame:
    """Grab the utility ids (reported as `respondent_id`) in the FERC1 DBF database."""
    all_utils_ferc1_dbf = (
        pd.read_sql_table("f1_respondent_id", ferc1_engine_dbf)
        .rename(
            columns={
                "respondent_id": "utility_id_ferc1_dbf",
                "respondent_name": "utility_name_ferc1",
            }
        )
        .pipe(
            normalize_strings_multicol,
            {"utility_name_ferc1": StringNormalization(**FERC1_STRING_NORM)},
        )
        .drop_duplicates(subset=["utility_id_ferc1_dbf"])
        .loc[:, ["utility_id_ferc1_dbf", "utility_name_ferc1"]]
    )
    return all_utils_ferc1_dbf


class GenericPlantFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Generic plant table transformer.

    Intended for use in compiling all plant names for manual ID mapping.
    """

    def __init__(self, table_id: TableIdFerc1, **kwargs):
        """Initialize generic table transformer with table_id."""
        self.table_id = table_id
        super().__init__(**kwargs)

    def transform(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Only apply the generic :meth:``transform_start``."""
        return self.transform_start(raw_dbf, raw_xbrl_instant, raw_xbrl_duration).pipe(
            self.transform_main
        )

    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic name normalization and dropping of invalid rows."""
        return (
            self.normalize_strings(df)
            .pipe(self.drop_invalid_rows)
            .assign(plant_table=self.table_id.value)
        )

    def drop_invalid_rows(self, df):
        """Add required valid columns before running standard drop_invalid_rows.

        This parent classes' method drops the whole df if all of the
        ``require_valid_columns`` don't exist in the df. For the glue tests only, we add
        in empty required columns because we know that the real ETL adds columns during
        the full transform step.
        """
        # ensure the required columns are actually in the df
        list_of_lists_of_required_valid_cols = [
            param.required_valid_cols
            for param in self.params.drop_invalid_rows
            if param.required_valid_cols
        ]
        required_valid_cols = pudl.helpers.dedupe_n_flatten_list_of_lists(
            list_of_lists_of_required_valid_cols
        )
        if required_valid_cols:
            missing_required_cols = set(required_valid_cols).difference(df.columns)
            if missing_required_cols:
                logger.info(
                    f"{self.table_id.value}: Found required columns for "
                    "`drop_invalid_rows`. Adding empty columns for: "
                    f"{missing_required_cols}"
                )
                df.loc[:, missing_required_cols] = pd.NA
        return super().drop_invalid_rows(df)


def get_plants_ferc1_raw_job() -> JobDefinition:
    """Pull all plants in the FERC Form 1 DBF and XBRL DB for given years.

    This job expects ferc1.sqlite and ferc_xbrl.sqlite databases to be populated.
    """
    plant_tables = [
        "plants_hydro_ferc1",
        "plants_small_ferc1",
        "plants_pumped_storage_ferc1",
        "plants_steam_ferc1",
        "fuel_ferc1",  # bc it has plants/is associated w/ the steam table
    ]

    @asset(ins={table_name: AssetIn() for table_name in plant_tables})
    def plants_ferc1_raw(**transformed_plant_tables):
        plant_dfs = transformed_plant_tables.values()
        all_plants = pd.concat(plant_dfs)
        # add the utility_name_ferc1
        util_map = get_utility_map_pudl()
        unique_utils_ferc1 = util_map.loc[
            util_map.utility_id_ferc1.notnull(),
            ["utility_id_ferc1", "utility_name_ferc1"],
        ].drop_duplicates(subset=["utility_id_ferc1"])
        all_plants = all_plants.merge(
            unique_utils_ferc1,
            on=["utility_id_ferc1"],
            how="left",
            validate="m:1",
        )
        # grab the most recent plant record
        all_plants = (
            all_plants.sort_values(["report_year"], ascending=False)
            .loc[
                :,
                [
                    "utility_id_ferc1",
                    "utility_name_ferc1",
                    "plant_name_ferc1",
                    "utility_id_ferc1_dbf",
                    "utility_id_ferc1_xbrl",
                    "capacity_mw",
                    "report_year",
                    "plant_table",
                ],
            ]
            .drop_duplicates(["utility_id_ferc1", "plant_name_ferc1"])
            .sort_values(["utility_id_ferc1", "plant_name_ferc1"])
        )
        return all_plants

    tfr_mapping = {
        table_name: GenericPlantFerc1TableTransformer for table_name in plant_tables
    }
    transform_assets = [
        ferc1_transform_asset_factory(
            table_name,
            tfr_mapping,
            io_manager_key=None,
            convert_dtypes=False,
            generic=True,
        )
        for table_name in plant_tables
    ]

    return Definitions(
        assets=transform_assets
        + raw_ferc1_assets
        + [plants_ferc1_raw]
        + [xbrl_metadata_json],
        resources={
            "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
            "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
            "dataset_settings": dataset_settings,
        },
        jobs=[define_asset_job(name="get_plants_ferc1_raw")],
    ).get_job_def("get_plants_ferc1_raw")


###############################
# Unmapped plants and utilities
###############################
def get_missing_ids(
    ids_left: pd.DataFrame,
    ids_right: pd.DataFrame,
    id_cols: list[str],
) -> pd.Index:
    """Identify IDs that are missing from the left df but show up in the right df.

    Args:
        ids_left: table which contains ``id_cols`` to be used as left table in join.
        ids_right: table which contains ``id_cols`` to be used as left table in join.
        id_cols: list of ID column(s)

    Return:
        index of unique values in ``id_cols`` that exist in ``ids_right`` but not
        ``ids_left``.
    """
    ids_left = ids_left.set_index(id_cols)
    ids_right = ids_right.set_index(id_cols)
    return ids_right.index.difference(ids_left.index)


def label_missing_ids_for_manual_mapping(
    missing_ids: pd.Index, label_df: pd.DataFrame
) -> pd.DataFrame:
    """Label unmapped IDs for manual mapping."""
    # the index name for single indexes are accessed differently than for multi-indexes
    return label_df.set_index(missing_ids.name or missing_ids.names).loc[missing_ids]


def label_plants_eia(pudl_out: pudl.output.pudltabl.PudlTabl):
    """Label plants with columns helpful in manual mapping."""
    plants = pudl_out.plants_eia860()
    # generator table for capacity
    plant_capacity = (
        pudl_out.gens_eia860()
        .groupby(["plant_id_eia", "report_date"], as_index=False)[["capacity_mw"]]
        .sum(min_count=1)
        .pipe(apply_pudl_dtypes, group="eia")
    )
    plants_w_capacity = (
        plants.merge(
            plant_capacity,
            on=["plant_id_eia", "report_date"],
            how="outer",
            validate="1:1",
        )
        .assign(link_to_ferc1=lambda x: x.capacity_mw >= MIN_PLANT_CAPACITY_MW)
        .sort_values(["report_date"], ascending=False)
        .loc[
            :,
            [
                "plant_id_eia",
                "plant_name_eia",
                "utility_id_eia",
                "utility_name_eia",
                "link_to_ferc1",
                "state",
                "capacity_mw",
            ],
        ]
        .drop_duplicates(subset=["plant_id_eia"])
    )
    return plants_w_capacity


def label_utilities_ferc1_dbf(
    utilities_ferc1_dbf: pd.DataFrame, util_ids_ferc1_raw_dbf: pd.DataFrame
) -> pd.DataFrame:
    """Get the DBF FERC1 utilities with their names."""
    return utilities_ferc1_dbf.merge(
        util_ids_ferc1_raw_dbf, how="outer", on="utility_id_ferc1_dbf"
    )


def label_utilities_ferc1_xbrl(
    utilities_ferc1_xbrl: pd.DataFrame, util_ids_ferc1_raw_xbrl: pd.DataFrame
) -> pd.DataFrame:
    """Get the XBRL FERC1 utilities with their names."""
    return utilities_ferc1_xbrl.merge(
        util_ids_ferc1_raw_xbrl, how="outer", on="utility_id_ferc1_xbrl"
    )


def get_utility_most_recent_capacity(pudl_engine) -> pd.DataFrame:
    """Calculate total generation capacity by utility in most recent reported year."""
    gen_caps = pd.read_sql(
        "SELECT utility_id_eia, capacity_mw, report_date FROM generators_eia860",
        con=pudl_engine,
        parse_dates=["report_date"],
    )
    gen_caps["utility_id_eia"] = gen_caps["utility_id_eia"].astype("Int64")

    most_recent_gens_idx = (
        gen_caps.groupby("utility_id_eia")["report_date"].transform(max)
        == gen_caps["report_date"]
    )
    most_recent_gens = gen_caps.loc[most_recent_gens_idx]
    utility_caps = most_recent_gens.groupby("utility_id_eia").sum()
    return utility_caps


def get_plants_ids_eia923(pudl_out: pudl.output.pudltabl.PudlTabl) -> pd.DataFrame:
    """Get a list of plant_id_eia's that show up in EIA 923 tables."""
    pudl_out_methods_eia923 = [
        method_name
        for method_name in dir(pudl_out)
        if callable(getattr(pudl_out, method_name))
        and "_eia923" in method_name
        and "gen_fuel_by_generator" not in method_name
    ]
    list_of_plant_ids = []
    for eia923_meth in pudl_out_methods_eia923:
        new_ids = getattr(pudl_out, eia923_meth)()[["plant_id_eia"]]
        list_of_plant_ids.append(new_ids)
    plant_ids_in_eia923 = pd.concat(list_of_plant_ids).drop_duplicates()
    return plant_ids_in_eia923


def get_util_ids_eia_unmapped(
    pudl_out, pudl_engine, utilities_eia_mapped
) -> pd.DataFrame:
    """Get a list of all the EIA Utilities in the PUDL DB without PUDL IDs.

    Identify any EIA Utility that appears in the data but does not have a
    utility_id_pudl associated with it in our ID mapping spreadsheet. Label some of
    those utilities for potential linkage to FERC 1 utilities, but only if they have
    plants which report data somewhere in the EIA-923 data tables. For those utilites
    that do have plants reporting in EIA-923, sum up the total capacity of all of their
    plants and include that in the output dataframe so that we can effectively
    prioritize mapping them.
    """
    utilities_eia_db = pudl_out.utils_eia860()[
        ["utility_id_eia", "utility_name_eia"]
    ].drop_duplicates(["utility_id_eia"])
    unmapped_utils_eia = get_missing_ids(
        utilities_eia_mapped, utilities_eia_db, id_cols=["utility_id_eia"]
    )

    # Get the most recent total capacity for the unmapped utils.
    unmapped_utils_eia = (
        get_utility_most_recent_capacity(pudl_engine)
        .loc[unmapped_utils_eia]
        .merge(
            utilities_eia_db.set_index("utility_id_eia"),
            left_index=True,
            right_index=True,
            how="left",
        )
    )

    plant_ids_in_eia923 = get_plants_ids_eia923(pudl_out=pudl_out)
    utils_with_plants = (
        pudl_out.gens_eia860()
        .loc[:, ["utility_id_eia", "plant_id_eia"]]
        .drop_duplicates()
        .dropna()
    )
    utils_with_data_in_eia923 = utils_with_plants.loc[
        utils_with_plants.plant_id_eia.isin(plant_ids_in_eia923), "utility_id_eia"
    ].to_frame()

    # Most unmapped utilities have no EIA 923 data and so don't need to be linked:
    unmapped_utils_eia["link_to_ferc1"] = False
    # Any utility ID that's both unmapped and has EIA 923 data should get linked:
    idx_to_link = unmapped_utils_eia.index.intersection(
        utils_with_data_in_eia923.utility_id_eia
    )
    unmapped_utils_eia.loc[idx_to_link, "link_to_ferc1"] = True

    unmapped_utils_eia = unmapped_utils_eia.sort_values(
        by="capacity_mw", ascending=False
    )

    return unmapped_utils_eia


#################
# Glue Tables ETL
#################
def glue(ferc1=False, eia=False):
    """Generates a dictionary of dataframes for glue tables between FERC1, EIA.

    That data is primarily stored in the plant_output and
    utility_output tabs of package_data/glue/pudl_id_mapping.xlsx in the
    repository. There are a total of seven relations described in this data:

        - utilities: Unique id and name for each utility for use across the
          PUDL DB.
        - plants: Unique id and name for each plant for use across the PUDL DB.
        - utilities_eia: EIA operator ids and names attached to a PUDL
          utility id.
        - plants_eia: EIA plant ids and names attached to a PUDL plant id.
        - utilities_ferc: FERC respondent ids & names attached to a PUDL
          utility id.
        - plants_ferc: A combination of FERC plant names and respondent ids,
          associated with a PUDL plant ID. This is necessary because FERC does
          not provide plant ids, so the unique plant identifier is a
          combination of the respondent id and plant name.
        - utility_plant_assn: An association table which describes which plants
          have relationships with what utilities. If a record exists in this
          table then combination of PUDL utility id & PUDL plant id does have
          an association of some kind. The nature of that association is
          somewhat fluid, and more scrutiny will likely be required for use in
          analysis.

    Presently, the 'glue' tables are a very basic piece of infrastructure for
    the PUDL DB, because they contain the primary key fields for utilities and
    plants in FERC1.

    Args:
        ferc1 (bool): Are we ingesting FERC Form 1 data?
        eia (bool): Are we ingesting EIA data?

    Returns:
        dict: a dictionary of glue table DataFrames
    """
    # ferc glue tables are structurally entity tables w/ foreign key
    # relationships to ferc datatables, so we need some of the eia/ferc 'glue'
    # even when only ferc is ingested into the database.

    # We need to standardize plant names -- same capitalization and no leading
    # or trailing white space... since this field is being used as a key in
    # many cases. This also needs to be done any time plant_name is pulled in
    # from other tables.
    plant_map = get_plant_map().pipe(
        pudl.helpers.simplify_strings, ["plant_name_ferc1"]
    )

    plants_pudl = (
        plant_map.loc[:, ["plant_id_pudl", "plant_name_pudl"]]
        .drop_duplicates("plant_id_pudl")
        .dropna(how="all")
    )
    plants_eia = (
        plant_map.loc[:, ["plant_id_eia", "plant_name_eia", "plant_id_pudl"]]
        .drop_duplicates("plant_id_eia")
        .dropna(subset=["plant_id_eia"])
    )
    plants_ferc1 = (
        plant_map.loc[:, ["plant_name_ferc1", "utility_id_ferc1", "plant_id_pudl"]]
        .drop_duplicates(["plant_name_ferc1", "utility_id_ferc1"])
        .dropna(subset=["utility_id_ferc1", "plant_name_ferc1"])
    )

    utility_map_pudl = get_utility_map_pudl()
    utilities_pudl = (
        utility_map_pudl.loc[:, ["utility_id_pudl", "utility_name_pudl"]]
        .drop_duplicates("utility_id_pudl")
        .dropna(how="all")
    )
    utilities_eia = (
        utility_map_pudl.loc[
            :, ["utility_id_eia", "utility_name_eia", "utility_id_pudl"]
        ]
        .drop_duplicates("utility_id_eia")
        .dropna(subset=["utility_id_eia"])
    )
    utilities_ferc1 = (
        utility_map_pudl.loc[
            :, ["utility_id_ferc1", "utility_name_ferc1", "utility_id_pudl"]
        ]
        .drop_duplicates("utility_id_ferc1")
        .dropna(subset=["utility_id_ferc1"])
    )

    utility_map_ferc1 = get_utility_map_ferc1()
    utilities_ferc1_dbf = (
        utility_map_ferc1.loc[:, ["utility_id_ferc1", "utility_id_ferc1_dbf"]]
        .drop_duplicates("utility_id_ferc1_dbf")
        .dropna(subset=["utility_id_ferc1_dbf"])
    )
    utilities_ferc1_xbrl = (
        utility_map_ferc1.loc[:, ["utility_id_ferc1", "utility_id_ferc1_xbrl"]]
        .drop_duplicates("utility_id_ferc1_xbrl")
        .dropna(subset=["utility_id_ferc1_xbrl"])
    )

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_utilities_ferc1 = plant_map.loc[
        :, ["plant_id_pudl", "utility_id_ferc1"]
    ].dropna(subset=["utility_id_ferc1"])
    plants_utilities_eia = plant_map.loc[:, ["plant_id_pudl", "utility_id_eia"]].dropna(
        subset=["utility_id_eia"]
    )

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    # Now we can concatenate the two dataframes, and get rid of all the columns
    # except for plant_id and utility_id (which determine the  utility to plant
    # association), and get rid of any duplicates or lingering NaN values...
    utility_plant_assn = pd.concat(
        [
            pd.merge(utilities_eia, plants_utilities_eia, on="utility_id_eia"),
            pd.merge(utilities_ferc1, plants_utilities_ferc1, on="utility_id_ferc1"),
        ],
        sort=True,
    )

    utility_plant_assn = (
        utility_plant_assn.loc[:, ["plant_id_pudl", "utility_id_pudl"]]
        .dropna()
        .drop_duplicates()
    )

    # At this point there should be at most one row in each of these data
    # frames with NaN values after we drop_duplicates in each. This is because
    # there will be some plants and utilities that only exist in FERC, or only
    # exist in EIA, and while they will have PUDL IDs, they may not have
    # FERC/EIA info (and it'll get pulled in as NaN)
    # Is this check still meaningful with all the EIA plants and utilities that
    # we're harvesting IDs for, with no names?
    for df, df_n in zip(
        [plants_eia, plants_ferc1, utilities_eia, utilities_ferc1],
        ["plants_eia", "plants_ferc1", "utilities_eia", "utilities_ferc1"],
    ):
        if df[pd.isnull(df).any(axis="columns")].shape[0] > 1:
            logger.warning(
                f"FERC to EIA glue breaking in {df_n}. There are too many null "
                "fields. Check the mapping spreadhseet."
            )
        df = df.dropna()

    # Before we start inserting records into the database, let's do some basic
    # sanity checks to ensure that it's (at least kind of) clean.
    # INSERT SANITY HERE

    # Any FERC respondent_id that appears in plants_ferc1 must also exist in
    # utilities_ferc1:
    # INSERT MORE SANITY HERE

    glue_dfs = {
        "plants_pudl": plants_pudl,
        "utilities_pudl": utilities_pudl,
        "plants_ferc1": plants_ferc1,
        "utilities_ferc1": utilities_ferc1,
        "utilities_ferc1_dbf": utilities_ferc1_dbf,
        "utilities_ferc1_xbrl": utilities_ferc1_xbrl,
        "plants_eia": plants_eia,
        "utilities_eia": utilities_eia,
        "utility_plant_assn": utility_plant_assn,
    }

    return glue_dfs
