"""Extract and transform glue tables between FERC Form 1 and EIA 860/923.

FERC1 and EIA report on many of the same plants and utilities, but have no embedded
connection. We have combed through the FERC and EIA plants and utilities to generate
id's which can connect these datasets. The resulting fields in the PUDL tables are
`plant_id_pudl` and `utility_id_pudl`, respectively. This was done by hand in a
spreadsheet which is in the `package_data/glue` directory. When mapping plants, we
considered a plant a co-located collection of electricity generation equipment. If a
coal plant was converted to a natural gas unit, our aim was to consider this the same
plant.  This module simply reads in the mapping spreadsheet and converts it to a
dictionary of dataframes.

Because these mappings were done by hand and for every one of FERC Form 1's thousands of
reported plants, we know there are probably some incorrect or incomplete mappings. If
you see a `plant_id_pudl` or `utility_id_pudl` mapping that you think is incorrect,
please open an issue on our Github!

Note that the PUDL IDs may change over time. They are not guaranteed to be stable. If
you need to find a particular plant or utility reliably, you should use its
plant_id_eia, utility_id_eia, or utility_id_ferc1.

Another note about these id's: these id's map our definition of plants, which is not the
most granular level of plant unit. The generators are typically the smaller, more
interesting unit. FERC does not typically report in units (although it sometimes does),
but it does often break up gas units from coal units. EIA reports on the generator and
boiler level. When trying to use these PUDL id's, consider the granularity that you
desire and the potential implications of using a co-located set of plant infrastructure
as an id.
"""

import importlib
from collections.abc import Iterable

import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.helpers import get_logger
from pudl.transform.ferc1 import Ferc1AbstractTableTransformer, Ferc1TableId

logger = get_logger(__name__)

PUDL_ID_MAP_XLSX = importlib.resources.open_binary(
    "pudl.package_data.glue", "pudl_id_mapping.xlsx"
)
UTIL_ID_PUDL_MAP_CSV = importlib.resources.open_text(
    "pudl.package_data.glue", "utility_id_pudl.csv"
)

UTIL_ID_FERC_MAP_CSV = importlib.resources.open_text(
    "pudl.package_data.glue", "utility_id_ferc1.csv"
)

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


##########################
# Raw Plants and Utilities
##########################


def get_util_ids_ferc1_raw_xbrl(ferc1_xbrl_engine: sa.engine.Engine) -> pd.DataFrame:
    """Grab the utlity ids (reported as `entity_id`) in the FERC1 XBRL database."""
    all_utils_ferc1_xbrl = (
        pd.read_sql(
            "SELECT entity_id, respondent_legal_name FROM identification_001_duration",
            ferc1_xbrl_engine,
        )
        .rename(
            columns={
                "entity_id": "utility_id_ferc1_xbrl",
                "respondent_legal_name": "utility_name_ferc1",
            }
        )
        .drop_duplicates(subset=["utility_id_ferc1_xbrl"])
    )
    return all_utils_ferc1_xbrl


def get_utils_ferc1_raw_dbf(ferc1_engine_dbf: sa.engine.Engine) -> pd.DataFrame:
    """Grab the utlity ids (reported as `respondent_id`) in the FERC1 DBF database."""
    all_utils_ferc1_dbf = (
        pd.read_sql_table("f1_respondent_id", ferc1_engine_dbf)
        .rename(
            columns={
                "respondent_id": "utility_id_ferc1_dbf",
                "respondent_name": "utility_name_ferc1",
            }
        )
        .pipe(pudl.helpers.simplify_strings, ["utility_name_ferc1"])
        .drop_duplicates(subset=["utility_id_ferc1_dbf"])
    )
    return all_utils_ferc1_dbf


class GenericPlantFerc1TableTransformer(Ferc1AbstractTableTransformer):
    """Generic plant table transformer.

    Intended for use in compiling all plant names for mannual ID mapping.
    """

    def __init__(self, table_id: Ferc1TableId):
        """Initialize generic table transformer with table_id."""
        self.table_id = table_id
        super().__init__()

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


def get_raw_plants_ferc1(
    pudl_settings: dict[str, str], years: Iterable[int]
) -> pd.DataFrame:
    """Pull all plants in the FERC Form 1 DBF and XBRL DB for given years.

    Args:
        pudl_settings: Dictionary containing various paths and database URLs used by
            PUDL.
        years: Years for which plants should be compiled.

    Returns:
        A dataframe containing columns ``utility_id_ferc1``, ``utility_name_ferc1``,
        ``plant_name``, ``capacity_mw``, and ``plant_table``. Each row is a unique
        combination of ``utility_id_ferc1`` and ``plant_name``.
    """
    # Validate the input years:
    _ = pudl.settings.Ferc1Settings(years=list(years))

    plant_tables = [
        # "plants_hydro_ferc1",
        # "plants_pumped_storage_ferc1",
        # "plants_small_ferc1",
        "plants_steam_ferc1",
        "fuel_ferc1",
    ]
    ferc1_settings = pudl.settings.Ferc1Settings(tables=plant_tables)
    # Extract FERC form 1
    ferc1_dbf_raw_dfs = pudl.extract.ferc1.extract_dbf(
        ferc1_settings=ferc1_settings, pudl_settings=pudl_settings
    )
    # Extract FERC form 1 XBRL data
    ferc1_xbrl_raw_dfs = pudl.extract.ferc1.extract_xbrl(
        ferc1_settings=ferc1_settings, pudl_settings=pudl_settings
    )
    plant_dfs: list[pd.DataFrame] = []
    for table in plant_tables:
        plant_df = GenericPlantFerc1TableTransformer(
            table_id=Ferc1TableId(table)
        ).transform(
            raw_dbf=ferc1_dbf_raw_dfs[table],
            raw_xbrl_instant=ferc1_xbrl_raw_dfs[table].get("instant", pd.DataFrame()),
            raw_xbrl_duration=ferc1_xbrl_raw_dfs[table].get("duration", pd.DataFrame()),
        )
        plant_dfs.append(plant_df)

    all_plants = (
        pd.concat(plant_dfs)
        .drop_duplicates(["utility_id_ferc1", "plant_name_ferc1"])
        .sort_values(["utility_id_ferc1", "plant_name_ferc1"])
    )
    return all_plants


###############################
# Unmapped plants and utilities
###############################


def get_missing_ids(
    ids_left: pd.DataFrame,
    ids_right: pd.DataFrame,
    id_cols: list[str],
):
    """Identify IDs that are missing from the left df but show up in the right df."""
    id_test = pd.merge(ids_left, ids_right, on=id_cols, indicator=True, how="outer")
    missing = id_test[id_test._merge == "right_only"]
    return missing


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
    if not ferc1 and not eia:
        return

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

    # if we're not ingesting eia, exclude eia only tables
    if not eia:
        glue_dfs = {name: df for (name, df) in glue_dfs.items() if "_eia" not in name}
    # if we're not ingesting ferc, exclude ferc1 only tables
    if not ferc1:
        glue_dfs = {name: df for (name, df) in glue_dfs.items() if "_ferc1" not in name}

    return glue_dfs
