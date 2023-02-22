"""Code for transforming EIA data that pertains to more than one EIA Form.

This module helps normalize EIA datasets and infers additonal connections
between EIA entities (i.e. utilities, plants, units, generators...). This
includes:

- compiling a master list of plant, utility, boiler, and generator IDs that
  appear in any of the EIA 860 or 923 tables.
- inferring more complete boiler-generator associations.
- differentiating between static and time varying attributes associated with
  the EIA entities, storing the static fields with the entity table, and the
  variable fields in an annual table.

The boiler generator association inferrence (bga) takes the associations
provided by the EIA 860, and expands on it using several methods which can be
found in :func:`pudl.transform.eia._boiler_generator_assn`.
"""

import importlib.resources
from collections import namedtuple

import networkx as nx
import numpy as np
import pandas as pd
import timezonefinder

import pudl
from pudl.metadata.classes import DataSource
from pudl.metadata.enums import APPROXIMATE_TIMEZONES
from pudl.metadata.fields import apply_pudl_dtypes, get_pudl_dtypes
from pudl.metadata.resources import ENTITIES
from pudl.settings import EiaSettings

logger = pudl.logging_helpers.get_logger(__name__)

TZ_FINDER = timezonefinder.TimezoneFinder()
"""A global TimezoneFinder to cache geographies in memory for faster access."""


def find_timezone(*, lng=None, lat=None, state=None, strict=True):
    """Find the timezone associated with the a specified input location.

    Note that this function requires named arguments. The names are lng, lat,
    and state.  lng and lat must be provided, but they may be NA. state isn't
    required, and isn't used unless lng/lat are NA or timezonefinder can't find
    a corresponding timezone.

    Timezones based on states are imprecise, so it's far better to use lng/lat
    if possible. If `strict` is True, state will not be used.
    More on state-to-timezone conversion here:
    https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory

    Args:
        lng (int or float in [-180,180]): Longitude, in decimal degrees
        lat (int or float in [-90, 90]): Latitude, in decimal degrees
        state (str): Abbreviation for US state or Canadian province
        strict (bool): Raise an error if no timezone is found?

    Returns:
        str: The timezone (as an IANA string) for that location.

    Todo:
        Update docstring.
    """
    try:
        tz = TZ_FINDER.timezone_at(lng=lng, lat=lat)
        if tz is None:  # Try harder
            # Could change the search radius as well
            tz = TZ_FINDER.closest_timezone_at(lng=lng, lat=lat)
    # For some reason w/ Python 3.6 we get a ValueError here, but with
    # Python 3.7 we get an OverflowError...
    except (OverflowError, ValueError):
        # If we're being strict, only use lng/lat, not state
        if strict:
            raise ValueError(
                f"Can't find timezone for: lng={lng}, lat={lat}, state={state}"
            )
        tz = APPROXIMATE_TIMEZONES.get(state, None)
    return tz


def occurrence_consistency(
    entity_idx: list[str],
    compiled_df: pd.DataFrame,
    col: str,
    cols_to_consit: list[str],
    strictness: float = 0.7,
) -> pd.DataFrame:
    """Find the occurence of entities & the consistency of records.

    We need to determine how consistent a reported value is in the records
    across all of the years or tables that the value is being reported, so we
    want to compile two key numbers: the number of occurances of the entity and
    the number of occurances of each reported record for each entity. With that
    information we can determine if the reported records are strict enough.

    Args:
        entity_idx: a list of the id(s) for the entity. Ex: for a plant
            entity, the entity_idx is ['plant_id_eia']. For a generator entity,
            the entity_idx is ['plant_id_eia', 'generator_id'].
        compiled_df: a dataframe with every instance of the
            column we are trying to harvest.
        col: the column name of the column we are trying to harvest.
        cols_to_consit: a list of the columns to determine consistency.
            This either the [entity_id] or the [entity_id, 'report_date'],
            depending on whether the entity is static or annual.
        strictness: How consistent do you want the column records to
            be? The default setting is .7 (so 70% of the records need to be
            consistent in order to accept harvesting the record).

    Returns:
        pandas.DataFrame: this dataframe will be a transformed version of
        compiled_df with NaNs removed and with new columns with information
        about the consistency of the reported values.
    """
    # select only the colums you want and drop the NaNs
    # we want to drop the NaNs because
    col_df = compiled_df[entity_idx + ["report_date", col]].copy()
    if get_pudl_dtypes(group="eia")[col] == "string":
        nan_str_mask = (col_df[col] == "nan").fillna(False)
        col_df.loc[nan_str_mask, col] = pd.NA
    col_df = col_df.dropna()

    if len(col_df) == 0:
        col_df[f"{col}_is_consistent"] = pd.NA
        col_df[f"{col}_consistent_rate"] = pd.NA
        col_df["entity_occurences"] = pd.NA
        return col_df
    # determine how many times each entity occurs in col_df
    occur = (
        col_df.assign(entity_occurences=1)
        .groupby(by=cols_to_consit, observed=True)[["entity_occurences"]]
        .count()
        .reset_index()
    )

    # add the occurances into the main dataframe
    col_df = col_df.merge(occur, on=cols_to_consit)

    # determine how many instances of each of the records in col exist
    consist_df = (
        col_df.assign(record_occurences=1)
        .groupby(by=cols_to_consit + [col], observed=True)[["record_occurences"]]
        .count()
        .reset_index()
    )
    # now in col_df we have # of times an entity occurred accross the tables
    # and we are going to merge in the # of times each value occured for each
    # entity record. When we merge the consistency in with the occurances, we
    # can determine if the records are more than 70% consistent across the
    # occurances of the entities.
    col_df = col_df.merge(consist_df, how="outer")
    # change all of the fully consistent records to True
    col_df[f"{col}_consistent_rate"] = (
        col_df["record_occurences"] / col_df["entity_occurences"]
    )
    col_df[f"{col}_is_consistent"] = col_df[f"{col}_consistent_rate"] > strictness
    col_df = col_df.sort_values(f"{col}_consistent_rate")
    return col_df


def _lat_long(
    dirty_df, clean_df, entity_id_df, entity_idx, col, cols_to_consit, round_to=2
):
    """Harvests more complete lat/long in special cases.

    For all of the entities were there is not a consistent enough reported
    record for latitude and longitude, this function reduces the precision of
    the reported lat/long by rounding down the reported records in order to get
    more complete set of consistent records.

    Args:
        dirty_df (pandas.DataFrame): a dataframe with entity records that have
            inconsistently reported lat/long.
        clean_df (pandas.DataFrame): a dataframe with entity records that have
            consistently reported lat/long.
        entity_id_df (pandas.DataFrame): a dataframe with a complete set of
            possible entity ids
        entity_idx (list): a list of the id(s) for the entity. Ex: for a plant
            entity, the entity_idx is ['plant_id_eia']. For a generator entity,
            the entity_idx is ['plant_id_eia', 'generator_id'].
        col (string): the column name of the column we are trying to harvest.
        cols_to_consit (list): a list of the columns to determine consistency.
            This either the [entity_id] or the [entity_id, 'report_date'],
            depending on whether the entity is static or annual.
        round_to (integer): This is the number of decimals places we want to
            preserve while rounding down.

    Returns:
        pandas.DataFrame: a dataframe with all of the entity ids. some will
        have harvested records from the clean_df. some will have harvested
        records that were found after rounding. some will have NaNs if no
        consistently reported records were found.
    """
    # grab the dirty plant records, round and get a new consistency
    ll_df = dirty_df.round(decimals={col: round_to})
    logger.debug(f"Dirty {col} records: {len(ll_df)}")
    ll_df["table"] = "special_case"
    ll_df = occurrence_consistency(entity_idx, ll_df, col, cols_to_consit)
    # grab the clean plants
    ll_clean_df = clean_df.dropna()
    # find the new clean plant records by selecting the True consistent records
    ll_df = ll_df[ll_df[f"{col}_is_consistent"]].drop_duplicates(subset=entity_idx)
    logger.debug(f"Clean {col} records: {len(ll_df)}")
    # add the newly cleaned records
    ll_clean_df = pd.concat([ll_clean_df, ll_df])
    # merge onto the plants df w/ all plant ids
    ll_clean_df = entity_id_df.merge(ll_clean_df, how="outer")
    return ll_clean_df


def _add_timezone(plants_entity: pd.DataFrame) -> pd.DataFrame:
    """Add plant IANA timezone based on lat/lon or state if lat/lon is unavailable.

    Args:
        plants_entity: Plant entity table, including columns named "latitude",
            "longitude", and optionally "state"

    Returns:
        A DataFrame containing the same table, with a "timezone" column added.
        Timezone may be missing if lat / lon is missing or invalid.
    """
    plants_entity["timezone"] = plants_entity.apply(
        lambda row: find_timezone(
            lng=row["longitude"], lat=row["latitude"], state=row["state"], strict=False
        ),
        axis=1,
    )
    return plants_entity


def _add_additional_epacems_plants(plants_entity):
    """Adds the info for plants that have IDs in the CEMS data but not EIA data.

    The columns loaded are plant_id_eia, plant_name, state, latitude, and
    longitude. Note that a side effect will be resetting the index on
    plants_entity, if onecexists. If that's a problem, modify the code below.

    Note that some of these plants disappear from the CEMS before the
    earliest EIA data PUDL processes, so if PUDL eventually ingests older
    data, these may be redundant.

    The set of additional plants is every plant that appears in the hourly CEMS
    data (1995-2017) that never appears in the EIA 923 or 860 data (2009-2017
    for EIA 923, 2011-2017 for EIA 860).

    Args:
        plants_entity (pandas.DataFrame) The plant entity table that will be
            appended to

    Returns:
        pandas.DataFrame: The same plants_entity table, with the addition of
        some missing EPA CEMS plants.
    """
    # Add the plant IDs that are missing and update the values for the others
    # The data we're reading is a CSV in pudl/metadata/
    # SQL would call this whole process an upsert
    # See also: https://github.com/pandas-dev/pandas/issues/22812
    cems_df = pd.read_csv(
        importlib.resources.open_text(
            "pudl.package_data.epacems", "additional_epacems_plants.csv"
        ),
        index_col=["plant_id_eia"],
        usecols=["plant_id_eia", "plant_name_eia", "state", "latitude", "longitude"],
    )
    plants_entity = plants_entity.set_index("plant_id_eia")
    cems_unmatched = cems_df.loc[~cems_df.index.isin(plants_entity.index)]
    # update will replace columns and index values that add rows or affect
    # non-matching columns. It also requires an index, so we set and reset the
    # index as necessary. Also, it only works in-place, so we can't chain.
    plants_entity.update(cems_df, overwrite=True)
    return pd.concat([plants_entity, cems_unmatched]).reset_index()


def _compile_all_entity_records(entity, eia_transformed_dfs):
    """Compile all of the entity records from each table they appear in.

    Comb through each of the dataframes in the eia_transformed_dfs dictionary to pull
    out every instance of the entity id.
    """
    # we know these columns must be in the dfs
    id_cols = ENTITIES[entity]["id_cols"]
    static_cols = ENTITIES[entity]["static_cols"]
    annual_cols = ENTITIES[entity]["annual_cols"]
    base_cols = id_cols + ["report_date"]
    keep_cols = ENTITIES[entity].get("keep_cols", [])

    # empty list for dfs to be added to for each table below
    dfs = []
    # for each df in the dict of transformed dfs
    for table_name, transformed_df in eia_transformed_dfs.items():
        # inside of main() we are going to be adding items into
        # eia_transformed_dfs with the name 'annual'. We don't want to harvest
        # from our newly harvested tables.
        if "annual" not in table_name:
            # if the df contains the desired columns the grab those columns
            if set(base_cols).issubset(transformed_df.columns):
                logger.debug(f"        {table_name}...")
                # create a copy of the df to muck with
                df = transformed_df.copy()
                # we know these columns must be in the dfs
                cols = []
                # check whether the columns are in the specific table
                for column in static_cols + annual_cols:
                    if column in df.columns:
                        cols.append(column)
                df = df[(base_cols + cols)]
                df = df.dropna(subset=id_cols)
                # add a column with the table name so we know its origin
                df["table"] = table_name
                dfs.append(df)

                # remove the static columns, except the explicitly non-dropped cols
                transformed_df = transformed_df.drop(
                    columns=[c for c in cols if c not in keep_cols]
                )
                eia_transformed_dfs[table_name] = transformed_df

    # add those records to the compliation
    compiled_df = pd.concat(dfs, axis=0, ignore_index=True, sort=True)
    # strip the month and day from the date so we can have annual records
    compiled_df["report_date"] = compiled_df["report_date"].dt.year
    # convert the year back into a date_time object
    year = compiled_df["report_date"]
    compiled_df["report_date"] = pd.to_datetime({"year": year, "month": 1, "day": 1})

    logger.debug("    Casting harvested IDs to correct data types")
    # most columns become objects (ack!), so assign types
    compiled_df = apply_pudl_dtypes(compiled_df, group="eia")
    # encode the compiled options! (except for the boilers bc there is no annual table)
    if entity != "boilers":
        compiled_df = (
            pudl.metadata.classes.Package.from_resource_ids()
            .get_resource(f"{entity}_eia860")
            .encode(compiled_df)
        )
    return compiled_df


def _manage_strictness(col, eia860m):
    """Manage the strictness level for each column.

    Args:
        col (str): name of column
        eia860m (boolean): if True, the etl run is attempting to include
            year-to-date updated from EIA 860M.
    """
    strictness_default = 0.7
    # the longitude column is very different in the ytd 860M data (it appears
    # to have an additional decimal point) bc it shows up in the generator
    # table but it is a plant level data point, it mucks up the consistency
    strictness_cols = {
        "plant_name_eia": 0,
        "utility_name_eia": 0,
        "longitude": 0 if eia860m else 0.7,
    }
    return strictness_cols.get(col, strictness_default)


def harvesting(  # noqa: C901
    entity: str,
    eia_transformed_dfs: dict[str, pd.DataFrame],
    entities_dfs: dict[str, pd.DataFrame],
    eia860m: bool = False,
    debug: bool = False,
) -> tuple:
    """Compile consistent records for various entities.

    For each entity(plants, generators, boilers, utilties), this function
    finds all the harvestable columns from any table that they show up
    in. It then determines how consistent the records are and keeps the values
    that are mostly consistent. It compiles those consistent records into
    one normalized table.

    There are a few things to note here. First being that we are not expecting
    the outcome here to be perfect! We choose to pull the most consistent
    record as reported across all the EIA tables and years, but we also
    required a "strictness" level of 70% (this is currently a hard coded
    argument for :func:`occurrence_consistency`). That means at least 70% of the
    records must be the same for us to use that value. So if values for an
    entity haven't been reported 70% consistently, then it will show up as a
    null value. We built in the ability to add special cases for columns where
    we want to apply a different method to, but the only ones we added was for
    latitude and longitude because they are by far the dirtiest.

    We have determined which columns should be considered "static" or "annual".
    These can be found in constants in the `entities` dictionary. Static means
    That is should not change over time. Annual means there is annual
    variablity. This distinction was made in part by testing the consistency
    and in part by an understanding of how the entities and columns relate in
    the real world.

    Args:
        entity: plants, generators, boilers, utilties
        eia_transformed_dfs: A dictionary of tbl names (keys) and
            transformed dfs (values)
        entities_dfs: A dictionary of entity table names (keys) and
            entity dfs (values)
        eia860m: if True, the etl run is attempting to include
            year-to-date updated from EIA 860M.
        debug: If True, this function will also return an additional
            dictionary of dataframes that includes the pre-deduplicated
            compiled records with the number of occurances of the entity and
            the record to see consistency of reported values.

    Returns:
        tuple: A tuple containing:
            eia_transformed_dfs (dict): dictionary of tbl names (keys) and
            transformed dfs (values)
            entity_dfs (dict): dictionary of entity table names (keys) and
            entity dfs (values)

    Raises:
        AssertionError: If the consistency of any record value is <90%.

    Todo:
        * Return to role of debug.
        * Determine what to do with null records
        * Determine how to treat mostly static records
    """
    # we know these columns must be in the dfs
    id_cols = ENTITIES[entity]["id_cols"]
    static_cols = ENTITIES[entity]["static_cols"]
    annual_cols = ENTITIES[entity]["annual_cols"]

    logger.debug("    compiling plants for entity tables from:")

    compiled_df = _compile_all_entity_records(entity, eia_transformed_dfs)

    # compile annual ids
    annual_id_df = compiled_df[["report_date"] + id_cols].copy().drop_duplicates()
    annual_id_df.sort_values(["report_date"] + id_cols, inplace=True, ascending=False)

    # create the annual and entity dfs
    entity_id_df = annual_id_df.drop(["report_date"], axis=1).drop_duplicates(
        subset=id_cols
    )

    entity_df = entity_id_df.copy()
    annual_df = annual_id_df.copy()
    special_case_cols = {"latitude": [_lat_long, 1], "longitude": [_lat_long, 1]}
    consistency = pd.DataFrame(
        columns=["column", "consistent_ratio", "wrongos", "total"]
    )
    col_dfs = {}
    # determine how many times each of the columns occur
    for col in static_cols + annual_cols:
        if col in annual_cols:
            cols_to_consit = id_cols + ["report_date"]
        if col in static_cols:
            cols_to_consit = id_cols

        strictness = _manage_strictness(col, eia860m)
        col_df = occurrence_consistency(
            id_cols, compiled_df, col, cols_to_consit, strictness=strictness
        )

        # pull the correct values out of the df and merge w/ the plant ids
        col_correct_df = col_df[col_df[f"{col}_is_consistent"]].drop_duplicates(
            subset=(cols_to_consit + [f"{col}_is_consistent"])
        )

        # we need this to be an empty df w/ columns bc we are going to use it
        if col_correct_df.empty:
            col_correct_df = pd.DataFrame(columns=col_df.columns)

        if col in static_cols:
            clean_df = entity_id_df.merge(col_correct_df, on=id_cols, how="left")
            clean_df = clean_df[id_cols + [col]]
            entity_df = entity_df.merge(clean_df, on=id_cols)

        if col in annual_cols:
            clean_df = annual_id_df.merge(
                col_correct_df, on=(id_cols + ["report_date"]), how="left"
            )
            clean_df = clean_df[id_cols + ["report_date", col]]
            annual_df = annual_df.merge(clean_df, on=(id_cols + ["report_date"]))

        # get the still dirty records by using the cleaned ids w/null values
        # we need the plants that have no 'correct' value so
        # we can't just use the col_df records when the consistency is not True
        dirty_df = col_df.merge(clean_df[clean_df[col].isnull()][id_cols])

        if col in special_case_cols.keys():
            clean_df = special_case_cols[col][0](
                dirty_df,
                clean_df,
                entity_id_df,
                id_cols,
                col,
                cols_to_consit,
                special_case_cols[col][1],
            )

        if debug:
            col_dfs[col] = col_df
        # this next section is used to print and test whether the harvested
        # records are consistent enough
        total = len(col_df.drop_duplicates(subset=cols_to_consit))
        # if the total is 0, the ratio will error, so assign null values.
        if total == 0:
            ratio = np.NaN
            wrongos = np.NaN
            logger.debug(f"       Zero records found for {col}")
        if total > 0:
            ratio = (
                len(
                    col_df[(col_df[f"{col}_is_consistent"])].drop_duplicates(
                        subset=cols_to_consit
                    )
                )
                / total
            )
            wrongos = (1 - ratio) * total
            logger.debug(
                f"       Ratio: {ratio:.3}  "
                f"Wrongos: {wrongos:.5}  "
                f"Total: {total}   {col}"
            )
            if ratio < 0.9:
                if debug:
                    logger.error(f"{col} has low consistency: {ratio:.3}.")
                else:
                    raise AssertionError(
                        f"Harvesting of {col} is too inconsistent at {ratio:.3}."
                    )
        # add to a small df to be used in order to print out the ratio of
        # consistent records
        consistency = pd.concat(
            [
                consistency,
                pd.DataFrame(
                    {
                        "column": [col],
                        "consistent_ratio": [ratio],
                        "wrongos": [wrongos],
                        "total": [total],
                    }
                ),
            ],
            ignore_index=True,
        )
    mcs = consistency["consistent_ratio"].mean()
    logger.info(f"Average consistency of static {entity} values is {mcs:.2%}")

    if entity == "plants":
        entity_df = _add_additional_epacems_plants(entity_df)
        entity_df = _add_timezone(entity_df)

    eia_transformed_dfs[f"{entity}_annual_eia"] = annual_df
    entities_dfs[f"{entity}_entity_eia"] = entity_df
    if debug:
        return entities_dfs, eia_transformed_dfs, col_dfs
    return (entities_dfs, eia_transformed_dfs)


def _boiler_generator_assn(  # noqa: C901
    eia_transformed_dfs: dict[str, pd.DataFrame],
    eia923_years: list[int] | None = None,
    eia860_years: list[int] | None = None,
    debug: bool = False,
) -> dict[str, pd.DataFrame]:
    """Creates a set of more complete boiler generator associations.

    Creates a unique unit_id_pudl for each collection of boilers and generators
    within a plant that have ever been associated with each other, based on
    the boiler generator associations reported in EIA860. Unfortunately, this
    information is not complete for years before 2014, as the gas turbine
    portion of combined cycle power plants in those earlier years were not
    reporting their fuel consumption, or existence as part of the plants.

    For years 2014 and on, EIA860 contains a unit_id_eia value, allowing the
    combined cycle plant compoents to be associated with each other. For many
    plants not listed in the reported boiler generator associations, it is
    nonetheless possible to associate boilers and generators on a one-to-one
    basis, as they use identical strings to describe the units.

    In the end, between the reported BGA table, the string matching, and the
    unit_id_eia values, it's possible to create a nearly complete mapping of
    the generation units, at least for 2014 and later.

    Args:
        eia_transformed_dfs: a dictionary of post-transform dataframes
            representing the EIA database tables.
        eia923_years: a list of the years of EIA 923 data that
            should be used to infer the boiler-generator associations. By
            default it is all the working years of data.
        eia860_years: a list of the years of EIA 860 data that
            should be used to infer the boiler-generator associations. By
            default it is all the working years of data.
        debug: If True, include columns in the returned dataframe
            indicating by what method the individual boiler generator
            associations were inferred.

    Returns:
        eia_transformed_dfs (dict): Returns the same dictionary of dataframes
        that was passed in, and adds a new dataframe to it representing
        the boiler-generator associations as records containing
        plant_id_eia, generator_id, boiler_id, and unit_id_pudl

    Raises:
        AssertionError: If the boiler - generator association graphs are not
            bi-partite, meaning generators only connect to boilers, and boilers
            only connect to generators.
        AssertionError: If all the boilers do not end up with the same unit_id
            each year.
        AssertionError: If all the generators do not end up with the same
            unit_id each year.
    """
    if eia923_years is None:
        eia923_years = DataSource.from_id("eia923").working_partitions["years"]
    if eia860_years is None:
        eia860_years = DataSource.from_id("eia860").working_partitions["years"]

    # if eia860_years or eia923_years are still empty, we can't compile the BGA.
    # Return the unaltered input dictionary of dataframes instead.
    if not (eia860_years and eia923_years):
        return eia_transformed_dfs
    # compile and scrub all the parts
    logger.info("Inferring complete EIA boiler-generator associations.")
    bga_eia860 = (
        eia_transformed_dfs["boiler_generator_assn_eia860"]
        .copy()
        .pipe(_restrict_years, eia923_years, eia860_years)
        .astype(
            {
                "generator_id": pd.StringDtype(),
                "boiler_id": pd.StringDtype(),
                "plant_id_eia": int,
            }
        )
    )
    # grab the generation_eia923 table, group annually, generate a new tag
    gen_eia923 = eia_transformed_dfs["generation_eia923"].copy()
    gen_eia923 = gen_eia923.set_index(pd.DatetimeIndex(gen_eia923.report_date))
    gen_eia923 = (
        _restrict_years(gen_eia923, eia923_years, eia860_years)
        .astype(
            {
                "generator_id": pd.StringDtype(),
                "plant_id_eia": int,
            }
        )
        .groupby([pd.Grouper(freq="AS"), "plant_id_eia", "generator_id"])
        .net_generation_mwh.sum()
        .reset_index()
        .assign(missing_from_923=False)
    )

    # compile all of the generators
    gens_eia860 = (
        eia_transformed_dfs["generators_eia860"]
        .copy()
        .pipe(_restrict_years, eia923_years, eia860_years)
        .astype(
            {
                "generator_id": pd.StringDtype(),
                "plant_id_eia": int,
            }
        )
    )
    gens = pd.merge(
        gen_eia923,
        gens_eia860,
        on=["plant_id_eia", "report_date", "generator_id"],
        how="outer",
    )

    gens = (
        gens[
            [
                "plant_id_eia",
                "report_date",
                "generator_id",
                "unit_id_eia",
                "net_generation_mwh",
                "missing_from_923",
            ]
        ]
        .drop_duplicates()
        .astype(
            {
                "generator_id": pd.StringDtype(),
                "plant_id_eia": int,
            }
        )
    )

    # create the beginning of a bga compilation w/ the generators as the
    # background
    bga_compiled_1 = pd.merge(
        gens,
        bga_eia860,
        on=["plant_id_eia", "generator_id", "report_date"],
        how="outer",
    )

    # Create a set of bga's that are linked, directly from bga8
    bga_assn = bga_compiled_1[bga_compiled_1["boiler_id"].notnull()].copy()
    bga_assn.loc[:, "bga_source"] = "eia860_org"

    # Create a set of bga's that were not linked directly through bga8
    bga_unassn = bga_compiled_1[bga_compiled_1["boiler_id"].isnull()].copy()
    bga_unassn = bga_unassn.drop(["boiler_id"], axis=1)

    # Side note: there are only 6 generators that appear in bga8 that don't
    # apear in gens9 or gens8 (must uncomment-out the og_tag creation above)
    # bga_compiled_1[bga_compiled_1['og_tag'].isnull()]

    bf_eia923 = (
        eia_transformed_dfs["boiler_fuel_eia923"]
        .copy()
        .pipe(_restrict_years, eia923_years, eia860_years)
        .astype(
            {
                "boiler_id": pd.StringDtype(),
                "plant_id_eia": int,
            }
        )
        .assign(
            total_heat_content_mmbtu=lambda x: x.fuel_consumed_units
            * x.fuel_mmbtu_per_unit
        )
    )
    bf_eia923 = (
        bf_eia923.set_index(pd.DatetimeIndex(bf_eia923.report_date))
        .groupby([pd.Grouper(freq="AS"), "plant_id_eia", "boiler_id"])
        .agg({"total_heat_content_mmbtu": pudl.helpers.sum_na})
        .reset_index()
        .drop_duplicates(subset=["plant_id_eia", "report_date", "boiler_id"])
    )

    # Create a list of boilers that were not in bga8
    bf9_bga = bf_eia923.merge(
        bga_compiled_1,
        on=["plant_id_eia", "boiler_id", "report_date"],
        how="outer",
        indicator=True,
    )
    bf9_not_in_bga = bf9_bga[bf9_bga["_merge"] == "left_only"]
    bf9_not_in_bga = bf9_not_in_bga.drop(["_merge"], axis=1)

    # Match the unassociated generators with unassociated boilers
    # This method is assuming that some the strings of the generators and the
    # boilers are the same
    bga_unassn = bga_unassn.merge(
        bf9_not_in_bga[["plant_id_eia", "boiler_id", "report_date"]],
        how="left",
        left_on=["report_date", "plant_id_eia", "generator_id"],
        right_on=["report_date", "plant_id_eia", "boiler_id"],
    )
    bga_unassn["bga_source"] = np.where(
        bga_unassn.boiler_id.notnull(), "string_assn", pd.NA
    )

    bga_compiled_2 = pd.concat([bga_assn, bga_unassn]).fillna(
        {"missing_from_923": True}
    )

    # Connect the gens and boilers in units
    bga_compiled_units = bga_compiled_2.loc[bga_compiled_2["unit_id_eia"].notnull()]
    bga_gen_units = bga_compiled_units.drop(["boiler_id"], axis=1)
    bga_boil_units = bga_compiled_units[
        ["plant_id_eia", "report_date", "boiler_id", "unit_id_eia"]
    ].copy()
    bga_boil_units.dropna(subset=["boiler_id"], inplace=True)

    # merge the units with the boilers
    bga_unit_compilation = bga_gen_units.merge(
        bga_boil_units,
        how="outer",
        on=["plant_id_eia", "report_date", "unit_id_eia"],
        indicator=True,
    )
    # label the bga_source
    bga_unit_compilation.loc[
        bga_unit_compilation["bga_source"].isnull(), "bga_source"
    ] = "unit_connection"
    bga_unit_compilation.drop(["_merge"], axis=1, inplace=True)
    bga_non_units = bga_compiled_2[bga_compiled_2["unit_id_eia"].isnull()]

    # combine the unit compilation and the non units
    bga_compiled_3 = pd.concat([bga_non_units, bga_unit_compilation])

    bga_compiled_3 = bga_compiled_3[
        [
            "plant_id_eia",
            "report_date",
            "generator_id",
            "boiler_id",
            "unit_id_eia",
            "bga_source",
            "boiler_generator_assn_type_code",
            "steam_plant_type_code",
            "net_generation_mwh",
            "missing_from_923",
            "data_maturity",
        ]
    ]

    # label plants that have 'bad' generator records (generators that have MWhs
    # in gens9 but don't have connected boilers) create a df with just the bad
    # plants by searching for the 'bad' generators
    bad_plants = bga_compiled_3[
        (bga_compiled_3["boiler_id"].isnull())
        & (bga_compiled_3["net_generation_mwh"] > 0)
    ].drop_duplicates(subset=["plant_id_eia", "report_date"])
    bad_plants = bad_plants[["plant_id_eia", "report_date"]]

    # merge the 'bad' plants back into the larger frame
    bga_compiled_3 = bga_compiled_3.merge(
        bad_plants, how="outer", on=["plant_id_eia", "report_date"], indicator=True
    )

    # use the indicator to create labels
    bga_compiled_3["plant_w_bad_generator"] = np.where(
        bga_compiled_3["_merge"] == "both", True, False
    )
    # Note: At least one gen has reported MWh in 923, but could not be
    # programmatically mapped to a boiler

    # we don't need this one anymore
    bga_compiled_3 = bga_compiled_3.drop(["_merge"], axis=1)

    # create a label for generators that are unmapped but in 923
    bga_compiled_3["unmapped_but_in_923"] = np.where(
        (bga_compiled_3.boiler_id.isnull())
        & ~bga_compiled_3.missing_from_923
        & (bga_compiled_3.net_generation_mwh == 0),
        True,
        False,
    )

    # create a label for generators that are unmapped
    bga_compiled_3["unmapped"] = np.where(
        bga_compiled_3.boiler_id.isnull(), True, False
    )
    bga_out = bga_compiled_3.drop("net_generation_mwh", axis=1)
    bga_out.loc[bga_out.unit_id_eia.isnull(), "unit_id_eia"] = pd.NA

    bga_for_nx = bga_out[
        ["plant_id_eia", "report_date", "generator_id", "boiler_id", "unit_id_eia"]
    ]
    # If there's no boiler... there's no boiler-generator association
    bga_for_nx = bga_for_nx.dropna(subset=["boiler_id"]).drop_duplicates()

    # Need boiler & generator specific ID strings, or they look like
    # the same node to NX
    bga_for_nx["generators"] = (
        "p"
        + bga_for_nx.plant_id_eia.astype(int).astype(str)
        + "_g"
        + bga_for_nx.generator_id.astype(pd.StringDtype())
    )
    bga_for_nx["boilers"] = (
        "p"
        + bga_for_nx.plant_id_eia.astype(int).astype(str)
        + "_b"
        + bga_for_nx.boiler_id.astype(pd.StringDtype())
    )

    # dataframe to accumulate the unit_ids in
    bga_w_units = pd.DataFrame()
    # We want to start our unit_id counter anew for each plant:
    for pid in bga_for_nx.plant_id_eia.unique():
        bga_byplant = bga_for_nx[bga_for_nx.plant_id_eia == pid].copy()

        # Create a graph from the dataframe of boilers and generators. It's a
        # multi-graph, meaning the same nodes can be connected by more than one
        # edge -- this allows us to preserve multiple years worth of boiler
        # generator association information for later inspection if need be:
        bga_graph = nx.from_pandas_edgelist(
            bga_byplant,
            source="generators",
            target="boilers",
            edge_attr=True,
            create_using=nx.MultiGraph(),
        )

        # Each connected sub-graph is a generation unit:
        gen_units = [
            bga_graph.subgraph(c).copy() for c in nx.connected_components(bga_graph)
        ]

        # Assign a unit_id to each subgraph, and extract edges into a dataframe
        for unit_id, unit in zip(range(len(gen_units)), gen_units):
            # All the boiler-generator association graphs should be bi-partite,
            # meaning generators only connect to boilers, and boilers only
            # connect to generators.
            if not nx.algorithms.bipartite.is_bipartite(unit):
                raise AssertionError(
                    f"Non-bipartite generation unit graph found."
                    f"plant_id_eia={pid}, unit_id_pudl={unit_id}."
                )
            nx.set_edge_attributes(unit, name="unit_id_pudl", values=unit_id + 1)
            new_unit_df = nx.to_pandas_edgelist(unit)
            bga_w_units = pd.concat([bga_w_units, new_unit_df])

    bga_w_units = bga_w_units.drop(["source", "target"], axis=1)

    # Check whether the PUDL unit_id values we've inferred conflict with
    # the unit_id_eia values that were reported to EIA. Are there any PUDL
    # unit_id values that have more than 1 EIA unit_id_eia within them?
    bga_unit_id_eia_counts = (
        bga_w_units.groupby(["plant_id_eia", "unit_id_pudl"])["unit_id_eia"]
        .nunique()
        .to_frame()
        .reset_index()
    )
    bga_unit_id_eia_counts = bga_unit_id_eia_counts.rename(
        columns={"unit_id_eia": "unit_id_eia_count"}
    )
    bga_unit_id_eia_counts = pd.merge(
        bga_w_units, bga_unit_id_eia_counts, on=["plant_id_eia", "unit_id_pudl"]
    )
    too_many_codes = bga_unit_id_eia_counts[
        bga_unit_id_eia_counts.unit_id_eia_count > 1
    ]
    too_many_codes = (
        too_many_codes[too_many_codes.unit_id_eia.notna()]
        .groupby(["plant_id_eia", "unit_id_pudl"])["unit_id_eia"]
        .unique()
    )
    for row in too_many_codes.items():
        logger.warning(
            f"Multiple EIA unit codes:"
            f"plant_id_eia={row[0][0]}, "
            f"unit_id_pudl={row[0][1]}, "
            f"unit_id_eia={row[1]}"
        )
    bga_w_units = bga_w_units.drop("unit_id_eia", axis=1)

    # These assertions test that all boilers and generators ended up in the
    # same unit_id across all the years of reporting:
    pgu_gb = bga_w_units.groupby(["plant_id_eia", "generator_id"])["unit_id_pudl"]
    if not (pgu_gb.nunique() <= 1).all():
        # raise AssertionError("Inconsistent inter-annual BGA assignment!")
        logger.error("Inconsistent inter-annual plant-generator-units!")
    pbu_gb = bga_w_units.groupby(["plant_id_eia", "boiler_id"])["unit_id_pudl"]
    if not (pbu_gb.nunique() <= 1).all():
        # raise AssertionError("Inconsistent inter-annual BGA assignment!")
        logger.error("Inconsistent inter-annual plant-boiler-units!")

    bga_w_units = (
        bga_w_units.drop("report_date", axis=1)
        .loc[:, ["plant_id_eia", "unit_id_pudl", "generator_id", "boiler_id"]]
        .drop_duplicates()
    )

    bga_out = pd.merge(
        left=bga_out,
        right=bga_w_units,
        how="left",
        on=["plant_id_eia", "generator_id", "boiler_id"],
    ).astype({"unit_id_pudl": pd.Int64Dtype()})

    if not debug:
        bga_out = (
            bga_out[
                ~bga_out.missing_from_923
                & ~bga_out.plant_w_bad_generator
                & ~bga_out.unmapped_but_in_923
                & ~bga_out.unmapped
            ]
            .drop(
                [
                    "missing_from_923",
                    "plant_w_bad_generator",
                    "unmapped_but_in_923",
                    "unmapped",
                ],
                axis=1,
            )
            .drop_duplicates(
                subset=["report_date", "plant_id_eia", "boiler_id", "generator_id"]
            )
        )

    bga_out = apply_pudl_dtypes(bga_out, group="eia")
    eia_transformed_dfs["boiler_generator_assn_eia860"] = bga_out

    return eia_transformed_dfs


def _restrict_years(
    df: pd.DataFrame,
    eia923_years: list[int] | None = None,
    eia860_years: list[int] | None = None,
) -> pd.DataFrame:
    """Restricts eia years for boiler generator association."""
    if eia923_years is None:
        eia923_years = DataSource.from_id("eia923").working_partitions["years"]
    if eia860_years is None:
        eia860_years = DataSource.from_id("eia860").working_partitions["years"]

    bga_years = set(eia860_years) & set(eia923_years)
    df = df[df.report_date.dt.year.isin(bga_years)]
    return df


def map_balancing_authority_names_to_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Build a map of the BA names to their most frequently associated BA codes.

    We know there are some inconsistent pairings of codes and names so we grab the most
    consistently reported combo, making the assumption that the most consistent pairing
    is most likely to be the correct.

    Args:
        df: a data table with columns ``balancing_authority_code_eia`` and
            ``balancing_authority_name_eia``

    Returns:
        a table with a unique index of ``balancing_authority_name_eia`` and a column of
        ``balancing_authority_code``.
    """
    return (
        # count the unquie combos of BA code and name's.
        df.assign(count=1)
        .groupby(
            by=["balancing_authority_name_eia", "balancing_authority_code_eia"],
            observed=True,
        )[["count"]]
        .count()
        .reset_index()
        # then sort so the most common is at the top.
        .sort_values(by=["count"], ascending=False)
        # then drop duplicates on the BA name
        .drop_duplicates(["balancing_authority_name_eia"])
        .set_index("balancing_authority_name_eia")
        .drop(columns=["count"])
    )


def fillna_balancing_authority_codes_via_names(df: pd.DataFrame) -> pd.DataFrame:
    """Fill null balancing authority (BA) codes via a map of the BA names to codes.

    There are a handful of missing ``balancing_authority_code_eia``'s that are easy to
    map given the balancing_authority_name_eia. This function fills in null BA codes
    using the BA names. The map ofo the BA names to codes is generated via
    :func:`map_balancing_authority_names_to_codes`.

    Args:
        df: a data table with columns ``balancing_authority_code_eia`` and
            ``balancing_authority_name_eia``
    """
    pre_len = len(df[df.balancing_authority_code_eia.notnull()])

    # Identify the most common mapping from a BA name to a BA code:
    ba_name_to_code_map = map_balancing_authority_names_to_codes(df)

    null_ba_code_mask = (
        df.balancing_authority_code_eia.isnull()
        & df.balancing_authority_name_eia.notnull()
        & df.balancing_authority_name_eia.isin(ba_name_to_code_map.index)
    )
    # For each row with a missing BA code, identify the likely code based on its
    # associated BA name. Here the argument to map() is a Series containing
    # balancing_authority_code that's indexed by balancing_authority_name.
    ba_codes = df.loc[null_ba_code_mask, "balancing_authority_name_eia"].map(
        ba_name_to_code_map.balancing_authority_code_eia
    )
    # Fill in the null BA codes
    df.loc[null_ba_code_mask, "balancing_authority_code_eia"] = ba_codes

    post_len = len(df[df.balancing_authority_code_eia.notnull()])
    logger.info(f"filled {post_len - pre_len} balancing authority codes using names.")
    return df


def fix_balancing_authority_codes_with_state(
    plants: pd.DataFrame, plants_entity: pd.DataFrame
) -> pd.DataFrame:
    """Fix selective balancing_authority_code_eia's based on states.

    There are some known errors in the ``balancing_authority_code_eia`` column that we
    can identify and fix based on the state where the plant is located. Where we update
    the ``balancing_authority_code_eia`` column, we also update the
    ``balancing_authority_name_eia`` column using the name generated by
    :func:`map_balancing_authority_names_to_codes`.

    This function should only be applied post-:func:`harvesting`. The ``state`` column
    is a "static" entity column so the first step in this function is merging the static
    and annually varying plants together. Then we fix known errors in the BA codes:

    * reported PACE, but state is OR or CA, code should be PACW
    * reported PACW, but state is UT, code should be PACE

    Args:
        plants: annually harvested plant table with columns: ``plant_id_eia``,
            ``report_date`` and ``balancing_authority_code_eia``.
        plants_entity: static harvested plant table with columns: ``plant_id_eia`` and
            ``state``.

    Returns:
        plants table that has the same set of columns and rows, with cleaned
        ``balancing_authority_code_eia`` column and an updated corresponding
        ``balancing_authority_name_eia`` column.
    """
    # Identify the most common mapping from a BA name to a BA code:
    ba_name_to_code_map = map_balancing_authority_names_to_codes(plants)
    ba_name_to_code_map.reset_index(inplace=True)

    # Prior to 2013, there are no BA codes or names. Running a pre-2013 subset of data
    # through the transform will thus return an empty ba_name_to_code_map.
    if (
        not ba_name_to_code_map.empty
        and plants.balancing_authority_code_eia.isin(["PACW", "PACE"]).any()
    ):
        logger.info("Spot fixing incorrect PACW/PACE BA codes and names.")

        plants = plants.merge(
            plants_entity[["plant_id_eia", "state"]],  # only merge in state, drop later
            on=["plant_id_eia"],
            how="left",
            validate="m:1",
        )
        BACodeFix = namedtuple(
            "BACodeFix", ["ba_code_found", "ba_code_fix", "ba_name_fix", "states"]
        )
        fixes = [
            BACodeFix(
                "PACE",
                "PACW",
                ba_name_to_code_map.loc[
                    ba_name_to_code_map.balancing_authority_code_eia == "PACW",
                    "balancing_authority_name_eia",
                ].tolist()[0],
                ["OR", "CA"],
            ),
            BACodeFix(
                "PACW",
                "PACE",
                ba_name_to_code_map.loc[
                    ba_name_to_code_map.balancing_authority_code_eia == "PACE",
                    "balancing_authority_name_eia",
                ].tolist()[0],
                ["UT"],
            ),
        ]
        for fix in fixes:
            plants.loc[
                (plants.balancing_authority_code_eia == fix.ba_code_found)
                & (plants.state.isin(fix.states)),
                ["balancing_authority_code_eia", "balancing_authority_name_eia"],
            ] = [fix.ba_code_fix, fix.ba_name_fix]
        plants = plants.drop(columns=["state"])

    return plants


def transform(
    eia_transformed_dfs, eia_settings: EiaSettings = EiaSettings(), debug=False
):
    """Creates DataFrames for EIA Entity tables and modifies EIA tables.

    This function coordinates two main actions: generating the entity tables
    via ``harvesting()`` and generating the boiler generator associations via
    ``_boiler_generator_assn()``.

    There is also some removal of tables that are no longer needed after the
    entity harvesting is finished.

    Args:
        eia_transformed_dfs (dict): a dictionary of table names (kays) and
            transformed dataframes (values).
        settings: Object containing validated settings
            relevant to EIA datasets.
        debug (bool): if true, informational columns will be added into
            boiler_generator_assn

    Returns:
        tuple: two dictionaries having table names as keys and
        dataframes as values for the entity tables transformed EIA dataframes
    """
    # create the empty entities df to fill up
    entities_dfs = {}

    # for each of the entities, harvest the static and annual columns.
    # the order of the entities matter! the
    for entity in ENTITIES:
        logger.info(f"Harvesting IDs & consistently static attributes for EIA {entity}")

        harvesting(
            entity,
            eia_transformed_dfs,
            entities_dfs,
            debug=debug,
            eia860m=eia_settings.eia860.eia860m,
        )
    _boiler_generator_assn(
        eia_transformed_dfs,
        eia923_years=eia_settings.eia923.years,
        eia860_years=eia_settings.eia860.years,
        debug=debug,
    )
    # get rid of the original annual dfs in the transformed dict
    remove = ["generators", "plants", "utilities"]
    for entity in remove:
        eia_transformed_dfs[f"{entity}_eia860"] = eia_transformed_dfs.pop(
            f"{entity}_annual_eia", f"{entity}_annual_eia"
        )
    # remove the boilers annual table bc it has no columns
    eia_transformed_dfs.pop(
        "boilers_annual_eia",
    )

    eia_transformed_dfs["plants_eia860"] = fillna_balancing_authority_codes_via_names(
        df=eia_transformed_dfs["plants_eia860"]
    ).pipe(
        fix_balancing_authority_codes_with_state,
        plants_entity=entities_dfs["plants_entity_eia"],
    )
    return entities_dfs, eia_transformed_dfs
