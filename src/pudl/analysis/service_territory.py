"""
Compile historical utility and balancing area territories.

Use the mapping of utilities to counties, and balancing areas to utilities, available
within the EIA 861, in conjunction with the US Census geometries for counties, to
infer the historical spatial extent of utility and balancing area territories. Output
the resulting geometries for use in other applications.

"""
import argparse
import logging
import math
import pathlib
import sys
import zipfile

import coloredlogs
import contextily as ctx
import geopandas
import pandas as pd
import sqlalchemy as sa
from matplotlib import pyplot as plt

import pudl

logger = logging.getLogger(__name__)

################################################################################
# Coordinate Reference Systems used in different contexts
################################################################################
MAP_CRS = "EPSG:3857"  # For mapping w/ OSM baselayer tiles
CALC_CRS = "ESRI:102003"  # For accurate area calculations


################################################################################
# Outside data that we rely on for this analysis
################################################################################
def get_census2010_gdf(pudl_settings, layer):
    """
    Obtain a GeoDataFrame containing US Census demographic data for 2010.

    If we don't have it locally already, download the US Census DP1 data and store it
    under the "local/uscb/census2010" directory within the PUDL datastore directory,
    as it isn't yet integrated into the core PUDL data management.

    Read the specified layer out of the downloaded geodatabase, and return the
    resulting geopandas.GeoDataFrame. The column names and types are not altered from
    the US Census originals.

    Args:
        pudl_settings (dict): PUDL Settings dictionary.
        layer (str): Indicates which layer of the Census GeoDB to read.
            Must be one of "state", "county", or "tract".

    Returns:
        geopandas.GeoDataFrame: DataFrame containing the US Census
        Demographic Profile 1 (DP1) data, aggregated to the layer

    """
    census2010_url = "http://www2.census.gov/geo/tiger/TIGER2010DP1/Profile-County_Tract.zip"
    census2010_dir = pathlib.Path(
        pudl_settings["data_dir"]) / "local/uscb/census2010"
    census2010_dir.mkdir(parents=True, exist_ok=True)
    census2010_zipfile = census2010_dir / "census2010.zip"
    census2010_gdb_dir = census2010_dir / "census2010.gdb"

    if not census2010_gdb_dir.is_dir():
        logger.warning("No Census GeoDB found. Downloading from US Census Bureau.")
        # Download to appropriate location
        pudl.helpers.download_zip_url(census2010_url, census2010_zipfile)
        # Unzip because we can't use zipfile paths with geopandas
        with zipfile.ZipFile(census2010_zipfile, 'r') as zip_ref:
            zip_ref.extractall(census2010_dir)
            # Grab the originally extracted directory name so we can change it:
            extract_root = census2010_dir / \
                pathlib.Path(zip_ref.filelist[0].filename)
        logger.warning(f"Rename {extract_root} to {census2010_gdb_dir}")
        extract_root.rename(census2010_gdb_dir)
    else:
        logger.info("We've already got the 2010 Census GeoDB.")

    logger.info("Extracting the GeoDB into a GeoDataFrame")
    layers = {
        "state": "State_2010Census_DP1",
        "county": "County_2010Census_DP1",
        "tract": "Tract_2010Census_DP1",
    }
    census_gdf = geopandas.read_file(
        census2010_gdb_dir,
        driver='FileGDB',
        layer=layers[layer],
    )
    return census_gdf


def get_all_utils(pudl_out):
    """
    Compile IDs and Names of all known EIA Utilities.

    Grab all EIA utility names and IDs from both the EIA 861 Service Territory table and
    the EIA 860 Utility entity table. This is a temporary function that's only needed
    because we haven't integrated the EIA 861 information into the entity harvesting
    process and PUDL database yet.

    Args:
        pudl_out (pudl.output.pudltabl.PudlTabl): The PUDL output object which should be
            used to obtain PUDL data.

    Returns:
        pandas.DataFrame: Having 2 columns ``utility_id_eia`` and ``utility_name_eia``.

    """
    return (
        pd.concat([
            pudl_out.utils_eia860()[["utility_id_eia", "utility_name_eia"]],
            pudl_out.service_territory_eia861()[["utility_id_eia", "utility_name_eia"]],
        ]).dropna(subset=["utility_id_eia"]).drop_duplicates(subset=["utility_id_eia"])
    )


################################################################################
# Functions that compile geometries based on EIA 861 data tables:
################################################################################
def get_territory_fips(ids, assn, assn_col, st_eia861, limit_by_state=True):
    """
    Compile county FIPS codes associated with an entity's service territory.

    For each entity identified by ids, look up the set of counties associated
    with that entity on an annual basis. Optionally limit the set of counties
    to those within states where the selected entities reported activity elsewhere
    within the EIA 861 data.

    Args:
        ids (iterable of ints): A collection of EIA utility or balancing authority IDs.
        assn (pandas.DataFrame): Association table, relating ``report_date``,
        ``state``, and ``utility_id_eia`` to each other, as well as the
            column indicated by ``assn_col`` -- if it's not ``utility_id_eia``.
        assn_col (str): Label of the dataframe column in ``assn`` that contains
            the ID of the entities of interest. Should probably be either
            ``balancing_authority_id_eia`` or ``utility_id_eia``.
        st_eia861 (pandas.DataFrame): The EIA 861 Service Territory table.
        limit_by_state (bool): Whether to require that the counties associated
            with the balancing authority are inside a state that has also been
            seen in association with the balancing authority and the utility
            whose service territory contians the county.

    Returns:
        pandas.DataFrame: A table associating the entity IDs with a collection of
        counties annually, identifying counties both by name and county_id_fips
        (both state and state_id_fips are included for clarity).

    """
    # Limit the association table to only the relevant values:
    assn = assn.loc[assn[assn_col].isin(ids)]

    if not limit_by_state:
        assn = assn.drop("state", axis="columns")

    return (
        pd.merge(assn, st_eia861, how="inner")
        .loc[:, [
            "report_date", assn_col,
            "state", "county",
            "state_id_fips", "county_id_fips"
        ]]
        .drop_duplicates()
    )


def add_geometries(df, census_gdf, dissolve=False, dissolve_by=None):
    """
    Merge census geometries into dataframe on county_id_fips, optionally dissolving.

    Merge the US Census county-level geospatial information into the DataFrame df
    based on the the column county_id_fips (in df), which corresponds to the column
    GEOID10 in census_gdf.

    Args:
        df (pandas.DataFrame): A DataFrame containing a county_id_fips column.
        census_gdf (geopandas.GeoDataFrame): A GeoDataFrame based on the US Census
            demographic profile (DP1) data at county resolution, with the original
            column names as published by US Census.
        dissolve (bool): If True, dissolve individual county geometries into larger
            service territories.
        dissolve_by (list): The columns to group by in the dissolve. For example,
            dissolve_by=["report_date", "utility_id_eia"] might provide annual utility
            service territories, while ["report_date", "balancing_authority_id_eia"]
            would provide annual balancing authority territories.

    Returns:
        geopandas.GeoDataFrame

    """
    out_gdf = (
        census_gdf[["GEOID10", "NAMELSAD10", "geometry"]]
        .rename(columns={
            "GEOID10": "county_id_fips",
            "NAMELSAD10": "county_name_census"
        })
        .merge(df, how="right")
    )
    if dissolve is True:
        out_gdf = (
            out_gdf.drop_duplicates(subset=dissolve_by + ["county_id_fips", ])
            .dissolve(by=dissolve_by)
            .drop([
                "county_id_fips",
                "county",
                "county_name_census",
                "state",
                "state_id_fips"
            ], axis="columns")
            .reset_index()
        )
    return out_gdf


def get_territory_geometries(ids,
                             assn,
                             assn_col,
                             st_eia861,
                             census_gdf,
                             limit_by_state=True,
                             dissolve=False):
    """
    Compile service territory geometries based on county_id_fips.

    Calls ``get_territory_fips`` to generate the list of counties associated with
    each entity identified by ``ids``, and then merges in the corresponding county
    geometries from the US Census DP1 data passed in via ``census_gdf``.

    Optionally dissolve all of the county level geometries into a single geometry for
    each combination of entity and year.

    Note:
        Dissolving geometires is a costly operation, and may take half an hour or more
        if you are processing all entities for all years. Dissolving also means that all
        the per-county information will be lost, rendering the output inappropriate for
        use in many analyses. Dissolving is mostly useful for generating visualizations.

    Args:
        ids (iterable of ints): A collection of EIA balancing authority IDs.
        assn (pandas.DataFrame): Association table, relating ``report_date``,
        ``state``, and ``utility_id_eia`` to each other, as well as the
            column indicated by ``assn_col`` -- if it's not ``utility_id_eia``.
        assn_col (str): Label of the dataframe column in ``assn`` that contains
            the ID of the entities of interest. Should probably be either
            ``balancing_authority_id_eia`` or ``utility_id_eia``.
        st_eia861 (pandas.DataFrame): The EIA 861 Service Territory table.
        census_gdf (geopandas.GeoDataFrame): The US Census DP1 county-level geometries
            as returned by get_census2010_gdf().
        limit_by_state (bool): Whether to require that the counties associated
            with the balancing authority are inside a state that has also been
            seen in association with the balancing authority and the utility
            whose service territory contians the county.
        dissolve (bool): If False, each record in the compiled territory will correspond
            to a single county, with a county-level geometry, and there will be many
            records enumerating all the counties associated with a given
            balancing_authority_id_eia in each year. If dissolve=True, all of the
            county-level geometries for each utility in each year will be merged
            together ("dissolved") resulting in a single geometry and record for each
            balancing_authority-year.

    Returns:
        geopandas.GeoDataFrame

    """
    return (
        get_territory_fips(
            ids=ids,
            assn=assn,
            assn_col=assn_col,
            st_eia861=st_eia861,
            limit_by_state=limit_by_state,
        )
        .pipe(
            add_geometries,
            census_gdf,
            dissolve=dissolve,
            dissolve_by=["report_date", assn_col]
        )
    )


def compile_geoms(pudl_out,
                  census_counties,
                  entity_type,  # "ba" or "util"
                  dissolve=False,
                  limit_by_state=True,
                  save=True):
    """
    Compile all available utility or balancing authority geometries.

    Args:
        pudl_out (pudl.output.pudltabl.PudlTabl): A PUDL output object, which will
            be used to extract and cache the EIA 861 tables.
        census_counties (geopandas.GeoDataFrame): A GeoDataFrame containing the county
            level US Census DP1 data and county geometries.
        entity_type (str): The type of service territory geometry to compile. Must be
            either "ba" (balancing authority) or "util" (utility).
        dissolve (bool): Whether to dissolve the compiled geometries to the
            utility/balancing authority level, or leave them as counties.
        limit_by_state (bool): Whether to limit included counties to those with
            observed EIA 861 data in association with the state and utility/balancing
            authority.
        save (bool): If True, save the compiled GeoDataFrame as a GeoParquet file before
            returning. Especially useful in the case of dissolved geometries, as they
            are computationally expensive.

    Returns:
        geopandas.GeoDataFrame

    """
    logger.info(
        f"Compiling {entity_type} geometries with {dissolve=} and {limit_by_state=}.")

    if entity_type == "ba":
        ids = pudl_out.balancing_authority_eia861().balancing_authority_id_eia.unique()
        assn = pudl_out.balancing_authority_assn_eia861()
        assn_col = "balancing_authority_id_eia"
    elif entity_type == "util":
        ids = get_all_utils(pudl_out).utility_id_eia.unique()
        assn = pudl_out.utility_assn_eia861()
        assn_col = "utility_id_eia"
    else:
        raise ValueError(f"Got {entity_type=}, but need either 'ba' or 'util'")

    # Identify all Utility IDs with service territory information
    geom = get_territory_geometries(
        ids=ids,
        assn=assn,
        assn_col=assn_col,
        st_eia861=pudl_out.service_territory_eia861(),
        census_gdf=census_counties,
        limit_by_state=limit_by_state,
        dissolve=dissolve,
    )
    if save:
        _save_geoparquet(
            geom,
            entity_type=entity_type,
            dissolve=dissolve,
            limit_by_state=limit_by_state
        )

    return geom


def _save_geoparquet(gdf, entity_type, dissolve, limit_by_state):
    # For filenames based on input args:
    dissolved = ""
    if dissolve:
        dissolved = "_dissolved"
    else:
        # States & counties only remain at this point if we didn't dissolve
        for col in ("county_id_fips", "state_id_fips"):
            # pandas.NA values are not compatible with Parquet Strings yet.
            gdf[col] = gdf[col].fillna("")
    limited = ""
    if limit_by_state:
        limited = "_limited"
    # Save the geometries to a GeoParquet file
    fn = f"{entity_type}_geom{limited+dissolved}.pq"
    gdf.to_parquet(fn, index=False)


################################################################################
# Functions for visualizing the service territory geometries
################################################################################
def plot_historical_territory(gdf, id_col, id_val):
    """
    Plot all the historical geometries defined for the specified entity.

    This is useful for exploring how a particular entity's service territory has evolved
    over time, or for identifying individual missing or inaccurate territories.

    Args:
        gdf (geopandas.GeoDataFrame): A geodataframe containing geometries pertaining
            electricity planning areas. Can be broken down by county FIPS code, or
            have a single record containing a geometry for each combination of
            report_date and the column being used to select planning areas (see
            below).
        id_col (str): The label of a column in gdf that identifies the planning area
            to be visualized, like utility_id_eia, balancing_authority_id_eia, or
            balancing_authority_code_eia.
        id_val (str or int): The value identifying the

    Returns:
        None

    """
    if id_col not in gdf.columns:
        raise ValueError(f"The input id_col {id_col} doesn't exist in this GDF.")
    logger.info(f"Plotting historical territories for {id_col}=={id_val}.")

    # Pare down the GDF so this all goes faster
    entity_gdf = gdf[gdf[id_col] == id_val]
    if "county_id_fips" in entity_gdf.columns:
        entity_gdf = entity_gdf.drop_duplicates(
            subset=["report_date", "county_id_fips"])
    entity_gdf["report_year"] = entity_gdf.report_date.dt.year
    logger.info(f"Plotting service territories of {len(entity_gdf)} {id_col} records.")

    # Create a grid of subplots sufficient to hold all the years:
    years = entity_gdf.report_year.sort_values().unique()
    ncols = 5
    nrows = math.ceil(len(years) / ncols)
    fig, axes = plt.subplots(
        ncols=ncols, nrows=nrows, figsize=(15, 3 * nrows),
        sharex=True, sharey=True, facecolor="white")
    fig.suptitle(f"{id_col} == {id_val}")

    for year, ax in zip(years, axes.flat):
        ax.set_title(f"{year}")
        ax.set_xticks([])
        ax.set_yticks([])
        year_gdf = entity_gdf.loc[entity_gdf.report_year == year]
        year_gdf.plot(ax=ax, linewidth=0.1)
    plt.show()


def plot_all_territories(gdf,
                         report_date,
                         respondent_type=("balancing_authority", "utility"),
                         color="black",
                         alpha=0.25,
                         basemap=True):
    """
    Plot all of the planning areas of a given type for a given report date.

    Todo:
        This function needs to be made more general purpose, and less
        entangled with the FERC 714 data.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame containing planning area
            geometries, organized by respondent_id_ferc714 and report_date.

        report_date (datetime): A Datetime indicating what year's planning
            areas should be displayed.
        respondent_type (str or iterable): Type of respondent whose planning
            areas should be displayed. Either "utility" or
            "balancing_authority" or an iterable collection containing both.
        color (str): Color to use for the planning areas.
        alpha (float): Transparency to use for the planning areas.
        basemap (bool): If true, use the OpenStreetMap tiles for context.

    Returns:
        matplotlib.axes.Axes

    """
    unwanted_respondent_ids = (  # noqa: F841 variable is used, in df.query() below
        112,  # Alaska
        133,  # Alaska
        178,  # Hawaii
        301,  # PJM Dupe
        302,  # PJM Dupe
        303,  # PJM Dupe
        304,  # PJM Dupe
        305,  # PJM Dupe
        306,  # PJM Dupe
    )
    if type(respondent_type) == str:
        respondent_type = (respondent_type, )

    plot_gdf = (
        gdf.query("report_date==@report_date")
        .query("respondent_id_ferc714 not in @unwanted_respondent_ids")
        .query("respondent_type in @respondent_type")
    )
    ax = plot_gdf.plot(figsize=(20, 20), color=color, alpha=0.25, linewidth=1)
    plt.title(f"FERC 714 {', '.join(respondent_type)} planning areas for {report_date}")
    if basemap:
        ctx.add_basemap(ax)
    plt.show()
    return ax


################################################################################
# Functions that provide a CLI to the service territory module
################################################################################
def parse_command_line(argv):
    """
    Parse script command line arguments. See the -h option.

    Args:
        argv (list): command line arguments including caller file name.

    Returns:
        dict: A dictionary mapping command line arguments to their values.

    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-d",
        "--dissolve",
        dest="dissolve",
        action="store_true",
        default=False,
        help="Dissolve county level geometries to utility or balancing authorities",
    )

    return parser.parse_args(argv[1:])


def main():
    """Compile historical utility and balancing area territories."""
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)
    pudl_settings = pudl.workspace.setup.get_defaults()
    pudl_engine = sa.create_engine(pudl_settings['pudl_db'])
    pudl_out = pudl.output.pudltabl.PudlTabl(pudl_engine)
    # Load the US Census DP1 county data:
    census_counties = get_census2010_gdf(pudl_settings, layer="county")

    kwargs_dicts = [
        {"entity_type": "util", "limit_by_state": False},
        {"entity_type": "util", "limit_by_state": True},
        {"entity_type": "ba", "limit_by_state": True},
        {"entity_type": "ba", "limit_by_state": False},
    ]

    for kwargs in kwargs_dicts:
        _ = compile_geoms(
            pudl_out,
            census_counties=census_counties,
            dissolve=args.dissolve,
            **kwargs,
        )


if __name__ == "__main__":
    sys.exit(main())
