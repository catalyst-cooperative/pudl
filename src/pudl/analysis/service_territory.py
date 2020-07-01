"""
Compile historical utility and balancing area territories.

Use the mapping of utilities to counties, and balancing areas to utilities, available
within the EIA 861, in conjunction with the US Census geometries for counties, to
infer the historical spatial extent of utility and balancing area territories. Output
the resulting geometries for use in other applications.

"""

import argparse
import logging
import pathlib
import sys
import zipfile

import coloredlogs
import contextily as ctx
import geopandas
import matplotlib.pyplot as plt
import pandas as pd

import pudl
import pudl.constants as pc

logger = logging.getLogger(__name__)

################################################################################
# Coordinate Reference Systems used in different contexts
################################################################################
MAP_CRS = "EPSG:3857"  # For mapping w/ OSM baselayer tiles
CALC_CRS = "ESRI:102003"  # For accurate area calculations


def get_census2010_gdf(pudl_settings, layer):
    """
    Obtain a GeoDataFrame containing US Census demographic data for 2010.

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
        logger.info("No Census GeoDB found. Downloading from US Census Bureau.")
        # Download to appropriate location
        pudl.helpers.download_zip_url(census2010_url, census2010_zipfile)
        # Unzip because we can't use zipfile paths with geopandas
        with zipfile.ZipFile(census2010_zipfile, 'r') as zip_ref:
            zip_ref.extractall(census2010_dir)
            # Grab the UUID based directory name so we can change it:
            extract_root = census2010_dir / \
                pathlib.Path(zip_ref.filelist[0].filename).parent
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


################################################################################
# Functions that compile geometries based on EIA 861 data tables:
################################################################################
def balancing_authority_counties(ba_ids,
                                 st_eia861,
                                 ba_assn_eia861,
                                 limit_by_state=True):
    """
    Compile the counties associated with the selected balancing authorities by year.

    For each balancing authority identified by ba_ids, look up the set of counties
    associated with that BA on an annual basis. Optionally limit the set of counties
    to those within states where the selected balancing authorities have been seen
    in association with each utility whose counties make up the BA territory.

    Args:
        ba_ids (iterable of ints): A collection of EIA balancing authority IDs.
        st_eia861 (pandas.DataFrame): The EIA 861 Service Territory table.
        ba_assn_eia861 (pandas.DataFrame): The EIA 861 Balancing Authority
            association table, indicating which combinations of utility IDs and
            states a balancing authority ID has been associated with each year.
        limit_by_state (bool): Whether to require that the counties associated
            with the balancing authority are inside a state that has also been
            seen in association with the balancing authority and the utility
            whose service territory contians the county.

    Returns:
        pandas.DataFrame: A table associating the given balancing authority
        IDs with a collection of counties annually, identifying counties both by
        name and county_id_fips (state and state_id_fips are included for
        clarity).

    """
    util_assn = ba_assn_eia861.loc[ba_assn_eia861.balancing_authority_id_eia.isin(
        ba_ids)]

    if not limit_by_state:
        util_assn = util_assn.drop("state", axis="columns")

    ba_counties = (
        pd.merge(util_assn, st_eia861)
        .drop(["utility_id_eia", "utility_name_eia"], axis="columns")
        .drop_duplicates()
    )
    return ba_counties


def utility_counties(util_ids, st_eia861):
    """
    Compile the list of counties associated with the given utility IDs.

    Select all records in the service territory_eia861 table that pertain to the
    input utility_id_eia values. These records contain the state and county FIPS IDs
    for the counties served by the utility in each year. In combination with the
    similarly identified US Census geometries, these IDs can be used to compile
    geospatial information about utility service territories.

    Note:
        Currently this function does not allow the same kind of limiting based on
        observed state associations that the analogous balancing_authority_counites
        function does. The counties returned represent the entire service territory of
        each utility. It may be desirable to make the two processes more directly
        comparable, but that would require a utility_assn_eia861 table (which could
        also be implemented by allowing NA balancing_authority_id_eia values in the
        balancing_authority_assn_eia861 table)

    Args:
        util_ids (iterable of ints): The EIA Utility IDs associated with the utilities
            whose service territories we are compiling.
        st_eia861 (pandas.DataFrame): The service_territory_eia861 dataframe to use
            for looking up the state and county FIPS IDs associated with the utilities.

    Returns
        pandas.DataFrame: A dataframe containing columns: report_year, utility_id_eia,
        state, state_id_fips, county, and county_id_fips, including all of the counties
        in each year that are associated with the service territory of each
        utility_id_eia value.

    """
    return (
        st_eia861.loc[st_eia861.utility_id_eia.isin(util_ids)]
        .drop("utility_name_eia", axis="columns")
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
            dissolve_by=["report_data", "utility_id_eia"] might provide annual utility
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


def utility_geometries(util_ids, st_eia861, census_gdf, dissolve=False):
    """Compile utility territory geometries based on county_id_fips."""
    return (
        utility_counties(
            util_ids=util_ids,
            st_eia861=st_eia861
        )
        .pipe(
            add_geometries,
            census_gdf,
            dissolve=dissolve,
            dissolve_by=["report_date", "utility_id_eia"]
        )
    )


def balancing_authority_geometries(ba_ids,
                                   st_eia861,
                                   ba_assn_eia861,
                                   census_gdf,
                                   dissolve=False,
                                   limit_by_state=True):
    """Compile balancing authority territory geometries based on county_id_fips."""
    return (
        balancing_authority_counties(
            ba_ids=ba_ids,
            st_eia861=st_eia861,
            ba_assn_eia861=ba_assn_eia861,
            limit_by_state=limit_by_state,
        )
        .pipe(
            add_geometries,
            census_gdf,
            dissolve=dissolve,
            dissolve_by=["report_date", "balancing_authority_id_eia"]
        )
    )


################################################################################
# Functions for visualizing the service territory geometries
################################################################################
def plot_historical_territory(gdf, entity=None):
    """
    Plot all the historical planning area geometries defined for the specified entity.

    This is useful for exploring how a particular entity's service territory has evolved
    over time, or for identifying individual missing or inaccurate territories.

    Args:
        gdf (geopandas.GeoDataFrame): A geodataframe containing geometries pertaining
            electricity planning areas. Can be broken down by county FIPS code, or
            have a single record containing a geometry for each combination of
            report_date and the column being used to select planning areas (see
            below).
        entity (dict): A dictionary with a single element. The key is the label of a
            column in gdf that identifies planning area entities, like utility_id_eia
            or balancing_authority_id_eia, or balancing_authority_code_eia. The value
            is an iterable collection of values indicating which entities' historical
            geometries should be plotted.

    Returns:
        None

    """
    assert len(entity) == 1
    col = list(entity.keys())[0]
    assert col in gdf.columns
    logger.info(f"Plotting historical territories by {col}.")

    # Pare down the GDF so this all goes faster
    entities_gdf = gdf[gdf[col].isin(entity[col])]
    if "county_id_fips" in entities_gdf.columns:
        entities_gdf = (
            entities_gdf.drop_duplicates(subset=["report_date", "county_id_fips", col])
        )
    entities_gdf = entities_gdf.assign(report_year=lambda x: x.report_date.dt.year)
    logger.info(f"{len(entities_gdf)=}")

    for pa_id in entity[col]:
        logger.info(f"Plotting {pa_id} territories...")
        entity_gdf = entities_gdf.loc[entities_gdf[col] == pa_id]
        fig, axes = plt.subplots(
            ncols=6, nrows=3, figsize=(18, 9),
            sharex=True, sharey=True, facecolor="white")
        fig.suptitle(f"{col} == {pa_id}")

        for year, ax in zip(range(2001, 2019), axes.flat):
            ax.set_title(f"{year}")
            ax.set_xticks([])
            ax.set_yticks([])
            year_gdf = entity_gdf.loc[entity_gdf["report_year"] == year]
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
        "-u",
        "--utility",
        dest="do_utils",
        type=bool,
        default=False,
        help="Compile geometries for utility service territories."
    )
    parser.add_argument(
        "-b",
        "--balancing_authority",
        dest="do_bas",
        type=bool,
        default=False,
        help="Compile geometries for balancing authority territories."
    )
    parser.add_argument(
        "-y",
        "--years",
        dest="years",
        type=tuple,
        default=pc.working_years["eia861"],
        help="Years of data to include when compiling geometries."
    )
    parser.add_argument(
        "-s",
        "--state_priority",
        dest="state_priority",
        type=bool,
        default=pc.working_years["eia861"],
        help="Years of data to include when compiling geometries."
    )


def main():
    """Compile historical utility and balancing area territories."""
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)
    if args["do_utils"]:
        print("stuff")


if __name__ == "__main__":
    sys.exit(main())


################################################################################
# Obsolete functions temporarily retained for comparison purposes
################################################################################
def georef_planning_areas(ba_eia861,     # Balancing Area
                          st_eia861,     # Service Territory
                          sales_eia861,  # Sales
                          census_gdf,    # Census DP1
                          output_crs):
    """
    Georeference balancing authority and utility territories from EIA 861.

    Use data from the EIA 861 balancing authority, service territory, and sales tables,
    compile a list of counties (and county FIPS IDs) associated with each balancing
    authority for each year, as well as for any utilities which don't appear to be
    associated with any balancing authority. Then associate a county-level geometry from
    the US Census DP1 dataset with each record, based on the county FIPS ID. This
    (enormous) GeoDataFrame can then be used to produce simpler annual geometries by
    dissolving based on either balancing authority or utility IDs and the report date.

    The way that the relationship between balancing authorities and utilities is
    reported changed between 2012 and 2013. Prior to 2013, the EIA 861 balancing
    authority table enumerates all of the utilities which participate in each balancing
    authority. In 2013 and subsequent years, the balancing authority table associates a
    balancing authority code (e.g. SWPP or ERCO) with each balancing authority ID, and
    also lists which states the balancing authority was operating in. These balancing
    authority codes then appear in other EIA 861 tables like the Sales table, in
    association with utilities and often states. For these later years, we must compile
    the list of utility IDs which are seen in association with a particular balancing
    authority code to understand which utilities are operating within which balancing
    authorities, and thus which counties should be included in that authority's
    territory. Because the state is also listed, we can select only a subset of the
    counties that are part of the utility, providing much more geographic specificity.
    This is especially important in the case of sprawling western utilities like
    PacifiCorp, which drastically expand the apparent territory of a balancing authority
    if the utility's entire service territory is included just because the sold
    electricty within one small portion of the balancing authority's territory.

    Args:
        ba_eia861 (pandas.DataFrame): The balancing_authority_eia861 table.
        st_eia861 (pandas.DataFrame): The service_territory_eia861 table.
        sales_eia861 (pandas.DataFrame): The sales_eia861 table.
        census_gdf (geopandas.GeoDataFrame): The counties layer of the US Census DP1
            geospatial dataset.
        output_crs (str): String representing a coordinate reference system (CRS) that
            is recognized by geopandas. Applied to the output GeoDataFrame.

    Returns:
        geopandas.GeoDataFrame: Contains columns identifying the balancing authority,
        utility, and state, along with the county geometry, for each year in which
        those balancing authorities / utilities appeared in the EIA 861 Balancing
        Authority table (through 2012) or the EIA 861 Sales table (for 2013 onward).

    """
    # Which utilities were part of what balancing areas in 2010-2012?
    early_ba_by_util = (
        ba_eia861
        .query("report_date <= '2012-12-31'")
        .loc[:, [
            "report_date",
            "balancing_authority_id_eia",
            "balancing_authority_code_eia",
            "utility_id_eia",
            "balancing_authority_name_eia",
        ]]
        .drop_duplicates(
            subset=["report_date", "balancing_authority_id_eia", "utility_id_eia"])
    )

    # Create a dataframe that associates utilities and balancing authorities.
    # This information is directly avaialble in the early_ba_by_util dataframe
    # but has to be compiled for 2013 and later years based on the utility
    # BA associations that show up in the Sales table
    # Create an annual, normalized version of the BA table:
    ba_normed = (
        ba_eia861
        .loc[:, [
            "report_date",
            "state",
            "balancing_authority_code_eia",
            "balancing_authority_id_eia",
            "balancing_authority_name_eia",
        ]]
        .drop_duplicates(subset=[
            "report_date",
            "state",
            "balancing_authority_code_eia",
            "balancing_authority_id_eia",
        ])
    )
    ba_by_util = (
        pd.merge(
            ba_normed,
            sales_eia861
            .loc[:, [
                "report_date",
                "state",
                "utility_id_eia",
                "balancing_authority_code_eia"
            ]].drop_duplicates()
        )
        .loc[:, [
            "report_date",
            "state",
            "utility_id_eia",
            "balancing_authority_id_eia"
        ]]
        .append(early_ba_by_util[["report_date", "utility_id_eia", "balancing_authority_id_eia"]])
        .drop_duplicates()
        .merge(ba_normed)
        .dropna(subset=["report_date", "utility_id_eia", "balancing_authority_id_eia"])
        .sort_values(
            ["report_date", "balancing_authority_id_eia", "utility_id_eia", "state"])
    )
    # Merge in county FIPS IDs for each county served by the utility from
    # the service territory dataframe. We do an outer merge here so that we
    # retain any utilities that are not part of a balancing authority. This
    # lets us generate both BA and Util maps from the same GeoDataFrame
    # We have to do this separately for the data up to 2012 (which doesn't
    # include state) and the 2013 and onward data (which we need to have
    # state for)
    early_ba_util_county = (
        ba_by_util.drop("state", axis="columns")
        .merge(st_eia861, on=["report_date", "utility_id_eia"], how="outer")
        .query("report_date <= '2012-12-31'")
    )
    late_ba_util_county = (
        ba_by_util
        .merge(st_eia861, on=["report_date", "utility_id_eia", "state"], how="outer")
        .query("report_date >= '2013-01-01'")
    )
    ba_util_county = pd.concat([early_ba_util_county, late_ba_util_county])
    # Bring in county geometry information based on FIPS ID from Census
    ba_util_county_gdf = (
        census_gdf[["GEOID10", "NAMELSAD10", "geometry"]]
        .to_crs(output_crs)
        .rename(
            columns={
                "GEOID10": "county_id_fips",
                "NAMELSAD10": "county_name_census",
            }
        )
        .merge(ba_util_county)
    )

    return ba_util_county_gdf
