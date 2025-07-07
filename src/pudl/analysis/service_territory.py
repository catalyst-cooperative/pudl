"""Compile historical utility and balancing area territories.

Use the mapping of utilities to counties, and balancing areas to utilities, available
within the EIA 861, in conjunction with the US Census geometries for counties, to infer
the historical spatial extent of utility and balancing area territories. Output the
resulting geometries for use in other applications.
"""

import math
import pathlib
import sys
from collections.abc import Iterable
from typing import Literal

import click
import geopandas as gpd
import pandas as pd
import sqlalchemy as sa
from dagster import AssetsDefinition, Field, asset
from matplotlib import pyplot as plt

import pudl
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

################################################################################
# Coordinate Reference Systems used in different contexts
################################################################################
MAP_CRS = "EPSG:3857"  # For mapping w/ OSM baselayer tiles
CALC_CRS = "ESRI:102003"  # For accurate area calculations


def utility_ids_all_eia(
    out_eia__yearly_utilities: pd.DataFrame,
    core_eia861__yearly_service_territory: pd.DataFrame,
) -> pd.DataFrame:
    """Compile IDs and Names of all known EIA Utilities.

    Grab all EIA utility names and IDs from both the EIA 861 Service Territory table and
    the EIA 860 Utility entity table. This is a temporary function that's only needed
    because we haven't integrated the EIA 861 information into the entity harvesting
    process and PUDL database yet.

    Args:
        out_eia__yearly_utilities: De-normalized EIA 860 utility attributes table.
        core_eia861__yearly_service_territory: Normalized EIA 861 Service Territory table.

    Returns:
        A DataFrame having 2 columns ``utility_id_eia`` and ``utility_name_eia``.
    """
    return (
        pd.concat(
            [
                out_eia__yearly_utilities[["utility_id_eia", "utility_name_eia"]],
                core_eia861__yearly_service_territory[
                    ["utility_id_eia", "utility_name_eia"]
                ],
            ]
        )
        .dropna(subset=["utility_id_eia"])
        .drop_duplicates(subset=["utility_id_eia"])
    )


################################################################################
# Functions that compile geometries based on EIA 861 data tables:
################################################################################
def get_territory_fips(
    ids: Iterable[int],
    assn: pd.DataFrame,
    assn_col: str,
    core_eia861__yearly_service_territory: pd.DataFrame,
    limit_by_state: bool = True,
) -> pd.DataFrame:
    """Compile county FIPS codes associated with an entity's service territory.

    For each entity identified by ids, look up the set of counties associated
    with that entity on an annual basis. Optionally limit the set of counties
    to those within states where the selected entities reported activity elsewhere
    within the EIA 861 data.

    Args:
        ids: A collection of EIA utility or balancing authority IDs.
        assn: Association table, relating ``report_date``,
        ``state``, and ``utility_id_eia`` to each other, as well as the
            column indicated by ``assn_col`` -- if it's not ``utility_id_eia``.
        assn_col: Label of the dataframe column in ``assn`` that contains
            the ID of the entities of interest. Should probably be either
            ``balancing_authority_id_eia`` or ``utility_id_eia``.
        core_eia861__yearly_service_territory: The EIA 861 Service Territory table.
        limit_by_state: Whether to require that the counties associated
            with the balancing authority are inside a state that has also been
            seen in association with the balancing authority and the utility
            whose service territory contains the county.

    Returns:
        A table associating the entity IDs with a collection of
        counties annually, identifying counties both by name and county_id_fips
        (both state and state_id_fips are included for clarity).
    """
    # Limit the association table to only the relevant values:
    assn = assn.loc[assn[assn_col].isin(ids)]

    if not limit_by_state:
        assn = assn.drop("state", axis="columns")

    return (
        pd.merge(assn, core_eia861__yearly_service_territory, how="inner")
        .loc[
            :,
            [
                "report_date",
                assn_col,
                "state",
                "county",
                "state_id_fips",
                "county_id_fips",
            ],
        ]
        .drop_duplicates()
    )


def add_geometries(
    df: pd.DataFrame,
    census_gdf: gpd.GeoDataFrame,
    dissolve: bool = False,
    dissolve_by: list[str] = None,
) -> gpd.GeoDataFrame:
    """Merge census geometries into dataframe on county_id_fips, optionally dissolving.

    Merge the US Census county-level geospatial information into the DataFrame df
    based on the the column county_id_fips (in df), which corresponds to the column
    GEOID10 in census_gdf. Also bring in the population and area of the counties,
    summing as necessary in the case of dissolved geometries.

    Args:
        df: A DataFrame containing a county_id_fips column.
        census_gdf (geopandas.GeoDataFrame): A GeoDataFrame based on the US Census
            demographic profile (DP1) data at county resolution, with the original
            column names as published by US Census.
        dissolve: If True, dissolve individual county geometries into larger
            service territories.
        dissolve_by: The columns to group by in the dissolve. For example,
            dissolve_by=["report_date", "utility_id_eia"] might provide annual utility
            service territories, while ["report_date", "balancing_authority_id_eia"]
            would provide annual balancing authority territories.

    Returns:
        geopandas.GeoDataFrame
    """
    out_gdf = (
        census_gdf[["geoid10", "namelsad10", "dp0010001", "geometry"]]
        .rename(
            columns={
                "geoid10": "county_id_fips",
                "namelsad10": "county_name_census",
                "dp0010001": "population",
            }
        )
        # Calculate county areas using cylindrical equal area projection:
        .assign(area_km2=lambda x: x.geometry.to_crs(epsg=6933).area / 1e6)
        .merge(df, how="right")
    )
    if dissolve is True:
        # Don't double-count duplicated counties, if any.
        out_gdf = out_gdf.drop_duplicates(
            subset=dissolve_by
            + [
                "county_id_fips",
            ]
        )
        # Sum these numerical columns so we can merge with dissolved geometries
        summed = (
            out_gdf.groupby(dissolve_by)[["population", "area_km2"]].sum().reset_index()
        )
        out_gdf = (
            out_gdf.dissolve(by=dissolve_by)
            .drop(
                [
                    "county_id_fips",
                    "county",
                    "county_name_census",
                    "state",
                    "state_id_fips",
                    "population",
                    "area_km2",
                ],
                axis="columns",
            )
            .reset_index()
            .merge(summed)
        )
    return out_gdf


def get_territory_geometries(
    ids: Iterable[int],
    assn: pd.DataFrame,
    assn_col: str,
    core_eia861__yearly_service_territory: pd.DataFrame,
    census_gdf: gpd.GeoDataFrame,
    limit_by_state: bool = True,
    dissolve: bool = False,
) -> gpd.GeoDataFrame:
    """Compile service territory geometries based on county_id_fips.

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
        ids: A collection of EIA balancing authority IDs.
        assn: Association table, relating ``report_date``,
        ``state``, and ``utility_id_eia`` to each other, as well as the
            column indicated by ``assn_col`` -- if it's not ``utility_id_eia``.
        assn_col: Label of the dataframe column in ``assn`` that contains
            the ID of the entities of interest. Should probably be either
            ``balancing_authority_id_eia`` or ``utility_id_eia``.
        core_eia861__yearly_service_territory: The EIA 861 Service Territory table.
        census_gdf: The US Census DP1 county-level geometries.
        limit_by_state: Whether to require that the counties associated
            with the balancing authority are inside a state that has also been
            seen in association with the balancing authority and the utility
            whose service territory contains the county.
        dissolve: If False, each record in the compiled territory will correspond
            to a single county, with a county-level geometry, and there will be many
            records enumerating all the counties associated with a given
            balancing_authority_id_eia in each year. If dissolve=True, all of the
            county-level geometries for each utility in each year will be merged
            together ("dissolved") resulting in a single geometry and record for each
            balancing_authority-year.

    Returns:
        A GeoDataFrame with service territory geometries for each entity.
    """
    return get_territory_fips(
        ids=ids,
        assn=assn,
        assn_col=assn_col,
        core_eia861__yearly_service_territory=core_eia861__yearly_service_territory,
        limit_by_state=limit_by_state,
    ).pipe(
        add_geometries,
        census_gdf,
        dissolve=dissolve,
        dissolve_by=["report_date", assn_col],
    )


def _save_geoparquet(
    gdf: gpd.GeoDataFrame,
    entity_type: Literal["utility", "balancing_authority"],
    dissolve: bool,
    limit_by_state: bool,
    output_dir: pathlib.Path | None = None,
) -> None:
    """Save utility or balancing authority service territory geometries to GeoParquet.

    In order to prevent the geometry data from exceeding the 2GB maximum size of an
    Arrow object, we need to keep the row groups small. Sort the dataframe by the
    primary key columns to minimize the number of values in any row group.  Output
    filename is constructed based on input arguments.

    Args:
        gdf: GeoDataframe containing utility or balancing authority geometries.
        entity_type: string indicating whether we're outputting utility or balancing
            authority geometries.
        dissolve: Whether the individual county geometries making up the service
            territories have been merged together. Used to construct filename.
        limit_by_state: Whether service territories have been limited to include only
            counties in states where the utilities reported sales. Used to construct
            filename.
        output_dir: Path to the directory where the GeoParquet file will be written.

    """
    dissolved = "_dissolved" if dissolve else ""
    limited = "_limited" if limit_by_state else ""
    if output_dir is None:
        output_dir = pathlib.Path.cwd()
    file_path = output_dir / f"{entity_type}_geometry{limited}{dissolved}.parquet"
    gdf.sort_values(["report_date", f"{entity_type}_id_eia"]).to_parquet(
        file_path, row_group_size=512, compression="snappy", index=False
    )


def compile_geoms(
    core_eia861__yearly_balancing_authority: pd.DataFrame,
    core_eia861__assn_balancing_authority: pd.DataFrame,
    out_eia__yearly_utilities: pd.DataFrame,
    core_eia861__yearly_service_territory: pd.DataFrame,
    core_eia861__assn_utility: pd.DataFrame,
    census_counties: pd.DataFrame,
    entity_type: Literal["balancing_authority", "utility"],
    save_format: Literal["geoparquet", "geodataframe", "dataframe"],
    output_dir: pathlib.Path | None = None,
    dissolve: bool = False,
    limit_by_state: bool = True,
    years: list[int] = [],
) -> pd.DataFrame:
    """Compile all available utility or balancing authority geometries.

    Returns a geoparquet file, geopandas GeoDataFrame or a pandas DataFrame with the
    geometry column removed depending on the value of the save_format parameter. By
    default, this returns only counties with observed EIA 861 data for a utility or
    balancing authority, with geometries available at the county level.
    """
    logger.info(
        f"Compiling {entity_type} geometries with {dissolve=}, {limit_by_state=}, "
        f"and {years=}."
    )
    if save_format == "geoparquet" and output_dir is None:
        raise ValueError("No output_dir provided while writing geoparquet.")

    if years:

        def _limit_years(df: pd.DataFrame) -> pd.DataFrame:
            return df[df.report_date.dt.year.isin(years)]

        core_eia861__yearly_balancing_authority = _limit_years(
            core_eia861__yearly_balancing_authority
        )
        core_eia861__assn_balancing_authority = _limit_years(
            core_eia861__assn_balancing_authority
        )
        out_eia__yearly_utilities = _limit_years(out_eia__yearly_utilities)
        core_eia861__yearly_service_territory = _limit_years(
            core_eia861__yearly_service_territory
        )
        core_eia861__assn_utility = _limit_years(core_eia861__assn_utility)

    utilids_all_eia = utility_ids_all_eia(
        out_eia__yearly_utilities, core_eia861__yearly_service_territory
    )

    if entity_type == "balancing_authority":
        ids = (
            core_eia861__yearly_balancing_authority.balancing_authority_id_eia.unique()
        )
        assn = core_eia861__assn_balancing_authority
        assn_col = "balancing_authority_id_eia"
    elif entity_type == "utility":
        ids = utilids_all_eia.utility_id_eia.unique()
        assn = core_eia861__assn_utility
        assn_col = "utility_id_eia"
    else:
        raise ValueError(
            f"Got {entity_type=}, but need either 'balancing_authority' or 'utility'"
        )

    # Identify all Utility IDs with service territory information
    geom = get_territory_geometries(
        ids=ids,
        assn=assn,
        assn_col=assn_col,
        core_eia861__yearly_service_territory=core_eia861__yearly_service_territory,
        census_gdf=census_counties,
        limit_by_state=limit_by_state,
        dissolve=dissolve,
    )
    if save_format == "geoparquet":
        # TODO[dagster]: update to use IO Manager.
        _save_geoparquet(
            geom,
            entity_type=entity_type,
            dissolve=dissolve,
            limit_by_state=limit_by_state,
            output_dir=output_dir,
        )
    elif save_format == "dataframe":
        geom = pd.DataFrame(geom.drop(columns="geometry"))

    return geom


def service_territory_asset_factory(
    entity_type: Literal["balancing_authority", "utility"],
    io_manager_key: str | None = None,
) -> list[AssetsDefinition]:
    """Build asset definitions for balancing authority and utility territories."""

    @asset(
        name=f"out_eia861__yearly_{entity_type}_service_territory",
        io_manager_key=io_manager_key,
        config_schema={
            "dissolve": Field(
                bool,
                default_value=False,
                description=(
                    "If True, dissolve the compiled geometries to the entity level. If False, leave them as counties."
                ),
            ),
            "limit_by_state": Field(
                bool,
                default_value=True,
                description=(
                    "If True, only include counties with observed EIA 861 data in association with the state and utility/balancing authority."
                ),
            ),
            "save_format": Field(
                str,
                default_value="dataframe",
                description=(
                    "Format of output in PUDL. One of: geoparquet, geodataframe, dataframe."
                ),
            ),
        },
        compute_kind="Python",
    )
    def _service_territory(
        context,
        core_eia861__yearly_balancing_authority: pd.DataFrame,
        core_eia861__assn_balancing_authority: pd.DataFrame,
        out_eia__yearly_utilities: pd.DataFrame,
        core_eia861__yearly_service_territory: pd.DataFrame,
        core_eia861__assn_utility: pd.DataFrame,
        _core_censusdp1tract__counties: pd.DataFrame,
    ) -> pd.DataFrame:
        """Compile all available utility or balancing authority geometries.

        Returns:
            A dataframe compiling all available utility or balancing authority geometries.
        """
        # Get options from dagster
        dissolve = context.op_config["dissolve"]
        limit_by_state = context.op_config["limit_by_state"]
        save_format = context.op_config["save_format"]

        return compile_geoms(
            core_eia861__yearly_balancing_authority=core_eia861__yearly_balancing_authority,
            core_eia861__assn_balancing_authority=core_eia861__assn_balancing_authority,
            out_eia__yearly_utilities=out_eia__yearly_utilities,
            core_eia861__yearly_service_territory=core_eia861__yearly_service_territory,
            core_eia861__assn_utility=core_eia861__assn_utility,
            census_counties=_core_censusdp1tract__counties,
            entity_type=entity_type,
            dissolve=dissolve,
            limit_by_state=limit_by_state,
            save_format=save_format,
        )

    return _service_territory


service_territory_eia861_assets = [
    service_territory_asset_factory(
        entity_type=entity, io_manager_key="pudl_io_manager"
    )
    for entity in ["balancing_authority", "utility"]
]


################################################################################
# Functions for visualizing the service territory geometries
################################################################################
def plot_historical_territory(
    gdf: gpd.GeoDataFrame,
    id_col: str,
    id_val: str | int,
) -> None:
    """Plot all the historical geometries defined for the specified entity.

    This is useful for exploring how a particular entity's service territory has evolved
    over time, or for identifying individual missing or inaccurate territories.

    Args:
        gdf: A geodataframe containing geometries pertaining electricity planning areas.
            Can be broken down by county FIPS code, or have a single record containing a
            geometry for each combination of report_date and the column being used to
            select planning areas (see below).
        id_col: The label of a column in gdf that identifies the planning area to be
            visualized, like ``utility_id_eia``, ``balancing_authority_id_eia``, or
            ``balancing_authority_code_eia``.
        id_val: The ID of the entity whose territory should be plotted.
    """
    if id_col not in gdf.columns:
        raise ValueError(f"The input id_col {id_col} doesn't exist in this GDF.")
    logger.info("Plotting historical territories for %s==%s.", id_col, id_val)

    # Pare down the GDF so this all goes faster
    entity_gdf = gdf[gdf[id_col] == id_val]
    if "county_id_fips" in entity_gdf.columns:
        entity_gdf = entity_gdf.drop_duplicates(
            subset=["report_date", "county_id_fips"]
        )
    entity_gdf["report_year"] = entity_gdf.report_date.dt.year
    logger.info(
        "Plotting service territories of %s %s records.", len(entity_gdf), id_col
    )

    # Create a grid of subplots sufficient to hold all the years:
    years = entity_gdf.report_year.sort_values().unique()
    ncols = 5
    nrows = math.ceil(len(years) / ncols)
    fig, axes = plt.subplots(
        ncols=ncols,
        nrows=nrows,
        figsize=(15, 3 * nrows),
        sharex=True,
        sharey=True,
        facecolor="white",
    )
    fig.suptitle(f"{id_col} == {id_val}")

    for year, ax in zip(years, axes.flat, strict=True):
        ax.set_title(f"{year}")
        ax.set_xticks([])
        ax.set_yticks([])
        year_gdf = entity_gdf.loc[entity_gdf.report_year == year]
        year_gdf.plot(ax=ax, linewidth=0.1)
    plt.show()


def plot_all_territories(
    gdf: gpd.GeoDataFrame,
    report_date: str,
    respondent_type: str | Iterable[str] = ("balancing_authority", "utility"),
    color: str = "black",
    alpha: float = 0.25,
):
    """Plot all of the planning areas of a given type for a given report date.

    Todo:
        This function needs to be made more general purpose, and less
        entangled with the FERC 714 data.

    Args:
        gdf: GeoDataFrame containing planning area geometries, organized by
            ``respondent_id_ferc714`` and ``report_date``.
        report_date: A string representing a datetime that indicates what year's
            planning areas should be displayed.
        respondent_type: Type of respondent whose planning
            areas should be displayed. Either "utility" or
            "balancing_authority" or an iterable collection containing both.
        color: Color to use for the planning areas.
        alpha: Transparency to use for the planning areas.

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
    if isinstance(respondent_type, str):
        respondent_type = (respondent_type,)

    plot_gdf = (
        gdf.query("report_date==@report_date")
        .query("respondent_id_ferc714 not in @unwanted_respondent_ids")
        .query("respondent_type in @respondent_type")
    )
    ax = plot_gdf.plot(figsize=(20, 20), color=color, alpha=alpha, linewidth=1)
    plt.title(f"FERC 714 {', '.join(respondent_type)} planning areas for {report_date}")
    plt.show()
    return ax


################################################################################
# Provide a CLI for generating service territories
################################################################################
@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--entity-type",
    type=click.Choice(["utility", "balancing_authority"]),
    default="util",
    show_default=True,
    help="What kind of entity's service territories should be generated?",
)
@click.option(
    "--limit-by-state/--no-limit-by-state",
    default=False,
    help=(
        "Limit service territories to including only counties located in states where "
        "the utility or balancing authority also reported electricity sales in EIA-861 "
        "in the year that the geometry pertains to. In theory a utility could serve a "
        "county, but not sell any electricity there, but that seems like an unusual "
        "situation."
    ),
    show_default=True,
)
@click.option(
    "--year",
    "-y",
    "years",
    type=click.IntRange(min=2001),
    default=[],
    multiple=True,
    help=(
        "Limit service territories generated to those from the given year. This can "
        "dramatically reduce the memory and CPU intensity of the geospatial "
        "operations. Especially useful for testing. Option can be used multiple times "
        "toselect multiple years."
    ),
)
@click.option(
    "--dissolve/--no-dissolve",
    default=True,
    help=(
        "Dissolve county level geometries to the utility or balancing authority "
        "boundaries. The dissolve operation may take several minutes and is quite "
        "memory intensive, but results in significantly smaller files, in which each "
        "record contains the whole geometry of a utility or balancing authority. The "
        "un-dissolved geometries use many records to describe each service territory, "
        "with each record containing the geometry of a single constituent county."
    ),
    show_default=True,
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(
        exists=True,
        writable=True,
        dir_okay=True,
        file_okay=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
    default=pathlib.Path.cwd(),
    show_default=True,
    help=(
        "Path to the directory where the service territory geometries should be saved. "
        "Defaults to the current working directory. Filenames are constructed based on "
        "the other flags provided."
    ),
)
@click.option(
    "--logfile",
    help="If specified, write logs to this file.",
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.option(
    "--loglevel",
    default="INFO",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    show_default=True,
)
def pudl_service_territories(
    entity_type: Literal["utility", "balancing_authority"],
    dissolve: bool,
    output_dir: pathlib.Path,
    limit_by_state: bool,
    years: list[int],
    logfile: pathlib.Path,
    loglevel: str,
):
    """Compile historical utility and balancing area service territory geometries.

    This script produces GeoParquet files describing the historical service territories
    of utilities and balancing authorities based on data reported in the EIA Form 861
    and county geometries from the US Census DP1 geodatabase.

    See: https://geoparquet.org/ for more on the GeoParquet file format.

    Usage examples:

    pudl_service_territories --entity-type balancing_authority --dissolve --limit-by-state
    pudl_service_territories --entity-type utility
    """
    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)

    pudl_engine = sa.create_engine(PudlPaths().pudl_db)
    # Load the required US Census DP1 county geometry data:
    dp1_engine = PudlPaths().sqlite_db_uri("censusdp1tract")
    sql = """
SELECT
    geoid10,
    namelsad10,
    dp0010001,
    shape AS geometry
FROM
    county_2010census_dp1;
"""
    county_gdf = gpd.read_postgis(
        sql,
        con=dp1_engine,
        geom_col="geometry",
        crs="EPSG:4326",
    )

    _ = compile_geoms(
        core_eia861__yearly_balancing_authority=pd.read_sql(
            "core_eia861__yearly_balancing_authority",
            pudl_engine,
        ),
        core_eia861__assn_balancing_authority=pd.read_sql(
            "core_eia861__assn_balancing_authority",
            pudl_engine,
        ),
        out_eia__yearly_utilities=pd.read_sql("out_eia__yearly_utilities", pudl_engine),
        core_eia861__yearly_service_territory=pd.read_sql(
            "core_eia861__yearly_service_territory", pudl_engine
        ),
        core_eia861__assn_utility=pd.read_sql("core_eia861__assn_utility", pudl_engine),
        census_counties=county_gdf,
        dissolve=dissolve,
        save_format="geoparquet",
        output_dir=output_dir,
        entity_type=entity_type,
        limit_by_state=limit_by_state,
        years=years,
    )


if __name__ == "__main__":
    sys.exit(pudl_service_territories())
