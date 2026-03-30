"""CLI for compiling historical utility and balancing area service territory geometries."""

import pathlib
import sys
from typing import Literal

import click


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
def main(
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
    # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
    import pudl  # noqa: PLC0415
    from pudl.analysis.service_territory import compile_geoms  # noqa: PLC0415
    from pudl.helpers import get_parquet_table  # noqa: PLC0415

    pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)

    _ = compile_geoms(
        core_eia861__yearly_balancing_authority=get_parquet_table(
            "core_eia861__yearly_balancing_authority"
        ),
        core_eia861__assn_balancing_authority=get_parquet_table(
            "core_eia861__assn_balancing_authority"
        ),
        out_eia__yearly_utilities=get_parquet_table("out_eia__yearly_utilities"),
        core_eia861__yearly_service_territory=get_parquet_table(
            "core_eia861__yearly_service_territory"
        ),
        core_eia861__assn_utility=get_parquet_table("core_eia861__assn_utility"),
        census_counties=get_parquet_table(
            "out_censusdp1tract__counties",
            columns=["county_id_fips", "geometry", "county", "dp0010001"],
        ),
        dissolve=dissolve,
        save_format="geoparquet",
        output_dir=output_dir,
        entity_type=entity_type,
        limit_by_state=limit_by_state,
        years=years,
    )


if __name__ == "__main__":
    sys.exit(main())
