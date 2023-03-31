"""Routines that provide user-friendly access to the partitioned EPA CEMS dataset."""
from collections.abc import Iterable, Sequence
from itertools import product
from pathlib import Path

import dask.dataframe as dd

import pudl
from pudl.settings import EpaCemsSettings


def year_state_filter(
    years: Iterable[int] = None, states: Iterable[str] = None
) -> list[list[tuple[str | int]]]:
    """Create filters to read given years and states from partitioned parquet dataset.

    A subset of an Apache Parquet dataset can be read in more efficiently if files which
    don't need to be queried are avoideed. Some datasets are partitioned based on the
    values of columns to make this easier. The EPA CEMS dataset which we publish is
    partitioned by state and report year.

    However, the way the filters are specified can be unintuitive. They use DNF
    (disjunctive normal form) See this blog post for more details:

    https://blog.datasyndrome.com/python-and-parquet-performance-e71da65269ce

    This function takes a set of years, and a set of states, and returns a list of lists
    of tuples, appropriate for use with the read_parquet() methods of pandas and dask
    dataframes. The filter will include all combinations of the specified years and
    states. E.g. if years=(2018, 2019) and states=("CA", "CO") then the filter would
    result in getting 2018 and 2019 data for CO, as well as 2018 and 2019 data for CA.

    Args:
        years: 4-digit integers indicating the years of data you would like
            to read. By default it includes all available years.
        states: 2-letter state abbreviations indicating what states you would
            like to include. By default it includes all available states.

    Returns:
        A list of lists of tuples, suitable for use as a filter in the
        read_parquet() method of pandas and dask dataframes.
    """
    if years is not None:
        year_filters = [("year", "=", year) for year in years]
    if states is not None:
        state_filters = [("state", "=", state.upper()) for state in states]

    if states and not years:
        filters = [[tuple(x)] for x in state_filters]
    elif years and not states:
        filters = [[tuple(x)] for x in year_filters]
    elif years and states:
        filters = [list(x) for x in product(year_filters, state_filters)]
    else:
        filters = None

    return filters


def get_plant_states(plant_ids, pudl_out):
    """Determine what set of states a given set of EIA plant IDs are within.

    If you only want to select data about a particular set of power plants from the EPA
    CEMS data, this is useful for identifying which patitions of the Parquet dataset
    you will need to search.

    Args:
        plant_ids (iterable): A collection of integers representing valid plant_id_eia
            values within the PUDL DB.
        pudl_out (pudl.output.pudltabl.PudlTabl): A PudlTabl output object to use to
            access the PUDL DB.

    Returns:
        list: A list containing the 2-letter state abbreviations for any state that was
        found in association with one or more of the plant_ids.
    """
    return list(
        pudl_out.plants_eia860().query("plant_id_eia in @plant_ids").state.unique()
    )


def get_plant_years(plant_ids, pudl_out):
    """Determine which years a given set of EIA plant IDs appear in.

    If you only want to select data about a particular set of power plants from the EPA
    CEMS data, this is useful for identifying which patitions of the Parquet dataset
    you will need to search.

    NOTE: the EIA-860 and EIA-923 data which are used here don't cover as many years as
    the EPA CEMS, so this is probably of limited utility -- you may want to simply
    include all years, or manually specify the years of interest instead.

    Args:
        plant_ids (iterable): A collection of integers representing valid plant_id_eia
            values within the PUDL DB.
        pudl_out (pudl.output.pudltabl.PudlTabl): A PudlTabl output object to use to
            access the PUDL DB.

    Returns:
        list: A list containing the 4-digit integer years found in association with one
        or more of the plant_ids.
    """
    return list(
        pudl_out.plants_eia860()
        .query("plant_id_eia in @plant_ids")
        .report_date.dt.year.unique()
    )


def epacems(
    states: Sequence[str] | None = None,
    years: Sequence[int] | None = None,
    columns: Sequence[str] | None = None,
    epacems_path: Path | None = None,
) -> dd.DataFrame:
    """Load EPA CEMS data from PUDL with optional subsetting.

    Args:
        states: subset by state abbreviation.  Defaults to None (which gets all states).
        years: subset by year. Defaults to None (which gets all years).
        columns: subset by column. Defaults to None (which gets all columns).
        epacems_path: path to parquet dir. By default it automatically loads the path
            from :mod:`pudl.workspace`

    Returns:
        The requested epacems data
    """
    epacems_settings = EpaCemsSettings(states=states, years=years)

    # columns=None is handled by dd.read_parquet; gives all columns
    if columns is not None:
        # nonexistent columns are handled by dd.read_parquet; raises ValueError
        columns = list(columns)

    if epacems_path is None:
        pudl_settings = pudl.workspace.setup.get_defaults()
        epacems_path = Path(pudl_settings["pudl_out"]) / "epacems"

    epacems = dd.read_parquet(
        epacems_path,
        use_nullable_dtypes=True,
        columns=columns,
        engine="pyarrow",
        index=False,
        split_row_groups=True,
        filters=year_state_filter(
            states=epacems_settings.states,
            years=epacems_settings.years,
        ),
    )

    return epacems
