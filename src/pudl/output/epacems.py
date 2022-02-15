"""Routines that provide user-friendly access to the partitioned EPA CEMS dataset."""

from itertools import product
from pathlib import Path
from typing import Optional, Sequence

import dask.dataframe as dd
import pandas as pd

import pudl
from pudl.settings import EpaCemsSettings

# TODO: hardcoded data version doesn't belong here, but will defer fixing it until
# the crosswalk is formally integrated into PUDL. See Issue #1123
EPA_CROSSWALK_RELEASE = "https://github.com/USEPA/camd-eia-crosswalk/releases/download/v0.2.1/"


def epa_crosswalk() -> pd.DataFrame:
    # TODO: formally integrate this into PUDL. See Issue #1123
    """Read EPA/EIA crosswalk from EPA github repo.

    See https://github.com/USEPA/camd-eia-crosswalk for details and data dictionary

    Returns:
        pd.Dataframe: EPA/EIA crosswalk
    """
    return pd.read_csv(EPA_CROSSWALK_RELEASE + "epa_eia_crosswalk.csv")


def year_state_filter(years=(), states=()):
    """
    Create filters to read given years and states from partitioned parquet dataset.

    A subset of an Apache Parquet dataset can be read in more efficiently if files
    which don't need to be queried are avoideed. Some datasets are partitioned based
    on the values of columns to make this easier. The EPA CEMS dataset which we
    publish is partitioned by state and report year.

    However, the way the filters are specified can be unintuitive. They use DNF
    (disjunctive normal form) See this blog post for more details:

    https://blog.datasyndrome.com/python-and-parquet-performance-e71da65269ce

    This function takes a set of years, and a set of states, and returns a list of lists
    of tuples, appropriate for use with the read_parquet() methods of pandas and dask
    dataframes. The filter will include all combinations of the specified years and
    states. E.g. if years=(2018, 2019) and states=("CA", "CO") then the filter would
    result in getting 2018 and 2019 data for CO, as well as 2018 and 2019 data for CA.

    Args:
        years (iterable): 4-digit integers indicating the years of data you would like
            to read. By default it includes all years.
        states (iterable): 2-letter state abbreviations indicating what states you would
            like to include. By default it includes all states.

    Returns:
        list: A list of lists of tuples, suitable for use as a filter in the
        read_parquet method of pandas and dask dataframes.

    """
    year_filters = [("year", "=", year) for year in years]
    state_filters = [("state", "=", state.upper()) for state in states]

    if states and not years:
        filters = [[tuple(x), ] for x in state_filters]
    elif years and not states:
        filters = [[tuple(x), ] for x in year_filters]
    elif years and states:
        filters = [list(x) for x in product(year_filters, state_filters)]
    else:
        filters = None

    return filters


def get_plant_states(plant_ids, pudl_out):
    """
    Determine what set of states a given set of EIA plant IDs are within.

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
        pudl_out.plants_eia860()
        .query("plant_id_eia in @plant_ids")
        .state.unique()
    )


def get_plant_years(plant_ids, pudl_out):
    """
    Determine which years a given set of EIA plant IDs appear in.

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
    states: Optional[Sequence[str]] = None,
    years: Optional[Sequence[int]] = None,
    columns: Optional[Sequence[str]] = None,
    epacems_path: Optional[Path] = None,
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
        epacems_path = Path(pudl_settings["parquet_dir"]) / "epacems"

    epacems = dd.read_parquet(
        epacems_path,
        use_nullable_dtypes=True,
        columns=columns,
        filters=year_state_filter(
            states=epacems_settings.states,
            years=epacems_settings.years,
        ),
    )

    return epacems
