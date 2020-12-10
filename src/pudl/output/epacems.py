"""Routines that provide user-friendly access to the partitioned EPA CEMS dataset."""

import itertools


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
        filters = [list(x) for x in itertools.product(year_filters, state_filters)]
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
