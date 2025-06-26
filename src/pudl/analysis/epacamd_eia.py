"""Helper functions for filtering the EPA CAMD crosswalk table.

This filtering was originally designed to filter the crosswalk before making a
``subplant_id`` so that the only ``subplant_id`` s that are generated are for records
that show up in EPA CAMD.

Usage Example:

.. code-block:: python

   epacems = pudl.output.epacems.epacems(states=['ID'], years=[2020])
   filtered_crosswalk = filter_crosswalk(core_epa__assn_eia_epacamd, epacems)
   crosswalk_with_subplant_ids = pudl.etl.make_subplant_ids(filtered_crosswalk)
"""

import dask.dataframe as dd
import pandas as pd


def _get_unique_keys(epacems: pd.DataFrame | dd.DataFrame) -> pd.DataFrame:
    """Get unique unit IDs from EPA CEMS data.

    Args:
        epacems: epacems dataset from :func:`pudl.output.epacems.epacems`

    Returns:
        Unique keys from the epacems dataset.
    """
    # The purpose of this function is mostly to resolve the
    # ambiguity between dask and pandas dataframes
    ids = epacems[["plant_id_eia", "emissions_unit_id_epa"]].drop_duplicates()
    if isinstance(epacems, dd.DataFrame):
        ids = ids.compute()
    return ids


def filter_crosswalk_by_epacems(
    crosswalk: pd.DataFrame, epacems: pd.DataFrame | dd.DataFrame
) -> pd.DataFrame:
    """Inner join unique CEMS units with :ref:`core_epa__assn_eia_epacamd` crosswalk.

    This is essentially an empirical filter on EPA units. Instead of filtering by
    construction/retirement dates in the crosswalk (thus assuming they are accurate),
    use the presence/absence of CEMS data to filter the units.

    Args:
        crosswalk: :ref:`core_epa__assn_eia_epacamd` crosswalk table.
        unique_epacems_ids: unique ids from :func:`_get_unique_keys`

    Returns:
        The inner join of the :ref:`core_epa__assn_eia_epacamd` crosswalk and unique
        epacems units. Adds the global ID column ``unit_id_epa``.
    """
    unique_epacems_ids = _get_unique_keys(epacems)
    key_map = unique_epacems_ids.merge(
        crosswalk,
        on=["plant_id_eia", "emissions_unit_id_epa"],
        how="inner",
    )
    return key_map


def filter_out_boiler_rows(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that represent graph edges between generators and boilers.

    Args:
        crosswalk: The :ref:`core_epa__assn_eia_epacamd` crosswalk table.

    Returns:
        The :ref:`core_epa__assn_eia_epacamd` crosswalk with boiler rows
        (many/one-to-many) removed.
    """
    crosswalk = crosswalk.drop_duplicates(
        subset=["plant_id_eia", "emissions_unit_id_epa", "generator_id"]
    )
    return crosswalk


def filter_crosswalk(
    crosswalk: pd.DataFrame, epacems: pd.DataFrame | dd.DataFrame
) -> pd.DataFrame:
    """Remove unmapped crosswalk rows or duplicates due to m2m boiler relationships.

    Args:
        crosswalk: The :ref:`core_epa__assn_eia_epacamd` crosswalk table.
        epacems: Emissions data. Must contain columns named
            ``["plant_id_eia", "emissions_unit_id_epa"]``

    Returns:
        A filtered copy of :ref:`core_epa__assn_eia_epacamd` crosswalk.
    """
    filtered_crosswalk = filter_out_boiler_rows(crosswalk)
    key_map = filter_crosswalk_by_epacems(filtered_crosswalk, epacems)
    return key_map
