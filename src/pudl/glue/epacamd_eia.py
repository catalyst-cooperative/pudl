"""Extract, clean, and normalize the EPACAMD-EIA crosswalk.

This module defines functions that read the raw EPACAMD-EIA crosswalk file and clean up
the column names.

The crosswalk file was a joint effort on behalf on EPA and EIA and is published on the
EPA's github account at www.github.com/USEPA/camd-eia-crosswalk". It's a work in
progress and worth noting that, at present, only pulls from 2018 data.
"""
import pandas as pd

import pudl.logging_helpers
from pudl.helpers import remove_leading_zeros_from_numeric_strings, simplify_columns
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


def extract(ds: Datastore) -> pd.DataFrame:
    """Extract the EPACAMD-EIA Crosswalk from the Datastore."""
    logger.info("Extracting the EPACAMD-EIA crosswalk from Zenodo")
    with ds.get_zipfile_resource("epacamd_eia", name="epacamd_eia.zip").open(
        "camd-eia-crosswalk-master/epa_eia_crosswalk.csv"
    ) as f:
        return pd.read_csv(f)


def transform(
    epacamd_eia: pd.DataFrame,
    generators_entity_eia: pd.DataFrame,
    boilers_entity_eia: pd.DataFrame,
    processing_all_eia_years: bool,
) -> dict[str, pd.DataFrame]:
    """Clean up the EPACAMD-EIA Crosswalk file.

    In its raw form, the crosswalk contains many fields. The transform process removes
    descriptive fields like state, location, facility name, capacity, operating status,
    and fuel type that can be found by linking this dataset to others already in the
    database. We're primarily concerned with linking ids in this table, not including
    any other plant information.

    The raw file contains several fields with the prefix ``MOD``. These fields are used
    to create a single ID for matches with different spelling. For example,
    ``EIA_PLANT_ID`` (``plant_id_eia`` in PUDL) has the generator ID ``CTG5`` in EPA and
    ``GT5`` in EIA. The ``MOD`` columns for both of these generators
    (``MOD_CAMD_GENERATOR_ID``, ``MOD_EIA_GENERATOR_ID_GEN``) converts that value to 5.
    The ``MATCH_TYPE_GEN``, ``MATCH_TYPE_BOILER``, and ``PLANT_ID_CHANGE_FLAG`` fields
    indicate whether these ``MOD`` fields contain new information. Because we're not
    concerned with creating a new, modified ID field for either EPA or EIA data, we
    don't need these ``MOD`` or ``MATCH_TYPE`` columns in our final output. We just care
    which EPA value maps to which EIA value.

    In terms of cleaning, we implement the standard column name changes: lower-case, no
    special characters, and underscores instead of spaces. We also rename some of the
    columns for clarity and to match how they appear in the tables you will merge with.
    Besides standardizing datatypes (again for merge compatability) the only meaningful
    data alteration we employ here is removing leading zeros from numeric strings on
    the ``generator_id`` and ``emissions_unit_id_epa`` fields. This is because the same
    function is used to clean those same fields in all the other tables in which they
    appear. In order to merge properly, we need to clean the values in the crosswalk the
    same way. Lastly, we drop all rows without ``EIA_PLANT_ID`` (``plant_id_eia``)
    values because that means that they are unmatched and do not provide any useful
    information to users.

    It's important to note that the crosswalk is kept intact (and not seperated into
    smaller reference tables) because the relationship between the ids is not 1:1. For
    example, you can't isolate the plant_id fields, drop duplicates, and call it a day.
    The plant discrepancies depend on which generator ids it's referring to. This goes
    for all fields. Be careful, and do some due diligence before eliminating columns.

    We talk more about the complexities regarding EPA "units" in our :doc:`Data Source
    documentation page for EPACEMS </data_sources/epacems>`.

    It's also important to note that the crosswalk is a static file: there is no year
    field. The plant_id_eia and generator_id fields, however, are foreign keys from an
    annualized table. If the fast ETL is run (on one year of data) the test will break
    because the crosswalk tables with ``plant_id_eia`` and ``generator_id`` contain
    values from various years. To keep the crosswalk in alignment with the available eia
    data, we'll restrict it based on the generator entity table which has
    ``plant_id_eia`` and ``generator_id`` so long as it's not using the full suite of
    avilable years. If it is, we don't want to restrict the crosswalk so we can get
    warnings and errors from any foreign key discrepancies. This isn't an ideal
    solution, but it works for now.

    Args:
        epacamd_eia: The result of running this module's extract() function.
        generators_entity_eia: The generators_entity_eia table.
        boilers_entity_eia: The boilers_entitiy_eia table.
        processing_all_years: A boolean indicating whether the years from the
            Eia860Settings object match the EIA860 working partitions. This indicates
            whether or not to restrict the crosswalk data so the tests don't fail on
            foreign key restraints.

    Returns:
        A dictionary containing the cleaned EPACAMD-EIA crosswalk DataFrame.
    """
    logger.info("Transforming the EPACAMD-EIA crosswalk")

    column_rename = {
        "camd_plant_id": "plant_id_epa",
        "camd_unit_id": "emissions_unit_id_epa",
        "camd_generator_id": "generator_id_epa",
        "eia_plant_id": "plant_id_eia",
        "eia_boiler_id": "boiler_id",  # Eventually change to boiler_id_eia
        "eia_generator_id": "generator_id",  # Eventually change to generator_id_eia
    }

    # Basic column rename, selection, and dtype alignment.
    crosswalk_clean = (
        epacamd_eia.pipe(simplify_columns)
        .rename(columns=column_rename)
        .filter(list(column_rename.values()))
        .pipe(remove_leading_zeros_from_numeric_strings, col_name="generator_id")
        .pipe(remove_leading_zeros_from_numeric_strings, col_name="boiler_id")
        .pipe(
            remove_leading_zeros_from_numeric_strings, col_name="emissions_unit_id_epa"
        )
        .pipe(apply_pudl_dtypes, "eia")
        .dropna(subset=["plant_id_eia"])
    )

    # Restrict crosswalk for tests if running fast etl
    if not processing_all_eia_years:
        logger.info(
            "Selected subset of avilable EIA years--restricting EPACAMD-EIA Crosswalk \
            to chosen subset of EIA years"
        )
        crosswalk_clean = pd.merge(
            crosswalk_clean,
            generators_entity_eia[["plant_id_eia", "generator_id"]],
            on=["plant_id_eia", "generator_id"],
            how="inner",
        )
        crosswalk_clean = pd.merge(
            crosswalk_clean,
            boilers_entity_eia[["plant_id_eia", "boiler_id"]],
            on=["plant_id_eia", "boiler_id"],
            how="inner",
        )

    return {"epacamd_eia": crosswalk_clean}
