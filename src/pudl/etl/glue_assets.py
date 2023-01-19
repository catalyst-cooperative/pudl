"""FERC and EIA glue assets."""
from dagster import AssetOut, Output, multi_asset

import pudl
from pudl.metadata.classes import Package

logger = pudl.logging_helpers.get_logger(__name__)


# TODO (bendnorman): Currently loading all glue tables. Could potentially allow users
# to load subsets of the glue tables, see: https://docs.dagster.io/concepts/assets/multi-assets#subsetting-multi-assets
# Could split out different types of glue tables into different assets. For example the cross walk table could be a separate asset
# that way dagster doesn't think all glue tables depend on generators_entity_eia, boilers_entity_eia.


@multi_asset(
    outs={
        table_name: AssetOut(io_manager_key="pudl_sqlite_io_manager")
        for table_name in Package.get_etl_group_tables("glue")
    },
    required_resource_keys={"datastore", "dataset_settings"},
)
def create_glue_tables(context, generators_entity_eia, boilers_entity_eia):
    """Extract, transform and load CSVs for the Glue tables.

    Args:
        glue_settings: Validated ETL parameters required by this data source.
        ds_kwargs: Keyword arguments for instantiating a PUDL datastore, so that the ETL
            can access the raw input data.
        sqlite_dfs: The dictionary of dataframes to be loaded into the pudl database.
            We pass the dictionary though because the EPACAMD-EIA crosswalk needs to
            know which EIA plants and generators are being loaded into the database
            (based on whether we run the full or fast etl). The tests will break if we
            pass the generators_entity_eia table as an argument because of the
            ferc1_solo test (where no eia tables are in the sqlite_dfs dict). Passing
            the whole dict avoids this because the crosswalk will only load if there
            are eia tables in the dict, but the dict will always be there.
        eia_settings: Validated ETL parameters required by this data source.

    Returns:
        A dictionary of DataFrames whose keys are the names of the corresponding
        database table.
    """
    dataset_settings = context.resources.dataset_settings
    # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(
        ferc1=dataset_settings.glue.ferc1,
        eia=dataset_settings.glue.eia,
    )

    # Add the EPA to EIA crosswalk, but only if the eia data is being processed.
    # Otherwise the foreign key references will have nothing to point at:
    ds = context.resources.datastore
    if dataset_settings.glue.eia:
        # Check to see whether the settings file indicates the processing of all
        # available EIA years.
        processing_all_eia_years = (
            dataset_settings.eia.eia860.years
            == dataset_settings.eia.eia860.data_source.working_partitions["years"]
        )
        glue_raw_dfs = pudl.glue.epacamd_eia.extract(ds)
        glue_transformed_dfs = pudl.glue.epacamd_eia.transform(
            glue_raw_dfs,
            generators_entity_eia,
            boilers_entity_eia,
            processing_all_eia_years,
        )
        glue_dfs.update(glue_transformed_dfs)

    # Ensure they are sorted so they match up with the asset outs
    glue_dfs = dict(sorted(glue_dfs.items()))

    return (
        Output(output_name=table_name, value=df) for table_name, df in glue_dfs.items()
    )
