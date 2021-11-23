"""Implements pipeline for processing EIA dataset."""
import pandas as pd
import prefect
from prefect import task

import pudl
from pudl import dfc, settings
from pudl.dfc import DataFrameCollection
from pudl.metadata.codes import (CONTRACT_TYPES_EIA, ENERGY_SOURCES_EIA,
                                 FUEL_TRANSPORTATION_MODES_EIA,
                                 FUEL_TYPES_AER_EIA, PRIME_MOVERS_EIA,
                                 SECTOR_CONSOLIDATED_EIA)
from pudl.workflow.dataset_pipeline import DatasetPipeline


@task(target="eia.static-tables")
def read_static_tables_eia() -> DataFrameCollection:
    """Build dataframes of static EIA tables for use as foreign key constraints.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin. For now these values are primarily stored in a large collection of
    lists, dictionaries, and dataframes which are specified in the
    :mod:`pudl.constants` module.
    """
    return DataFrameCollection(
        energy_sources_eia=ENERGY_SOURCES_EIA["df"],
        fuel_types_aer_eia=FUEL_TYPES_AER_EIA["df"],
        prime_movers_eia=PRIME_MOVERS_EIA["df"],
        sector_consolidated_eia=SECTOR_CONSOLIDATED_EIA["df"],
        fuel_transportation_modes_eia=FUEL_TRANSPORTATION_MODES_EIA["df"],
        contract_types_eia=CONTRACT_TYPES_EIA["df"]
    )

    return DataFrameCollection(
        energy_sources_eia=pd.DataFrame(
            columns=["abbr", "energy_source"],
            data=ENERGY_SOURCES_EIA.items(),
        ),
        fuel_types_aer_eia=pd.DataFrame(
            columns=["abbr", "fuel_type"],
            data=FUEL_TYPES_AER_EIA.items(),
        ),
        prime_movers_eia=pd.DataFrame(
            columns=["abbr", "prime_mover"],
            data=PRIME_MOVERS_EIA.items(),
        ),
        fuel_transportation_modes_eia=pd.DataFrame(
            columns=["abbr", "fuel_transportation_mode"],
            data=FUEL_TRANSPORTATION_MODES_EIA.items(),
        )
    )


@task(target='eia860m.merge')
def merge_eia860m(eia860: DataFrameCollection, eia860m: DataFrameCollection):
    """Combines overlapping eia860 and eia860m data frames."""
    logger = prefect.context.get("logger")
    eia860m_dfs = eia860m.to_dict()
    result = DataFrameCollection()

    for table_name, table_id in eia860.references():
        if table_name not in eia860m_dfs:
            result.add_reference(table_name, table_id)
        else:
            logger.info(
                f'Extending eia860 table {table_name} with montly data from eia860m.')
            df = eia860.get(table_name)
            df = df.append(eia860m_dfs[table_name], ignore_index=True, sort=True)
            result.store(table_name, df)
    # To be really safe, check that there are no eia860m tables that are not present in eia860.
    known_eia860_tables = set(eia860.get_table_names())
    for table_name in list(eia860m_dfs):
        if table_name not in known_eia860_tables:
            logger.error(f'eia860m table {table_name} is not present in eia860')
    return result


class EiaPipeline(DatasetPipeline):
    """Runs eia923, eia860 and eia (entity extraction) tasks."""

    dataset = 'eia'
    settings = settings.EiaSettings

    def build(self):
        """Extract, transform and load CSVs for the EIA datasets.

        Returns:
            prefect.Result object that contains emitted table names.
        """
        eia860_settings = self.pipeline_settings.eia860
        eia923_settings = self.pipeline_settings.eia923

        eia860_extract = pudl.extract.eia860.Extractor(
            name='eia860.extract',
            target='eia860.extract')
        eia860m_extract = pudl.extract.eia860m.Extractor(
            name='eia860m.extract',
            target='eia860m.extract')
        eia923_extract = pudl.extract.eia923.Extractor(
            name='eia923.extract',
            target='eia923.extract')
        with self.flow:
            eia860_df = eia860_extract(year=eia860_settings.years)
            if eia860_settings.eia860m:
                m_df = eia860m_extract(
                    year_month=eia860_settings.eia860m_date)
                eia860_df = merge_eia860m(
                    eia860_df, m_df,
                    task_args=dict(target='eia860m.merge'))
            eia860_df = pudl.transform.eia860.transform_eia860(
                eia860_df,
                eia860_settings.tables,
                task_args=dict(target='eia860.transform'))

            eia923_df = pudl.transform.eia923.transform_eia923(
                eia923_extract(year=eia923_settings.years),
                eia923_settings.tables,
                task_args=dict(target='eia923.transform'))

            output_tables = pudl.transform.eia.transform(
                dfc.merge_list(
                    [eia860_df, eia923_df, pudl.glue.eia_epacems.grab_clean_split()]),
                eia860_years=eia860_settings.years,
                eia923_years=eia923_settings.years,
                eia860m=eia860_settings.eia860m_date)

            return dfc.merge(
                read_static_tables_eia(),
                output_tables,
                task_args=dict(target='eia.final_tables'))

    def get_table(self, table_name):
        """Returns DataFrame for given table that is emitted by the pipeline.

        This is intended to be used to link pipeline outputs to other prefect tasks.
        It does not actually return the pandas.DataFrame directly but rather creates
        prefect task that scans through the results of this pipeline run and extracts
        the table of interest.

        Args:
          table_name: Name of the table to be returned.

        Returns:
          (prefect.Task) task which returns pandas.DataFrame for the requested table.
        """
        # Loads csv form
        # TODO(rousik): once we break down tasks to table-level granularity, this
        # extraction task/functionality may no longer be needed.
        return dfc.extract_table.bind(self.output_dfc, table_name, flow=self.flow)
