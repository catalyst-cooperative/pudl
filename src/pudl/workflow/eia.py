"""Implements pipeline for processing EIA dataset."""
import pandas as pd
import prefect
from prefect import task

import pudl
from pudl import constants as pc
from pudl import dfc
from pudl.dfc import DataFrameCollection
from pudl.metadata.labels import (ENERGY_SOURCES_EIA,
                                  FUEL_TRANSPORTATION_MODES_EIA,
                                  FUEL_TYPES_AER_EIA, PRIME_MOVERS_EIA)
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


@task
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

    DATASET = 'eia'

    @classmethod  # noqa: C901
    def validate_params(cls, etl_params):
        """Validate and normalize eia parameters."""
        # extract all of the etl_params for the EIA ETL function
        # empty dictionary to compile etl_params
        eia_input_dict = {
            'eia860_years': etl_params.get('eia860_years', []),
            'eia860_tables': etl_params.get('eia860_tables', pc.PUDL_TABLES['eia860']),

            'eia860_ytd': etl_params.get('eia860_ytd', False),

            'eia923_years': etl_params.get('eia923_years', []),
            'eia923_tables': etl_params.get('eia923_tables', pc.PUDL_TABLES['eia923']),
        }

        # if we are only extracting 860, we also need to pull in the
        # boiler_fuel_eia923 table. this is for harvesting and also for the boiler
        # generator association
        if not eia_input_dict['eia923_years'] and eia_input_dict['eia860_years']:
            eia_input_dict['eia923_years'] = eia_input_dict['eia860_years']
            eia_input_dict['eia923_tables'] = [
                'boiler_fuel_eia923', 'generation_eia923']

        # if someone is trying to generate 923 without 860... well that won't work
        # so we're forcing the same 860 years.
        if not eia_input_dict['eia860_years'] and eia_input_dict['eia923_years']:
            eia_input_dict['eia860_years'] = eia_input_dict['eia923_years']

        eia860m_year = pd.to_datetime(
            pc.WORKING_PARTITIONS['eia860m']['year_month']).year
        if (eia_input_dict['eia860_ytd']
                and (eia860m_year in eia_input_dict['eia860_years'])):
            raise AssertionError(
                "Attempting to integrate an eia860m year "
                f"({eia860m_year}) that is within the eia860 years: "
                f"{eia_input_dict['eia860_years']}. Consider switching eia860_ytd "
                "parameter to False."
            )
        cls.check_for_bad_tables(
            try_tables=eia_input_dict['eia923_tables'], dataset='eia923')
        cls.check_for_bad_tables(
            try_tables=eia_input_dict['eia860_tables'], dataset='eia860')
        cls.check_for_bad_years(
            try_years=eia_input_dict['eia860_years'], dataset='eia860')
        cls.check_for_bad_years(
            try_years=eia_input_dict['eia923_years'], dataset='eia923')
        return eia_input_dict

    def build(self, params):
        """Extract, transform and load CSVs for the EIA datasets.

        Returns:
            prefect.Result object that contains emitted table names.
        """
        if not (self.all_params_present(params, ['eia923_tables', 'eia923_years']) or
                self.all_params_present(params, ['eia860_tables', 'eia860_years'])):
            return None

        # TODO(rousik): task names are nice for human readable results but in the end, they
        # are probably not worth worrying about.
        eia860_extract = pudl.extract.eia860.Extractor(
            name='eia860.extract',
            target=f'{self.datapkg_name}/eia860.extract')
        eia860m_extract = pudl.extract.eia860m.Extractor(
            name='eia860m.extract',
            target=f'{self.datapkg_name}/eia860m.extract')
        eia923_extract = pudl.extract.eia923.Extractor(
            name='eia923.extract',
            target=f'{self.datapkg_name}/eia923.extract')
        with self.flow:
            eia860_df = eia860_extract(year=params['eia860_years'])
            if params['eia860_ytd']:
                m_df = eia860m_extract(
                    year_month=pc.WORKING_PARTITIONS['eia860m']['year_month'])
                eia860_df = merge_eia860m(
                    eia860_df, m_df,
                    task_args=dict(target=f'{self.datapkg_name}/eia860m.merge'))
            eia860_df = pudl.transform.eia860.transform_eia860(
                eia860_df,
                params['eia860_tables'],
                task_args=dict(target=f'{self.datapkg_name}/eia860.transform'))

            eia923_df = pudl.transform.eia923.transform_eia923(
                eia923_extract(year=params['eia923_years']),
                params['eia923_tables'],
                task_args=dict(target=f'{self.datapkg_name}/eia923.transform'))

            output_tables = pudl.transform.eia.transform(
                dfc.merge_list(
                    [eia860_df, eia923_df, pudl.glue.eia_epacems.grab_clean_split()]),
                eia860_years=params['eia860_years'],
                eia923_years=params['eia923_years'],
                eia860_ytd=params['eia860_ytd'])

            return dfc.merge(
                read_static_tables_eia(),
                output_tables,
                task_args=dict(target=f'{self.datapkg_name}/eia.final_tables'))

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
