"""Implements pipeline for processing FERC1 dataset."""
from prefect import task

import pudl
from pudl import constants as pc
from pudl import dfc
from pudl.dfc import DataFrameCollection
from pudl.extract.ferc1 import SqliteOverwriteMode
from pudl.workflow.dataset_pipeline import DatasetPipeline


@task(target="ferc1.static-tables")
def load_static_tables_ferc1():
    """Populate static PUDL tables with constants for use as foreign keys.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin. For now these values are primarily stored in a large collection of
    lists, dictionaries, and dataframes which are specified in the
    pudl.constants module.  This function uses those data structures to
    populate a bunch of small infrastructural tables within the PUDL DB.
    """
    # create dfs for tables with static data from constants.
    df = DataFrameCollection()

    df['ferc_accounts'] = (
        pc.ferc_electric_plant_accounts
        .drop('row_number', axis=1)
        .replace({'ferc_account_description': r'\s+'}, ' ', regex=True)
        .rename(columns={'ferc_account_description': 'description'})
    )

    df['ferc_depreciation_lines'] = (
        pc.ferc_accumulated_depreciation
        .drop('row_number', axis=1)
        .rename(columns={'ferc_account_description': 'description'})
    )
    return df


@task(target="ferc1.extract")
def _extract_ferc1(params, pudl_settings):
    return DataFrameCollection(
        **pudl.extract.ferc1.extract(
            ferc1_tables=params['ferc1_tables'],
            ferc1_years=params['ferc1_years'],
            pudl_settings=pudl_settings))


@task(target="ferc1.transform")
def _transform_ferc1(params, dfs):
    return DataFrameCollection(
        **pudl.transform.ferc1.transform(dfs, ferc1_tables=params['ferc1_tables']))


class Ferc1Pipeline(DatasetPipeline):
    """Runs ferc1 tasks."""

    DATASET = 'ferc1'

    def __init__(self, *args, overwrite_ferc1_db=SqliteOverwriteMode.ALWAYS, **kwargs):
        """Initializes ferc1 pipeline, optionally creates ferc1 sqlite database."""
        self.overwrite_ferc1_db = overwrite_ferc1_db
        super().__init__(*args, **kwargs)

    @classmethod  # noqa: C901
    def validate_params(cls, etl_params):
        """Validate and normalize ferc1 parameters."""
        ferc1_dict = {
            'ferc1_years': etl_params.get('ferc1_years', [None]),
            'ferc1_tables': etl_params.get('ferc1_tables', pc.pudl_tables['ferc1']),
            'debug': etl_params.get('debug', False),
        }
        if not ferc1_dict['debug']:
            cls.check_for_bad_tables(
                try_tables=ferc1_dict['ferc1_tables'], dataset='ferc1')

        if not ferc1_dict['ferc1_years']:
            # TODO(rousik): this really does not make much sense? We should be skipping the pipeline
            # when ferc1_years assumes false value so why do we need to clear the parameters dict
            # here???
            # Perhaps this is due to the [None] hack above that may span all years? Who knows.
            return {}
        else:
            return ferc1_dict

    def build(self, params):
        """Add ferc1 tasks to the flow."""
        if not self.all_params_present(params, ['ferc1_years', 'ferc1_tables']):
            return None
        with self.flow:
            # ferc1_to_sqlite task should only happen once.
            # Only add this task to the flow if it is not already present.
            if not self.flow.get_tasks(name='ferc1_to_sqlite'):
                pudl.extract.ferc1.ferc1_to_sqlite(
                    self.etl_settings,
                    self.pudl_settings,
                    overwrite=self.overwrite_ferc1_db)
            raw_dfs = _extract_ferc1(params, self.pudl_settings,
                                     upstream_tasks=self.flow.get_tasks(name='ferc1_to_sqlite'))
            dfs = _transform_ferc1(params, raw_dfs)
            return dfc.merge(load_static_tables_ferc1(), dfs)
