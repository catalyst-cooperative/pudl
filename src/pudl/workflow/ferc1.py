"""Implements pipeline for processing FERC1 dataset."""
import prefect
from prefect import task

import pudl
from pudl import dfc
from pudl.dfc import DataFrameCollection
from pudl.metadata.dfs import FERC_ACCOUNTS, FERC_DEPRECIATION_LINES
from pudl.workflow.dataset_pipeline import DatasetPipeline


@task()
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

    df['ferc_accounts'] = FERC_ACCOUNTS[[
        "ferc_account_id",
        "ferc_account_description",
    ]]

    df['ferc_depreciation_lines'] = FERC_DEPRECIATION_LINES[[
        "line_id",
        "ferc_account_description",
    ]]
    return df


@task()
def _extract_ferc1(pipeline_settings, pudl_settings):
    return DataFrameCollection(
        **pudl.extract.ferc1.extract(
            ferc1_tables=pipeline_settings.tables,
            ferc1_years=pipeline_settings.years,
            pudl_settings=pudl_settings))


@task()
def _transform_ferc1(pipeline_settings, dfs):
    return DataFrameCollection(
        **pudl.transform.ferc1.transform(dfs, ferc1_tables=pipeline_settings.tables))


class Ferc1Pipeline(DatasetPipeline):
    """Runs ferc1 tasks."""

    DATASET = 'ferc1'

    def __init__(self, *args, **kwargs):
        """Initializes ferc1 pipeline, optionally creates ferc1 sqlite database."""
        super().__init__(*args, **kwargs)

    def build(self):
        """Add ferc1 tasks to the flow."""
        with self.flow:
            # ferc1_to_sqlite task should only happen once.
            # Only add this task to the flow if it is not already present.
            ferc1_to_sqlite_settings = prefect.context.get(
                "etl_settings").ferc1_to_sqlite_settings
            overwrite_ferc1_db = prefect.context.get("overwrite_ferc1_db")
            pudl_settings = prefect.context.get("pudl_settings")

            if not self.flow.get_tasks(name='ferc1_to_sqlite'):
                pudl.extract.ferc1.ferc1_to_sqlite(
                    ferc1_to_sqlite_settings,
                    pudl_settings,
                    overwrite=overwrite_ferc1_db)
            raw_dfs = _extract_ferc1(self.pipeline_settings, pudl_settings,
                                     upstream_tasks=self.flow.get_tasks(name='ferc1_to_sqlite'))
            dfs = _transform_ferc1(self.pipeline_settings, raw_dfs)
            return dfc.merge(load_static_tables_ferc1(), dfs)
