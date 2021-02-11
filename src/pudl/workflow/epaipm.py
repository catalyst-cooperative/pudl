"""Implements pipeline for processing EPA IPM dataset."""
import pandas as pd

import pudl
from pudl import constants as pc
from pudl.dfc import DataFrameCollection
from pudl.workflow.dataset_pipeline import DatasetPipeline


def load_static_tables_epaipm() -> DataFrameCollection:
    """
    Populate static PUDL tables with constants for use as foreign keys.

    For IPM, there is only one list of regional id's stored in constants that
    we want to load as a tabular resource because many of the other tabular
    resources in IPM rely on the regional_id_epaipm as a foreign key.

    Args:
        datapkg_dir (path-like): The location of the directory for this
            package. The data package directory will be a subdirectory in the
            `datapkg_dir` directory, with the name of the package as the
            name of the subdirectory.

    Returns:
        list: names of tables which were loaded.

    """
    # compile the dfs in a dictionary, prep for dict_dump
    return DataFrameCollection(
        regions_entity_epaipm=pd.DataFrame(
            pc.epaipm_region_names, columns=['region_id_epaipm']))


class EpaIpmPipeline(DatasetPipeline):
    """Runs epaipm tasks."""

    DATASET = 'epaipm'

    @staticmethod
    def validate_params(etl_params):
        """Validate and normalize epaipm parameters."""
        epaipm_dict = {}
        # pull out the etl_params from the dictionary passed into this function
        try:
            epaipm_dict['epaipm_tables'] = etl_params['epaipm_tables']
        except KeyError:
            epaipm_dict['epaipm_tables'] = []
        if not epaipm_dict['epaipm_tables']:
            return {}
        return epaipm_dict

    def build(self, params):
        """Add epaipm tasks to the flow."""
        if not self.all_params_present(params, ['epaipm_tables']):
            return None
        with self.flow:
            # TODO(rousik): annotate epaipm extract/transform methods with @task decorators
            tables = params["epaipm_tables"]
            dfc = pudl.extract.epaipm.extract(tables)
            dfc = pudl.transform.epaipm.transform(dfc, tables)
            return dfc.merge(
                load_static_tables_epaipm(),
                dfc)
