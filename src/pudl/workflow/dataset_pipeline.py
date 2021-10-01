"""Defines generic API for DatasetPipeline.

Each dataset should make subclass for this and add prefect tasks to the flow
in its build() method.
"""
from pudl import constants as pc


class DatasetPipeline:
    """This object encapsulates the logic for processing pudl dataset.

    When instantiated, it will extract the relevant dataset parameters,
    determine if any tasks need to be run and attaches relevant prefect
    tasks to the provided flow.

    When implementing subclasses of this, you should:
    - set DATASET class attribute
    - implement validate_params() method
    - implement build() method
    """

    DATASET = None

    def __init__(self, pudl_settings, dataset_list, flow, etl_settings=None,
                 clobber=False):
        """Initialize Pipeline object and construct prefect tasks.

        Args:
            pudl_settings (dict): overall configuration (paths and such)
            dataset_list (list): list of named datasets associated with this bundle
            flow (prefect.Flow): attach prefect tasks to this flow
            datapkg_name (str): fully qualified name of the datapackage/bundle
            etl_settings (dict): the complete ETL configuration
            clobber (bool): if True, then existing outputs will be clobbered
        """
        if not self.DATASET:
            raise NotImplementedError(
                f'{self.__cls__.__name__}: missing DATASET attribute')
        self.flow = flow
        self.pipeline_params = None
        self.pudl_settings = pudl_settings
        self.datapkg_name = etl_settings["datapkg_bundle_settings"]["name"]
        self.output_dfc = None
        self.pipeline_params = self._get_dataset_params(dataset_list)
        self.etl_settings = etl_settings
        self.clobber = clobber
        if self.pipeline_params:
            self.pipeline_params = self.validate_params(self.pipeline_params)
            self.output_dfc = self.build(self.pipeline_params)

    def _get_dataset_params(self, dataset_list):
        """Return params that match self.DATASET.

        Args:
          dataset_list: list of dataset parameters to search for the matching params
          structure
        """
        matching_ds = []
        for ds in dataset_list:
            if self.DATASET in ds:
                matching_ds.append(ds[self.DATASET])
        if not matching_ds:
            return None
        if len(matching_ds) > 1:
            raise AssertionError(
                f'Non-unique settings found for dataset {self.DATASET}')
        return matching_ds[0]

    @classmethod
    def validate_params(cls, datapkg_settings):
        """Validates pipeline parameters/settings.

        Returns:
          (dict) normalized datapkg parameters
        """
        raise NotImplementedError('Please implement pipeline validate_settings method')

    def build(self, etl_params):
        """Add pipeline tasks to the flow.

        Args:
          etl_params: parameters for this pipeline (returned by self.validate_params)
        """
        raise NotImplementedError(
            f'{self.__name__}: Please implement pipeline build method.')

    def outputs(self):
        """Returns prefect.Result containing DataFrameCollection."""
        return self.output_dfc

    def is_executed(self):
        """Returns true if the pipeline is executed."""
        return bool(self.pipeline_params)

    @classmethod
    def get_pipeline_for_dataset(cls, dataset):
        """Returns subclass of DatasetPipeline associated with the given dataset."""
        for subclass in cls.__subclasses__():
            if subclass.DATASET == dataset:
                return subclass
        return None

    @staticmethod
    def all_params_present(params, required_params):
        """Returns true iff all params in required_params are defined and nonempty."""
        return all(params.get(p) for p in required_params)

    @staticmethod
    def check_for_bad_years(try_years, dataset):
        """Check for bad data years."""
        bad_years = [
            y for y in try_years
            if y not in pc.working_partitions[dataset]['years']]
        if bad_years:
            raise AssertionError(f"Unrecognized {dataset} years: {bad_years}")

    @staticmethod
    def check_for_bad_tables(try_tables, dataset):
        """Check for bad data tables."""
        bad_tables = [t for t in try_tables if t not in pc.pudl_tables[dataset]]
        if bad_tables:
            raise AssertionError(f"Unrecognized {dataset} table: {bad_tables}")
