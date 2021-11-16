"""Defines generic API for DatasetPipeline.

Each dataset should make subclass for this and add prefect tasks to the flow
in its build() method.
"""
import abc
from abc import ABC

from pudl.settings import GenericDatasetSettings


class DatasetPipeline(ABC):
    """This object encapsulates the logic for processing pudl dataset.

    When instantiated, it will extract the relevant dataset parameters,
    determine if any tasks need to be run and attaches relevant prefect
    tasks to the provided flow.

    When implementing subclasses of this, you should:
    - implement build() method
    - implement a dataset settings class to validate requested partitions.
    - implement a dataset name.
    """

    def __init__(self, flow, pipeline_settings):
        """Initialize Pipeline object and construct prefect tasks.

        Args:
            pudl_settings (dict): overall configuration (paths and such)
            flow (prefect.Flow): attach prefect tasks to this flow
        """
        self.flow = flow
        self.pipeline_settings = pipeline_settings
        self.output_dfc = self.build()

    @property
    @abc.abstractmethod
    def dataset(cls) -> str:    # noqa: N805
        """Abstract dataset property."""
        return cls.dataset

    @property
    @abc.abstractmethod
    def settings(cls) -> GenericDatasetSettings:    # noqa: N805
        """Abstract settings property."""
        return cls.settings

    def build(self):
        """Add pipeline tasks to the flow.

        Args:
          etl_params: parameters for this pipeline (returned by self.validate_params)
        """
        raise NotImplementedError(
            f'{self.dataset}: Please implement pipeline build method.')

    def outputs(self):
        """Returns prefect.Result containing DataFrameCollection."""
        return self.output_dfc

    @classmethod
    def get_pipeline_for_dataset(cls, dataset):
        """Returns subclass of DatasetPipeline associated with the given dataset."""
        for subclass in cls.__subclasses__():
            if subclass.dataset == dataset:
                return subclass
        return None
