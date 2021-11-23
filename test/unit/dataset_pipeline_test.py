"""Test dataset pipeline."""

import prefect
import pytest

from pudl.settings import EiaSettings
from pudl.workflow.dataset_pipeline import DatasetPipeline


class TestDatasetPipeline:
    """Test DatasetPipeline behavior."""

    def test_abstract_property_error(self):
        """Test DatasetPipeline forces you to dataset name and settings."""
        flow = prefect.Flow("test")
        settings = EiaSettings()
        with pytest.raises(TypeError, match="Can't instantiate abstract class EiaPipeline with abstract methods dataset, settings"):
            class EiaPipeline(DatasetPipeline):
                pass
            EiaPipeline(flow, settings)

    def test_not_implemented_error(self):
        """Test DatasetPipeline forces you to dataset name and settings."""
        flow = prefect.Flow("test")
        settings = EiaSettings()
        with pytest.raises(NotImplementedError):
            class EiaPipeline(DatasetPipeline):
                dataset = "eia"
                settings = EiaSettings

            EiaPipeline(flow, settings)
