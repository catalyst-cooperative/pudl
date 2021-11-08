"""Implements pipeline for processing glue dataset.

This produces several static tables that can be used for correlate
data from FERC1 and EIA datasets.
"""
import pudl
from pudl.workflow.dataset_pipeline import DatasetPipeline


class GluePipeline(DatasetPipeline):
    """Runs glue tasks combining eia/ferc1 results."""

    # TODO(rousik): this is a lot of boilerplate for very little use. Perhaps refactor.
    DATASET = 'glue'

    def build(self):
        """Add glue tasks to the flow."""
        with self.flow:
            # This expects two named attributes ferc1, eia that have bool values
            eia = self.pipeline_settings.eia
            ferc1 = self.pipeline_settings.ferc1
            return pudl.glue.ferc1_eia.glue(eia=eia, ferc1=ferc1)
