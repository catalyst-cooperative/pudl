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

    @staticmethod
    def validate_params(etl_params):
        """
        Validates and normalizes glue parameters.

        This effectively creates dict with ferc1 and eia entries that determine
        whether eia and ferc1 components of the glue process should be retrieved.
        """
        # Create dict that indicates whether ferc1, eia records are in the etl_params
        glue_params = {p: bool(etl_params.get(p, False)) for p in ['ferc1', 'eia']}
        if any(glue_params.values()):
            return glue_params
        return {}

    def build(self, params):
        """Add glue tasks to the flow."""
        with self.flow:
            # This expects two named attributes ferc1, eia that have bool values
            return pudl.glue.ferc1_eia.glue(**params)
