"""A collection of denormalized FERC assets."""
from pudl.output.sql.helpers import sql_asset_factory

denorm_plants_utilities_ferc1_asset = sql_asset_factory(
    "denorm_plants_utilities_ferc1", {"plants_ferc1", "utilities_ferc1"}
)
