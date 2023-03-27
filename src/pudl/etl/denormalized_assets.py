"""Denormalized tables, based on other simple normalized tables.

Right now these will mostly be tables defined in `pudl.output`. Ultimately it would
also include any tables built by joining and aggregating normalized and analysis
tables. These are probably data warehouse style tables with a lot of duplicated
information, ready for analysts to use.

Initially we'll probably just wrap existing output functions, but these tables should
also be amenable to construction using SQL, database views, dbt, or other similar tools.
"""
from dagster import asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


# TODO (bendnorman): We could also save the SQL code in sql files so we can
# take advantage of syntax highlighting and formatting. We could programatically create
# the view assets with a structure that contains the asset name and non_argument_deps.
@asset(
    non_argument_deps={"utilities_entity_eia", "utilities_eia860", "utilities_eia"},
    io_manager_key="pudl_sqlite_io_manager",
    compute_kind="SQL",
)
def utils_eia860() -> str:
    """Create view of all fields from the EIA860 Utilities table."""
    query = """
    CREATE VIEW utils_eia860 AS
    SELECT *
    FROM (
        SELECT *
        FROM utilities_entity_eia
            LEFT JOIN utilities_eia860 USING (utility_id_eia)
    )
    LEFT JOIN (
        SELECT utility_id_eia,
            utility_id_pudl
        FROM utilities_eia
    ) USING (utility_id_eia);"""
    return query
