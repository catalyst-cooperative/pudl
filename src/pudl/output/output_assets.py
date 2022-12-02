"""Output table assets."""
from dagster import asset


# TODO (bendnorman): We could also save the SQL code in sql files so we can
# take advantage of syntax highlighting and formatting. We could programatically create
# the view assets give a structure that contains the asset name and non_argument_deps.
@asset(
    non_argument_deps={"utilities_entity_eia", "utilities_eia860", "utilities_eia"},
    io_manager_key="pudl_sqlite_io_manager",
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


@asset(
    io_manager_key="pudl_sqlite_io_manager",
)
def utility_analysis(utils_eia860):
    """Example of how to create an analysis table that depends on an output view.

    This final dataframe will be written to the database (without a schema).
    """
    # Do some analysis on utils_eia860
    return utils_eia860
