"""Helper functions for creating output assets."""
import importlib

from dagster import AssetsDefinition, asset


def sql_asset_factory(
    name: str,
    non_argument_deps: set[str] = {},
    io_manager_key: str = "pudl_sqlite_io_manager",
    compute_kind: str = "SQL",
) -> AssetsDefinition:
    """Factory for creating assets that run SQL statements."""

    @asset(
        name=name,
        non_argument_deps=non_argument_deps,
        io_manager_key=io_manager_key,
        compute_kind=compute_kind,
    )
    def sql_view_asset() -> str:
        """Asset that creates sql view in a database."""
        sql_path_traversable = importlib.resources.files("pudl.output.sql").joinpath(
            f"{name}.sql"
        )
        try:
            with importlib.resources.as_file(sql_path_traversable) as sql_path:
                with open(sql_path) as sql_file:
                    return sql_file.read()
        # Raise a helpful error here if a sql file doesn't exist
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Could not find {sql_path}. Create a sql file in pudl.output.sql subpackage for {name} asset."
            )

    return sql_view_asset
