import pathlib
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from dagster import (
    AssetsDefinition,
    Field,
    GraphOut,
    IOManager,
    Out,
    Output,
    asset,
    define_asset_job,
    fs_io_manager,
    graph,
    io_manager,
    op,
    repository,
    with_resources,
)


class SQLiteIOManager(IOManager):
    """IO manager that stores and retrieves values from a sqlite database..

    Args:
        output_dir (Optional[str]): base directory where all the step outputs which use this object
            manager will be stored in.
    """

    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir

    def handle_output(self, context, obj):
        """Write dataframe to a sqlite database."""
        if context.has_asset_key:
            # TODO: raise a more helpful exception or use a different dagster function to get the table name.
            table_name = context.get_asset_identifier()[0]
        else:
            table_name = context.get_identifier()

        filepath = self.output_dir / "pudl.sqlite"
        filepath.touch()

        engine = sa.create_engine(f"sqlite:///{filepath}")
        print(table_name, obj)
        with engine.connect() as con:
            # TODO: Delete data from existing schema and append the new data
            obj.to_sql(table_name, con, if_exists="replace", index=False)

    def load_input(self, context):
        # upstream_output.name is the name given to the Out that we're loading for
        if context.has_asset_key:
            # TODO: raise a more helpful exception or use a different dagster function to get the table name.
            table_name = context.get_asset_identifier()[0]
        else:
            table_name = context.get_identifier()

        filepath = self.output_dir / "pudl.sqlite"

        engine = sa.create_engine(f"sqlite:///{filepath}")
        with engine.connect() as con:
            return pd.read_sql_table(table_name, con)


@io_manager(
    config_schema={
        "output_dir": Field(
            str, default_value=str(pathlib.Path().absolute() / "output_dir")
        )
    }
)
def sqlite_io_manager(init_context):
    output_dir = Path(init_context.resource_config["output_dir"])
    output_dir.mkdir(exist_ok=True)
    return SQLiteIOManager(output_dir)


#############
## Toy FERC extraction
#############
@op(
    out={
        "raw_steam": Out(pd.DataFrame, io_manager_key="sqlite_io_manager"),
        "raw_fuel": Out(pd.DataFrame, io_manager_key="sqlite_io_manager"),
    }
)
def dbf2sqlite():
    return pd.DataFrame([1, 2, 3], columns=["data"]), pd.DataFrame(
        [1, 2, 3], columns=["data"]
    )


@graph(out={"raw_steam": GraphOut(), "raw_fuel": GraphOut()})
def etl():
    """Run pudl_etl job."""
    return dbf2sqlite()


#############
## Toy PUDL ETL
#############
@op(out=Out(io_manager_key="fs_io_manager"))
def _etl_ferc1(raw_steam) -> dict[str, pd.DataFrame]:
    return {"steam": pd.DataFrame([1, 2, 3], columns=["data"])}


@op(out=Out(io_manager_key="fs_io_manager"))
def _etl_eia() -> dict[str, pd.DataFrame]:
    return {"gens": pd.DataFrame([1, 2, 3], columns=["data"])}


@op(
    out={
        "gens": Out(pd.DataFrame, io_manager_key="sqlite_io_manager"),
        "steam": Out(pd.DataFrame, io_manager_key="sqlite_io_manager"),
    }
)
def _add_glue(eia_dfs, ferc1_dfs):
    dfs = eia_dfs | ferc1_dfs
    for table_name, value in dfs.items():
        yield Output(output_name=table_name, value=value)


@graph(out={"gens": GraphOut(), "steam": GraphOut()})
def etl(raw_steam):
    """Run pudl_etl job."""
    ferc1_dfs = _etl_ferc1(raw_steam)
    eia_dfs = _etl_eia()
    return _add_glue(eia_dfs, ferc1_dfs)


#############
## Toy CEMS
#############
@op(
    out={
        "hourly_emissions_epacems": Out(
            pd.DataFrame, io_manager_key="sqlite_io_manager"
        ),
    }
)
def process_cems(gens) -> pd.DataFrame:
    return pd.DataFrame([1, 2, 3], columns=["data"])


@graph(out={"hourly_emissions_epacems": GraphOut()})
def cems_etl(gens):
    """Run pudl_etl job."""
    return process_cems(gens)


#############
## Output Assets
#############
@asset(group_name="output_assets")
def steam_denormalized(steam: pd.DataFrame):
    return steam


@asset(group_name="output_assets")
def steam_denormalized_dependency(steam_denormalized: pd.DataFrame):
    return steam_denormalized


@asset(group_name="output_assets")
def steam_generators(gens: pd.DataFrame, steam: pd.DataFrame):
    return steam


@asset(group_name="output_assets")
def another_steam_generators(gens: pd.DataFrame, steam: pd.DataFrame):
    print("steam", steam)
    return steam


@asset(group_name="output_assets")
def asset_dependency(another_steam_generators: pd.DataFrame):
    print("another_plants_steam_fuel_ferc1", another_steam_generators)
    return another_steam_generators


@asset(group_name="output_assets")
def cems_output_table(hourly_emissions_epacems, steam_denormalized):
    return hourly_emissions_epacems


@repository
def my_repo():
    return [
        define_asset_job("pudl_etl"),
        AssetsDefinition.from_graph(
            etl,
            group_name="pudl_assets",
            resource_defs={
                "sqlite_io_manager": sqlite_io_manager,
                "fs_io_manager": fs_io_manager,
            },
        ),
        AssetsDefinition.from_graph(
            cems_etl,
            group_name="cems_assets",
            resource_defs={"sqlite_io_manager": sqlite_io_manager},
        ),
        AssetsDefinition.from_graph(
            dbf2sqlite,
            group_name="raw_ferc_assets",
            resource_defs={"sqlite_io_manager": sqlite_io_manager},
        ),
        *with_resources(
            [
                steam_denormalized,
                steam_generators,
                another_steam_generators,
                asset_dependency,
                cems_output_table,
                steam_denormalized_dependency,
            ],
            resource_defs={
                "io_manager": sqlite_io_manager,
            },
        ),
    ]
