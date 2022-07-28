"""
To run this script: `python notebooks/work-in-progress/xbrl_dagster_prototypes/op_factory_option.py`

or to view in Dagit:
`dagit -f notebooks/work-in-progress/xbrl_dagster_prototypes/op_factory_option.py`
"""
import pandas as pd
from black import TRANSFORMED_MAGICS
from dagster import Field, GraphOut, Output, graph, in_process_executor, job, op
from sqlalchemy import column

#### Transform Metadata
"""
This can be a fat dictionary or pydantic metadata classes.
Something that returns transform parameters for a given table.
"""
TRANSFORM_METADATA = {
    "steam": {
        "xbrl": {"rename_columns": {"column_mapping": {"col": "columns"}}},
        "dbf": {"rename_columns": {"column_mapping": {"col": "columns"}}},
    },
    "fuel": {
        "xbrl": {"rename_columns": {"column_mapping": {"col": "columns"}}},
        "dbf": {"rename_columns": {"column_mapping": {"col": "columns"}}},
    },
    "hydro": {
        "xbrl": {"rename_columns": {"column_mapping": {"col": "columns"}}},
        "dbf": {"rename_columns": {"column_mapping": {"col": "columns"}}},
    },
}

#### Extract ops
@op
def extract_dbf():
    return {
        "steam": pd.DataFrame(columns=["col"]),
        "hydro": pd.DataFrame(columns=["col"]),
        "fuel": pd.DataFrame(columns=["col"]),
    }


@op
def extract_xbrl():
    return {
        "steam": pd.DataFrame(columns=["col"]),
        "hydro": pd.DataFrame(columns=["col"]),
        "fuel": pd.DataFrame(columns=["col"]),
    }


#### Helper functions
def get_df_from_dict_factory(table_name, source):
    """
    Return an op that gets a dataframe from a dictionary.

    This is required because the dictionary of dataframes we pass around
    is treated as a InputDefinition when passed to a graph.
    """

    @op(name=f"get_{table_name}_{source}")
    def get_df_from_dict(dfs):
        """Get a dataframe from a dictionary."""
        return dfs[table_name]

    return get_df_from_dict


#### Generic cleaning ops
def rename_columns_factory(
    column_mapping,
    name="default_name",
    ins=None,
    **kwargs,
):
    @op(name=name, ins=ins, **kwargs)
    def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Rename dataframe columns."""
        # Access the config params via the dagster context keyword.
        return df.rename(columns=column_mapping)

    return rename_columns


@op
def concat_xbrl_dbf_dfs(dbf_df: pd.DataFrame, xbrl_df: pd.DataFrame) -> pd.DataFrame:
    """Concat xbrl and dbf dataframes."""
    return pd.concat([dbf_df, xbrl_df])


@op
def process_steam_and_fuel(
    steam_df: pd.DataFrame, fuel_df: pd.DataFrame
) -> pd.DataFrame:
    """Process clean steam and fuel tables."""
    # Some made up transformation that depends on the steam and fuel dfs.
    return pd.concat([steam_df, fuel_df])


#### Table transform Graphs
@graph
def transform_steam(dbf_raw_dfs, xbrl_raw_dfs, transformed_fuel_table) -> pd.DataFrame:
    steam_xbrl_df = get_df_from_dict_factory("steam", "xbrl")(xbrl_raw_dfs)
    steam_dbf_df = get_df_from_dict_factory("steam", "dbf")(dbf_raw_dfs)
    table_metadata = TRANSFORM_METADATA["steam"]

    transformed_dbf_df = rename_columns_factory(
        table_metadata["dbf"]["rename_columns"]["column_mapping"],
        name="steam_dbf_rename_columns",
    )(steam_dbf_df)
    transformed_xbrl_df = rename_columns_factory(
        table_metadata["xbrl"]["rename_columns"]["column_mapping"],
        name="steam_xbrl_rename_columns",
    )(steam_xbrl_df)

    transformed_steam_df = concat_xbrl_dbf_dfs(transformed_dbf_df, transformed_xbrl_df)
    return process_steam_and_fuel(transformed_steam_df, transformed_fuel_table)


@graph
def transform_fuel(dbf_raw_dfs, xbrl_raw_dfs):
    fuel_xbrl_df = get_df_from_dict_factory("fuel", "xbrl")(xbrl_raw_dfs)
    fuel_dbf_df = get_df_from_dict_factory("fuel", "dbf")(dbf_raw_dfs)
    table_metadata = TRANSFORM_METADATA["fuel"]

    transformed_dbf_df = rename_columns_factory(
        table_metadata["dbf"]["rename_columns"]["column_mapping"],
        name="fuel_dbf_rename_columns",
    )(fuel_dbf_df)
    transformed_xbrl_df = rename_columns_factory(
        table_metadata["xbrl"]["rename_columns"]["column_mapping"],
        name="fuel_xbrl_rename_columns",
    )(fuel_xbrl_df)

    return concat_xbrl_dbf_dfs(transformed_dbf_df, transformed_xbrl_df)


@graph
def transform_hydro(dbf_raw_dfs, xbrl_raw_dfs):
    hydro_xbrl_df = get_df_from_dict_factory("hydro", "xbrl")(xbrl_raw_dfs)
    hydro_dbf_df = get_df_from_dict_factory("hydro", "dbf")(dbf_raw_dfs)
    table_metadata = TRANSFORM_METADATA["hydro"]

    transformed_dbf_df = rename_columns_factory(
        table_metadata["dbf"]["rename_columns"]["column_mapping"],
        name="hydro_dbf_rename_columns",
    )(hydro_dbf_df)
    transformed_xbrl_df = rename_columns_factory(
        table_metadata["xbrl"]["rename_columns"]["column_mapping"],
        name="hydro_xbrl_rename_columns",
    )(hydro_xbrl_df)

    return concat_xbrl_dbf_dfs(transformed_dbf_df, transformed_xbrl_df)


#### ETL ops and graphs
@op
def load(name):
    print(f"Loading {name}")


ferc1_tfr_funcs = {
    "steam": transform_steam,
    "fuel": transform_fuel,
    "hydro": transform_hydro,
}


@graph(out={name: GraphOut() for name in ferc1_tfr_funcs})
def transform(dbf_raw_dfs, xbrl_raw_dfs):
    transformed_dfs = {}

    # Define dependencies between tables.
    transformed_dfs["fuel"] = transform_fuel(dbf_raw_dfs, xbrl_raw_dfs)
    transformed_dfs["steam"] = transform_steam(
        dbf_raw_dfs, xbrl_raw_dfs, transformed_dfs["fuel"]
    )

    # Transform tables that don't have dependencies
    for table_name, transform_func in ferc1_tfr_funcs.items():
        # If the table hasn't been transformed, transform it!
        if table_name not in transformed_dfs:
            transformed_dfs[table_name] = transform_func(dbf_raw_dfs, xbrl_raw_dfs)

    return transformed_dfs


@graph
def etl_ferc1():
    dbf_raw_dfs = extract_dbf()
    xbrl_raw_dfs = extract_xbrl()
    transformed_dfs = transform(dbf_raw_dfs, xbrl_raw_dfs)
    for table in transformed_dfs:
        load(table)


etl_ferc1_job = etl_ferc1.to_job(executor_def=in_process_executor)


def main():
    etl_ferc1_job.execute_in_process()


if __name__ == "__main__":
    main()
