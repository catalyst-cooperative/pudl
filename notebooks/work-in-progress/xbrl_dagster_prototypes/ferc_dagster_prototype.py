import pandas as pd
from dagster import GraphOut, graph, in_process_executor, job, op


@op
def extract_dbf():
    return {
        "steam": pd.DataFrame(),
        "hydro": pd.DataFrame(),
        "small_plants": pd.DataFrame(),
        "fuel": pd.DataFrame(),
    }


@op
def extract_xbrl():
    return {
        "steam": pd.DataFrame(),
        "hydro": pd.DataFrame(),
        "small_plants": pd.DataFrame(),
        "fuel": pd.DataFrame(),
    }


@op
def transform_steam(dbf_raw_dfs, xbrl_raw_dfs, transformed_fuel_table):
    print(f"Do something with transformed_fuel_table: {transformed_fuel_table.shape}")
    return pd.concat([dbf_raw_dfs["steam"], xbrl_raw_dfs["steam"]])


@op
def transform_fuel(dbf_raw_dfs, xbrl_raw_dfs):
    return pd.concat([dbf_raw_dfs["fuel"], xbrl_raw_dfs["fuel"]])


@op
def transform_hydro(dbf_raw_dfs, xbrl_raw_dfs):
    return pd.concat([dbf_raw_dfs["hydro"], xbrl_raw_dfs["hydro"]])


@op
def transform_small_plants(dbf_raw_dfs, xbrl_raw_dfs):
    return pd.concat([dbf_raw_dfs["small_plants"], xbrl_raw_dfs["small_plants"]])


@op
def load(name):
    print(f"Loading {name}")


ferc1_tfr_funcs = {
    "steam": transform_steam,
    "fuel": transform_fuel,
    "hydro": transform_hydro,
    "small_plants": transform_small_plants,
}


@graph(out={name: GraphOut() for name in ferc1_tfr_funcs})
def transform(dbf_raw_dfs, xbrl_raw_dfs):
    transformed_dfs = {}

    # Define dependencies between tables. We don't have to pass around the
    # transform_dfs dictionary if we specify the dependencies using dagster.
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
