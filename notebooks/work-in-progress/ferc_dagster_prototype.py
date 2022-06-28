import pandas as pd
from dagster import GraphOut, graph, in_process_executor, job, op


@op
def extract():
    return {"steam": pd.DataFrame(), "hydro": pd.DataFrame()}


@op
def transform_steam(df):
    return df["steam"]


@op
def transform_hydro(df):
    return df["hydro"]


@op
def load(name):
    print(f"Loading {name}")


@graph()
def transform(raw_dfs):
    transformed_dfs = {}
    ferc1_tfr_funcs = {"steam": transform_steam, "hydro": transform_hydro}
    for table in ferc1_tfr_funcs:
        transformed_dfs[table] = ferc1_tfr_funcs[table](raw_dfs)
    return transformed_dfs


@graph
def etl_ferc1():
    raw_dfs = extract()
    transformed_dfs = transform(raw_dfs)
    for table in transformed_dfs:
        load(table)


etl_ferc1_job = etl_ferc1.to_job(executor_def=in_process_executor)
