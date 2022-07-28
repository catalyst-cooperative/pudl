import pandas as pd
from dagster import job, op


def rename_columns_factory(
    name="default_name",
    ins=None,
    column_mapping=None,
    **kwargs,
):
    """
    Args:
        name (str): The name of the new op.
        ins (Dict[str, In]): Any Ins for the new op. Default: None.

    Returns:
        function: The new op.
    """

    @op(name=name, ins=ins, **kwargs)
    def rename_df(context, df):
        context.log.info(f"\n The DataFrame: {df}\n")
        context.log.info(f"\n The Op Ins: {context.op_def.ins}\n")
        t_df = df.rename(columns=column_mapping)
        context.log.info(f"\n The Transformed DataFrame: {t_df}\n")
        return t_df

    return rename_df


@op
def extract():
    return pd.DataFrame([1, 2], columns=["col"])


@job()
def etl():
    df = extract()
    column_mapping = {"col": "column"}
    transformed_df = rename_columns_factory(column_mapping=column_mapping)(df)


etl.execute_in_process()
