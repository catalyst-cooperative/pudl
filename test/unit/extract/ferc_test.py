import hypothesis
import pandas as pd
import pandera as pa

from pudl.extract.ferc import use_latest_filing_for_context

example_schema = pa.DataFrameSchema(
    {
        "entity_id": pa.Column(str),
        "date": pa.Column("datetime64[ns]"),
        "utility_type": pa.Column(
            str, pa.Check.isin(["electric", "gas", "total", "other"])
        ),
        "publication_time": pa.Column("datetime64[ns]"),
        "int_factoid": pa.Column(int),
        "float_factoid": pa.Column(float),
        "str_factoid": pa.Column("str"),
    }
)


@hypothesis.given(example_schema.strategy(size=3))
def test_get_unique_row_per_context(df):
    context_cols = ["entity_id", "date", "utility_type"]
    deduped = use_latest_filing_for_context(df, context_cols)
    example_schema.validate(deduped)
    # for every [entity_id, utility_type, date] - there is only one row
    assert (~deduped.duplicated(subset=context_cols)).all()

    # for every *context* in the input there is a corresponding row in the output
    input_contexts = df.groupby(context_cols, as_index=False).last()
    inputs_paired_with_outputs = input_contexts.merge(
        deduped, on=context_cols, how="outer", suffixes=["_in", "_out"], indicator=True
    ).set_index(context_cols)
    assert (inputs_paired_with_outputs._merge == "both").all()

    # for every row in the output - its publication time is greater than or equal to all of the other ones for that [entity_id, utility_type, date] in the input data
    assert (
        inputs_paired_with_outputs["publication_time_out"]
        >= inputs_paired_with_outputs["publication_time_in"]
    ).all()


def test_latest_filing():
    first_2021_filing = [
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2020-12-31",
            "publication_time": "2022-02-02T01:02:03Z",
            "factoid_1": 10.0,
            "factoid_2": 20.0,
        },
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2021-12-31",
            "publication_time": "2022-02-02T01:02:03Z",
            "factoid_1": 11.0,
            "factoid_2": 21.0,
        },
    ]

    second_2021_filing = [
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2020-12-31",
            "publication_time": "2022-02-02T01:05:03Z",
            "factoid_1": 10.1,
            "factoid_2": 20.1,
        },
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2021-12-31",
            "publication_time": "2022-02-02T01:05:03Z",
            "factoid_1": 11.1,
            "factoid_2": 21.1,
        },
    ]

    first_2022_filing = [
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2021-12-31",
            "publication_time": "2023-04-02T01:05:03Z",
            "factoid_1": 110.0,
            "factoid_2": 120.0,
        },
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2022-12-31",
            "publication_time": "2023-04-02T01:05:03Z",
            "factoid_1": 111.0,
            "factoid_2": 121.0,
        },
    ]

    test_df = pd.DataFrame.from_records(
        first_2021_filing + first_2022_filing + second_2021_filing
    ).convert_dtypes()
    context_cols = ["entity_id", "date", "utility_type"]
    deduped = use_latest_filing_for_context(test_df, context_cols)

    # for every [entity_id, utility_type, date] - there is only one row
    assert (~deduped.duplicated(subset=context_cols)).all()

    # for every *context* in the input there is a corresponding row in the output
    input_contexts = test_df.groupby(context_cols, as_index=False).last()
    inputs_paired_with_outputs = input_contexts.merge(
        deduped, on=context_cols, how="outer", suffixes=["_in", "_out"], indicator=True
    ).set_index(context_cols)
    assert (inputs_paired_with_outputs._merge == "both").all()

    # for every row in the output - its publication time is greater than or equal to all of the other ones for that [entity_id, utility_type, date] in the input data
    assert (
        inputs_paired_with_outputs["publication_time_out"]
        >= inputs_paired_with_outputs["publication_time_in"]
    ).all()
