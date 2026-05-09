import pandas as pd

from pudl.transform.eiaaeo import (
    series_sum_ratio,
    subtotals_match_reported_totals_ratio,
)


def test_subtotals_match_reported_totals():
    df = pd.DataFrame(
        {
            "pk_1": [1, 1, 1, 1],
            "pk_2": [2, 2, 2, 2],
            "dimension": ["a", "b", "c", "total"],
            "var_match": [1, 2, 3, 6],
            "var_mismatch": [11, 12, 13, 35],
        }
    )

    assert (
        subtotals_match_reported_totals_ratio(
            df,
            pk=["pk_1", "pk_2"],
            fact_columns=["var_match", "var_mismatch"],
            dimension_column="dimension",
        )
        == 1 / 2
    )

    assert (
        subtotals_match_reported_totals_ratio(
            df,
            pk=["pk_1", "pk_2"],
            fact_columns=["var_match"],
            dimension_column="dimension",
        )
        == 1
    )

    assert (
        subtotals_match_reported_totals_ratio(
            df,
            pk=["pk_1", "pk_2"],
            fact_columns=["var_mismatch"],
            dimension_column="dimension",
        )
        == 0
    )


def test_series_sum_ratio():
    df = pd.DataFrame(
        {
            "pk_1": [1, 1, 1, 1, 1.1],
            "pk_2": [2, 2, 2, 2, 2.1],
            "dimension": ["a", "b", "c", "total", "total"],
            "var_1": [1, 2, 3, 6, 4],
            "var_2": [11, 12, 13, 36, 14],
            "var_total": [None, None, None, 42, 27],
        }
    ).set_index(["pk_1", "pk_2", "dimension"])

    totals = df.xs("total", level="dimension")
    assert (
        series_sum_ratio(
            summands=totals.loc[:, ["var_1", "var_2"]], total=totals.var_total
        )
        == 1 / 2
    )
