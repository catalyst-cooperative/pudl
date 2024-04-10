"""Module to perform data cleaning functions on EIA176 data tables."""

import pandas as pd


def get_wide_table(df):
    """Return a table comprising primary keys and one column per variable."""
    variable_names = ["report_year", "area", "id"] + list(
        df.groupby("line_atype").count().index
    )
    unaggregated_df = df[(df["company"] != " Total of All Companies")]

    new_df = pd.DataFrame(columns=variable_names)

    for report_year, area, id_ in (
        unaggregated_df.groupby(["report_year", "area", "id"]).count().index
    ):
        sub_df = unaggregated_df[
            (unaggregated_df["report_year"] == report_year)
            & (unaggregated_df["area"] == area)
            & (unaggregated_df["id"] == id_)
        ]

        new_row = {"report_year": report_year, "area": area, "id": id_}

        for i in sub_df.iterrows():
            row = i[0]
            new_row[row["line_atype"]] = row["value"]

        new_df.loc[len(new_df.index)] = new_row

    return new_df
