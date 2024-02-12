import duckdb
import pandas as pd
import streamlit as st
from splink.duckdb.linker import DuckDBLinker

# Streamlit page configuration
st.title("FERC1 - EIA Record Linkage")

# Connect to DuckDB database
# con = duckdb.connect(database="eia_ferc1_match.db", read_only=True)
# incorrect_matches = duckdb.sql("FROM incorrect_matches").df()
# sorted_preds_df = duckdb.sql("FROM out_pudl__yearly_assn_eia_ferc1").df()


st.write("Manually Assigned PUDL IDs (for validation):")
st.write(comp_df.loc[["plant_id_pudl", "utility_id_pudl"]])

# read in parquet, for now?
incorrect_matches = pd.read_parquet("incorrect_matches.parquet")
sorted_preds_df = pd.read_parquet("sorted_preds_df.parquet")
eia_df = pd.read_parquet("eia_df.parquet")
ferc_df = pd.read_parquet("ferc_df.parquet")
full_match_records = pd.read_parquet("full_match_records.parquet")
i = 0
ferc_id = incorrect_matches.record_id_ferc1.iloc[i]
true_eia_id = incorrect_matches.record_id_eia_true.iloc[i]
pred_eia_id = incorrect_matches.record_id_eia_pred.iloc[i]

rec_true = sorted_preds_df[
    (sorted_preds_df.record_id_r == ferc_id)
    & (sorted_preds_df.record_id_l == true_eia_id)
]
rec_pred = sorted_preds_df[
    (sorted_preds_df.record_id_r == ferc_id)
    & (sorted_preds_df.record_id_l == pred_eia_id)
]

rec_true = rec_true.to_dict(orient="records")
linker = DuckDBLinker(
    [eia_df, ferc_df],
    input_table_aliases=["eia_df", "ferc_df"],
)
linker.load_model("model_settings_splink_ferc_eia_demo.json")

chart = linker.waterfall_chart(rec_true, filter_nulls=False)
st.altair_chart(chart, use_container_width=True)
