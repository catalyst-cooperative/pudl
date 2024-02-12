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


# read in parquet, for now?
incorrect_matches = pd.read_parquet("incorrect_matches.parquet")
sorted_preds_df = pd.read_parquet("supervised_predictions.parquet")
eia_df = pd.read_parquet("eia_df.parquet")
ferc_df = pd.read_parquet("ferc_df.parquet")
full_match_records = pd.read_parquet("full_match_records.parquet")

merged_df = ferc_df.merge(
    sorted_preds_df,
    left_on="record_id",
    right_on="record_id_r",
    how="left",
    indicator=True,
)
cov_dict = {}
for fuel in ["gas", "coal", "oil", "hydro", "solar", "nuclear", "wind", "waste"]:
    counts = merged_df[merged_df.fuel_type_code_pudl == fuel]._merge.value_counts()
    cov = counts["both"] / (counts["left_only"] + counts["both"])
    total_cap = merged_df[merged_df.fuel_type_code_pudl == fuel]["capacity_mw"].sum()
    cov_cap = merged_df[
        (merged_df.fuel_type_code_pudl == fuel) & (merged_df._merge == "both")
    ]["capacity_mw"].sum()
    perc_cap = cov_cap / total_cap
    cov_dict[fuel] = [round(cov, 2), round(perc_cap, 2)]


cov_df = pd.DataFrame(cov_dict).T
cov_df.columns = ["% Record Coverage", "% Capacity Coverage"]
st.write("### Match Coverage of FERC Plants By Fuel:")
st.write(
    "**_Model uses a probability of match threshold of .9 to determine matches._**"
)
st.write(cov_df)
st.write(
    "**_% Record Coverage indicates the percentage of the total number of FERC records for that are assigned a matching EIA record._**"
)
st.write(
    "**_% Capacity Coverage indicates the percentage of the total MW capacity of FERC plants that's assigned a matching EIA record._**"
)


full_match_records.set_index("record_id_ferc1", inplace=True)

st.write("### Match Comparison Viewer:")

# Dropdown menu to select record
selected_ferc_index = st.selectbox("Select a FERC record:", full_match_records.index)


match_prob_df = (
    sorted_preds_df[sorted_preds_df.record_id_r == selected_ferc_index]
    .rename(columns={"record_id_l": "record_id_eia"})
    .sort_values(by="match_probability", ascending=False)
    .head(1)
)

eia_index = match_prob_df.record_id_eia.iloc[0]

match_prob_df = match_prob_df.set_index("record_id_eia")
st.write("EIA Match:")
st.write(match_prob_df[["match_probability"]])

# Display comparison of records
columns = [
    "record_id",
    "report_year",
    "plant_name",
    "utility_name",
    "construction_year",
    "installation_year",
    "capacity_mw",
    "fuel_type_code_pudl",
    "net_generation_mwh",
]

comp_df = pd.concat(
    [
        ferc_df[ferc_df.record_id == selected_ferc_index].T,
        eia_df[eia_df.record_id == eia_index].T,
    ],
    axis=1,
)
comp_df.columns = ["FERC record", "EIA record"]

st.write("Match Comparison:")
st.write(comp_df.loc[columns])

st.write("Manually Assigned PUDL IDs (for validation):")
st.write(comp_df.loc[["plant_id_pudl", "utility_id_pudl"]])

rec_true = sorted_preds_df[
    (sorted_preds_df.record_id_r == selected_ferc_index)
    & (sorted_preds_df.record_id_l == eia_index)
]


rec_true = rec_true.to_dict(orient="records")
linker = DuckDBLinker(
    [eia_df, ferc_df],
    input_table_aliases=["eia_df", "ferc_df"],
)
linker.load_model("model_settings_splink_ferc_eia_demo.json")

chart = linker.waterfall_chart(rec_true, filter_nulls=False)
st.altair_chart(chart, use_container_width=True)
