"""Connect FERC1 plant tables to EIA's plant-parts with record linkage.

FERC plant records are reported very non-uniformly. In the same table there are records
that are reported as whole plants, individual generators, and collections of prime
movers. This means portions of EIA plants that correspond to a plant record in FERC
Form 1 are heterogeneous, which complicates using the two data sets together.

The EIA plant data is much cleaner and more uniformly structured. The are generators
with ids and plants with ids reported in *separate* tables. Several generator IDs are
typically grouped under a single plant ID. In :mod:`pudl.analysis.plant_parts_eia`,
we create a large number of synthetic aggregated records representing many possible
slices of a power plant which could in theory be what is actually reported in the FERC
Form 1.

In this module we infer which of the many ``plant_parts_eia`` records is most likely to
correspond to an actually reported FERC Form 1 plant record. This is done with
``splink``, a Python package that implements Fellegi-Sunter's model of record linkage.

We train the parameters of the ``splink`` model using manually labeled training data
that links together several thousand EIA and FERC plant records. This trained model is
used to predict matches on the full dataset (see :func:`get_model_predictions`) using a
threshold match probability to predict if records are a match or not.
The model can return multiple EIA match options for each FERC1 record, so we rank the
matches and choose the one with the highest score. Any matches identified by the model
which are in conflict with our training data are overwritten with the manually
assigned associations (see :func:`overwrite_bad_predictions`). The final match results
are the connections we keep as the matches between FERC1 plant records and EIA
plant-parts.
"""
import pandas as pd
from dagster import Out, graph_asset, op
from splink.duckdb.linker import DuckDBLinker

import pudl
from pudl.analysis.record_linkage import embed_dataframe
from pudl.analysis.record_linkage.eia_ferc1_record_linkage import (
    add_null_overrides,
    get_compiled_input_manager,
    overwrite_bad_predictions,
    prettyify_best_matches,
)
from pudl.analysis.record_linkage.eia_ferc1_splink_model_config import (
    BLOCKING_RULES,
    COMPARISONS,
)

logger = pudl.logging_helpers.get_logger(__name__)

MATCHING_COLS = [
    "plant_name",
    "utility_name",
    "fuel_type_code_pudl",
    "installation_year",
    "construction_year",
    "capacity_mw",
    "net_generation_mwh",
]
# retain these columns either for blocking or validation
# not going to match with these
ID_COL = ["record_id"]
EXTRA_COLS = ["report_year", "plant_id_pudl", "utility_id_pudl"]

col_cleaner_ferc = embed_dataframe.dataframe_cleaner_factory(
    "col_cleaner_ferc",
    {
        "plant_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner()],
            columns=["plant_name"],
        ),
        "utility_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner()],
            columns=["utility_name"],
        ),
        "fuel_type_code_pudl": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.FuelTypeFiller(
                    fuel_type_col="fuel_type_code_pudl",
                    name_col="plant_name",
                )
            ],
            columns=["fuel_type_code_pudl", "plant_name"],
        ),
    },
)

col_cleaner_eia = embed_dataframe.dataframe_cleaner_factory(
    "col_cleaner_eia",
    {
        "plant_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner()],
            columns=["plant_name"],
        ),
        "utility_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner()],
            columns=["utility_name"],
        ),
    },
)


@op(out={"eia_df": Out(), "ferc_df": Out()})
def get_input_dfs(inputs):
    """Get EIA and FERC inputs for the model."""
    eia_df = (
        inputs.get_plant_parts_eia_true()
        .reset_index()
        .rename(
            columns={
                "record_id_eia": "record_id",
                "plant_name_eia": "plant_name",
                "utility_name_eia": "utility_name",
            }
        )
    )
    ferc_df = (
        inputs.get_plants_ferc1()
        .reset_index()
        .rename(
            columns={
                "record_id_ferc1": "record_id",
                "plant_name_ferc1": "plant_name",
                "utility_name_ferc1": "utility_name",
            }
        )
    )
    return eia_df, ferc_df


@op
def prepare_for_matching(df, transformed_df):
    """Prepare the input dataframes for matching with splink."""
    # replace old cols with transformed cols
    for col in transformed_df.columns:
        orig_col_name = col.split("__")[1]
        df[orig_col_name] = transformed_df[col]
    cols = ID_COL + MATCHING_COLS + EXTRA_COLS
    df = df.loc[:, cols]
    df["installation_year"] = pd.to_datetime(df["installation_year"], format="%Y")
    df["construction_year"] = pd.to_datetime(df["construction_year"], format="%Y")
    return df


@op
def get_training_data_df(inputs):
    """Get the manually created training data."""
    train_df = inputs.get_train_df().reset_index()
    train_df = train_df[["record_id_ferc1", "record_id_eia"]].rename(
        columns={"record_id_eia": "record_id_l", "record_id_ferc1": "record_id_r"}
    )
    train_df.loc[:, "source_dataset_r"] = "ferc_df"
    train_df.loc[:, "source_dataset_l"] = "eia_df"
    train_df.loc[
        :, "clerical_match_score"
    ] = 1  # this column shows that all these labels are positive labels
    return train_df


@op
def get_model_predictions(eia_df, ferc_df, train_df):
    """Train splink model and output predicted matches."""
    settings_dict = {
        "link_type": "link_only",
        "unique_id_column_name": "record_id",
        "additional_columns_to_retain": ["plant_id_pudl", "utility_id_pudl"],
        "comparisons": COMPARISONS,
        "blocking_rules_to_generate_predictions": BLOCKING_RULES,
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
        "probability_two_random_records_match": 1 / len(eia_df),
    }
    linker = DuckDBLinker(
        [eia_df, ferc_df],
        input_table_aliases=["eia_df", "ferc_df"],
        settings_dict=settings_dict,
    )
    linker.register_table(train_df, "training_labels", overwrite=True)
    linker.estimate_u_using_random_sampling(max_pairs=1e7)
    linker.estimate_m_from_pairwise_labels("training_labels")
    # set a match probability threshold?
    # do this at a bunch of different thresholds and print out stats
    preds_df = linker.predict(threshold_match_probability=0.9)
    return preds_df.as_pandas_dataframe()


@op
def get_best_matches_with_training_data_overwrites(preds_df, inputs):
    """Get the best EIA match for each FERC record."""
    preds_df = (
        preds_df.rename(
            columns={"record_id_l": "record_id_eia", "record_id_r": "record_id_ferc1"}
        )
        .sort_values(by="match_probability", ascending=False)
        .groupby("record_id_ferc1")
        .first()
    )
    preds_df = overwrite_bad_predictions(preds_df, inputs.get_train_df())
    return preds_df


@op
def get_full_records(best_match_df, inputs):
    """Join full dataframe onto matches to make usable and get stats."""
    connected_df = prettyify_best_matches(
        matches_best=best_match_df,
        plant_parts_eia_true=inputs.get_plant_parts_eia_true(),
        plants_ferc1=inputs.get_plants_ferc1(),
        train_df=inputs.get_train_df(),
    ).pipe(add_null_overrides)  # Override specified values with NA record_id_eia
    return connected_df


@graph_asset
def out_pudl__yearly_assn_eia_ferc1_plant_parts_splink(
    out_ferc1__yearly_all_plants: pd.DataFrame,
    out_ferc1__yearly_steam_plants_fuel_by_plant_sched402: pd.DataFrame,
    out_eia__yearly_plant_parts: pd.DataFrame,
) -> pd.DataFrame:
    """Using splink model the connection between FERC1 plants and EIA plant-parts.

    Args:
        out_ferc1__yearly_all_plants: Table of all of the FERC1-reporting plants.
        out_ferc1__yearly_steam_plants_fuel_by_plant_sched402: Table of the fuel
            reported aggregated to the FERC1 plant-level.
        out_eia__yearly_plant_parts: The EIA plant parts list.
    """
    inputs = get_compiled_input_manager(
        out_ferc1__yearly_all_plants,
        out_ferc1__yearly_steam_plants_fuel_by_plant_sched402,
        out_eia__yearly_plant_parts,
    )
    eia_df, ferc_df = get_input_dfs(inputs)
    train_df = get_training_data_df(inputs)
    # apply cleaning transformations to some columns
    transformed_eia_df = col_cleaner_eia(eia_df)
    transformed_ferc_df = col_cleaner_ferc(ferc_df)
    # prepare for matching with splink
    ferc_df = prepare_for_matching(ferc_df, transformed_ferc_df)
    eia_df = prepare_for_matching(eia_df, transformed_eia_df)
    # train model and predict matches
    preds_df = get_model_predictions(eia_df=eia_df, ferc_df=ferc_df, train_df=train_df)
    best_match_df = get_best_matches_with_training_data_overwrites(
        preds_df=preds_df, inputs=inputs
    )
    ferc1_eia_connected_df = get_full_records(best_match_df, inputs)

    return ferc1_eia_connected_df