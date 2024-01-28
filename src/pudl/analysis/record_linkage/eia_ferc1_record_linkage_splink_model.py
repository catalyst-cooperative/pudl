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
from pudl.analysis.record_linkage.eia_ferc1_splink_rule_definitions import (
    BLOCKING_RULES,
    COMPARISONS,
)
from pudl.analysis.record_linkage.name_cleaner import CompanyNameCleaner

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

eia_cleaner = embed_dataframe.dataframe_cleaner_factory(
    "eia_cleaners",
    {
        "plant_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner()],
            columns=["plant_name_eia"],
        ),
        "utility_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner()],
            columns=["utility_name_eia"],
        ),
        "fuel_type_code_pudl": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.FuelTypeFiller(
                    fuel_type_col="fuel_type_code_pudl",
                    name_col="plant_name_eia",
                )
            ],
            columns=["fuel_type_code_pudl", "plant_name_eia"],
        ),
    },
)
ferc_cleaner = embed_dataframe.dataframe_cleaner_factory(
    "ferc1_cleaners",
    {
        "plant_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner()],
            columns=["plant_name_ferc1"],
        ),
        "utility_name": embed_dataframe.ColumnVectorizer(
            transform_steps=[embed_dataframe.NameCleaner()],
            columns=["utility_name_ferc1"],
        ),
        "fuel_type_code_pudl": embed_dataframe.ColumnVectorizer(
            transform_steps=[
                embed_dataframe.FuelTypeFiller(
                    fuel_type_col="fuel_type_code_pudl",
                    name_col="plant_name_ferc1",
                )
            ],
            columns=["fuel_type_code_pudl", "plant_name_ferc1"],
        ),
    },
)


@op
def prepare_for_matching(df):
    cols = ID_COL + MATCHING_COLS + EXTRA_COLS
    logger.info(f"COLS: {cols}")
    df = df[cols]
    df["installation_year"] = pd.to_datetime(df["installation_year"], format="%Y")
    df["construction_year"] = pd.to_datetime(df["construction_year"], format="%Y")
    return df


@op
def get_training_data_df(inputs):
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


@op(out={"eia_df": Out(), "ferc_df": Out()})
def get_input_dfs(inputs):
    eia_df = (
        inputs.get_plant_parts_eia_true()
        .reset_index()
        .rename(columns={"record_id_eia": "record_id"})
    )
    ferc_df = (
        inputs.get_plants_ferc1()
        .reset_index()
        .rename(columns={"record_id_ferc1": "record_id"})
    )
    return eia_df, ferc_df


@op
def get_trained_model(eia_df, ferc_df, train_df):
    settings_dict = {
        "link_type": "link_only",
        "unique_id_column_name": "record_id",
        "additional_columns_to_retain": ["plant_id_pudl", "utility_id_pudl"],
        "comparisons": [COMPARISONS],
        "blocking_rules_to_generate_predictions": [BLOCKING_RULES],
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
    return linker


@op
def make_predictions(linker):
    # set a match probability threshold?
    # do this at a bunch of different thresholds and print out stats
    preds_df = linker.predict()
    return preds_df.as_pandas_dataframe()


@op
def get_best_matches_with_training_data_overwrites(preds_df, inputs):
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
    # apply cleaning transformations
    eia_df = eia_cleaner(eia_df)
    ferc_df = ferc_cleaner(ferc_df)
    # prepare for matching with splink
    ferc_df = prepare_for_matching(ferc_df)
    eia_df = prepare_for_matching(eia_df)
    # get linker object
    linker = get_trained_model(eia_df=eia_df, ferc_df=ferc_df, train_df=train_df)
    preds_df = make_predictions(linker)
    best_match_df = get_best_matches_with_training_data_overwrites(
        preds_df=preds_df, inputs=inputs
    )
    ferc1_eia_connected_df = get_full_records(best_match_df, inputs)

    return ferc1_eia_connected_df
