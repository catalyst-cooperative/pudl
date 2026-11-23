"""Generate FERC1-EIA utility pairs to hand-label as true or false matches.

Builds an **unsupervised** Splink model (parameters estimated with expectation
maximisation, no labels), so the candidates are not shaped by the existing training
data. It then samples a stratified set of utility pairs for manual review, so that the
labelled set contains both true matches and false matches across the whole range of
model scores:

- **score bands:** pairs spread evenly across match-probability bands.
- **disagreements:** pairs the exact cleaned-name match and the model disagree about
  (exact name match but low score; high score but no exact name match).
- **crosswalk-linked:** a random sample of pairs that ``utility_id_pudl.csv`` already
  links, to measure how many true matches the model scores low.
- **name similarity:** pairs whose cleaned names are similar (Jaro-Winkler, found by
  comparing *all* FERC names with *all* EIA names, with no blocking), so that likely
  matches the blocked model cannot see (renames, abbreviations) are included.
- **numeric agreement:** pairs whose customers (within 2%) and retail sales (within
  3%) agree in the same year, for the features in ``utility_match_features.py``. The
  unsupervised model also uses them. Review labels that depended on these numbers
  should be flagged in the ``numeric_informed`` column.
- **top-ups:** similar-name pairs first, then random pairs.

Pairs already in the training CSV or in ``utility_id_pudl_false_matches.csv`` are
skipped. Rows are shuffled so the review is not ordered by model score.

Fill in the ``is_match`` column (True / False) of the output, then:

- true matches can be added to the crosswalk and the training data;
- false matches can be added to ``utility_id_pudl_false_matches.csv`` and the training
  data.

Usage (from the repo root)::

    pixi run -e dev python devtools/generate_label_candidates.py --nightly
"""

from pathlib import Path

import click
import duckdb
import pandas as pd
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from utility_match_features import (
    load_numeric_features,
    numeric_blocking_rule,
    numeric_comparisons,
)

from pudl import PUDL_PACKAGE_DATA_PATH
from pudl.analysis.record_linkage.name_cleaner import (
    CompanyNameCleaner,
    HandleLegalTerms,
)
from pudl.helpers import get_parquet_table
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

DEVTOOLS_DIR = Path(__file__).parent
GLUE_DIR = PUDL_PACKAGE_DATA_PATH / "glue"
NIGHTLY_URL = "s3://pudl.catalyst.coop/nightly"

MATCH_COLS = {
    "utility_name": {"eia": "utility_name_eia", "ferc": "utility_name_ferc1"},
    "street_address": {"eia": "street_address", "ferc": "office_street_address"},
    "city": {"eia": "city", "ferc": "office_city"},
    "zip": {"eia": "zip_code", "ferc": "office_zip"},
    "state": {"eia": "state", "ferc": "office_state"},
}
ID_COLS = {"eia": "utility_id_eia", "ferc": "utility_id_ferc1"}
MODEL_COLS = [
    "utility_name",
    "street_address",
    "zip",
    "state",
    "customers",
    "sales_mwh",
    "record_id",
    "report_year",
    *ID_COLS.values(),
]
PAIR_KEY = ["utility_id_ferc1", "utility_id_eia"]

SCORE_BANDS = [0.0, 0.3, 0.6, 0.9, 0.99, 1.0001]
SIMILARITY_BANDS = [0.80, 0.85, 0.90, 0.95, 1.0001]
# Number of years in which both numeric features agree (customers within 5%, sales
# within 10%).
NUMERIC_YEAR_BANDS = [1, 2, 4, 1000]
# One-year agreements are mostly coincidences between similarly sized utilities, so
# that band gets a smaller share than the multi-year bands.
NUMERIC_BAND_WEIGHTS = [0.5, 1.0, 1.0]
# Fractions of the requested sample size; the remainder is topped up.
FRACTION_BANDS = 0.15
FRACTION_DISAGREEMENT = 0.10
FRACTION_CROSSWALK = 0.05
FRACTION_NAME_SIMILARITY = 0.25
FRACTION_NUMERIC = 0.30


def load_table(table_name: str, nightly: bool) -> pd.DataFrame:
    """Load a PUDL table from $PUDL_OUTPUT parquet files or the nightly S3 build."""
    if nightly:
        return pd.read_parquet(f"{NIGHTLY_URL}/{table_name}.parquet")
    return get_parquet_table(table_name)


def prepare_records(
    raw: pd.DataFrame, dataset: str, features: pd.DataFrame
) -> pd.DataFrame:
    """Clean one side's records for matching and keep raw values for display."""
    df = raw.rename(
        columns={cols[dataset]: name for name, cols in MATCH_COLS.items()}
    ).copy()
    if dataset == "eia":
        df["report_year"] = pd.to_datetime(df["report_date"]).dt.year
    # Splink needs unique records: keep one row per utility-year.
    df = df.drop_duplicates([ID_COLS[dataset], "report_year"])
    df = df.merge(features, on=[ID_COLS[dataset], "report_year"], how="left")
    df = df.reset_index(drop=True)
    df["record_id"] = df.index.astype("string")
    for col in MATCH_COLS:
        df[f"{col}_raw"] = df[col]
        df[col] = df[col].astype("string").str.strip().str.lower().replace("", pd.NA)
    df["utility_name"] = CompanyNameCleaner(
        handle_legal_terms=HandleLegalTerms.REMOVE
    ).clean_series(df["utility_name"])
    # Splink vertically concatenates inputs, so both need the same schema.
    for id_col in ID_COLS.values():
        df[id_col] = (
            df[id_col].astype("Int64")
            if id_col in df
            else pd.Series(pd.NA, index=df.index, dtype="Int64")
        )
    return df


def predict_unsupervised(
    eia_df: pd.DataFrame,
    ferc_df: pd.DataFrame,
    min_probability: float,
    seed: int,
    *,
    numeric: bool = True,
) -> pd.DataFrame:
    """Train a Splink model without labels (EM) and score candidate record pairs.

    Args:
        eia_df: Prepared EIA records.
        ferc_df: Prepared FERC1 records.
        min_probability: Lowest record-pair match probability to return.
        seed: Seed for the random sampling that estimates the u values.
        numeric: Whether to use the customers / sales comparisons and the numeric
            blocking rule.
    """
    settings = SettingsCreator(
        link_type="link_only",
        unique_id_column_name="record_id",
        comparisons=[
            cl.NameComparison(
                "utility_name", jaro_winkler_thresholds=[0.97, 0.93, 0.88]
            ),
            cl.NameComparison("street_address", jaro_winkler_thresholds=[0.97]),
            cl.ExactMatch("state").configure(term_frequency_adjustments=True),
            cl.ExactMatch("zip").configure(term_frequency_adjustments=True),
            *(numeric_comparisons() if numeric else []),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("report_year", "state"),
            block_on("report_year", "zip"),
            *([numeric_blocking_rule()] if numeric else []),
        ],
        probability_two_random_records_match=1 / len(eia_df),
    )
    db_api = DuckDBAPI()
    linker = Linker(
        [
            db_api.register(eia_df[MODEL_COLS], dataset_display_name="eia"),
            db_api.register(ferc_df[MODEL_COLS], dataset_display_name="ferc"),
        ],
        settings=settings,
    )
    linker.training.estimate_u_using_random_sampling(
        max_pairs=1e6, seed=seed, min_count_per_level=None
    )
    for rule in (block_on("report_year", "state"), block_on("report_year", "zip")):
        linker.training.estimate_parameters_using_expectation_maximisation(rule)
    return linker.inference.predict(
        threshold_match_probability=min_probability
    ).as_pandas_dataframe()[["record_id_l", "record_id_r", "match_probability"]]


def aggregate_to_utility_pairs(
    predictions: pd.DataFrame, eia_df: pd.DataFrame, ferc_df: pd.DataFrame
) -> pd.DataFrame:
    """Aggregate record-pair scores to one row per (FERC1, EIA) utility pair."""
    return (
        predictions.merge(
            eia_df[["record_id", "utility_id_eia", "report_year"]],
            left_on="record_id_l",
            right_on="record_id",
        )
        .merge(
            ferc_df[["record_id", "utility_id_ferc1"]],
            left_on="record_id_r",
            right_on="record_id",
        )
        .groupby(PAIR_KEY, as_index=False)
        .agg(
            match_probability=("match_probability", "max"),
            n_matching_years=("report_year", "nunique"),
            first_year=("report_year", "min"),
            last_year=("report_year", "max"),
        )
    )


def exact_name_pairs(eia_df: pd.DataFrame, ferc_df: pd.DataFrame) -> pd.DataFrame:
    """Utility pairs that share a cleaned name in any year."""
    names = ["utility_name"]
    return (
        ferc_df.dropna(subset=names)[[*names, "utility_id_ferc1"]]
        .merge(eia_df.dropna(subset=names)[[*names, "utility_id_eia"]], on=names)[
            PAIR_KEY
        ]
        .drop_duplicates()
        .astype("int64")
        .assign(exact_name_match=True)
    )


def similar_name_pairs(
    eia_df: pd.DataFrame, ferc_df: pd.DataFrame, min_similarity: float
) -> pd.DataFrame:
    """Utility pairs with similar cleaned names, compared without any blocking."""
    ferc_names = (
        ferc_df.dropna(subset=["utility_name"])[["utility_id_ferc1", "utility_name"]]
        .drop_duplicates()
        .astype({"utility_name": str, "utility_id_ferc1": "int64"})
    )
    eia_names = (
        eia_df.dropna(subset=["utility_name"])[["utility_id_eia", "utility_name"]]
        .drop_duplicates()
        .astype({"utility_name": str, "utility_id_eia": "int64"})
    )
    con = duckdb.connect()
    con.register("ferc_names", ferc_names)
    con.register("eia_names", eia_names)
    return con.sql(
        """
        SELECT f.utility_id_ferc1, e.utility_id_eia,
               max(jaro_winkler_similarity(f.utility_name, e.utility_name))
                   AS name_similarity
        FROM ferc_names f CROSS JOIN eia_names e
        GROUP BY f.utility_id_ferc1, e.utility_id_eia
        HAVING name_similarity >= $min_similarity
        """,
        params={"min_similarity": min_similarity},
    ).df()


def numeric_agreement_pairs(
    eia_df: pd.DataFrame, ferc_df: pd.DataFrame
) -> pd.DataFrame:
    """Utility pairs whose customers and sales agree in at least one year.

    Agreement means, in the same report year, customers within 2% and sales within 3%
    (customers within 1% when a side has no sales).
    """
    ferc = ferc_df[["utility_id_ferc1", "report_year", "customers", "sales_mwh"]]
    eia = eia_df[["utility_id_eia", "report_year", "customers", "sales_mwh"]]
    con = duckdb.connect()
    con.register("ferc", ferc.astype({"utility_id_ferc1": "int64"}))
    con.register("eia", eia.astype({"utility_id_eia": "int64"}))
    return con.sql(
        """
        SELECT f.utility_id_ferc1, e.utility_id_eia,
               count(*) AS numeric_years_agree,
               max(f.report_year) AS numeric_year
        FROM ferc f JOIN eia e ON f.report_year = e.report_year
        WHERE f.customers IS NOT NULL AND e.customers IS NOT NULL
          AND CASE
                WHEN f.sales_mwh IS NULL OR e.sales_mwh IS NULL
                  THEN abs(f.customers - e.customers)
                       <= 0.01 * greatest(f.customers, e.customers)
                ELSE abs(f.customers - e.customers)
                       <= 0.02 * greatest(f.customers, e.customers)
                     AND abs(f.sales_mwh - e.sales_mwh)
                       <= 0.03 * greatest(f.sales_mwh, e.sales_mwh)
              END
        GROUP BY f.utility_id_ferc1, e.utility_id_eia
        """
    ).df()


def crosswalk_status(pairs: pd.DataFrame) -> pd.Series:
    """Label each pair as linked, conflicting or unmapped in the crosswalk."""
    glue = pd.read_csv(GLUE_DIR / "utility_id_pudl.csv")
    maps = {}
    for side in ("ferc1", "eia"):
        col = f"utility_id_{side}"
        maps[side] = (
            glue.dropna(subset=[col])
            .astype({col: "int64"})
            .drop_duplicates(col)
            .set_index(col)["utility_id_pudl"]
        )
    ferc_pudl = pairs["utility_id_ferc1"].map(maps["ferc1"])
    eia_pudl = pairs["utility_id_eia"].map(maps["eia"])
    status = pd.Series("unmapped", index=pairs.index)
    status[ferc_pudl.notna() & eia_pudl.notna()] = "different PUDL IDs"
    status[ferc_pudl.notna() & (ferc_pudl == eia_pudl)] = "linked"
    return status


def known_pair_keys(label_paths: tuple[Path, ...]) -> set[tuple[int, int]]:
    """Pairs already labelled, removed from review, or listed as false matches."""
    frames = [pd.read_csv(GLUE_DIR / "utility_id_pudl_false_matches.csv")]
    frames += [pd.read_csv(path) for path in label_paths if path.exists()]
    keys: set[tuple[int, int]] = set()
    for frame in frames:
        frame = frame.dropna(subset=PAIR_KEY)
        keys |= set(
            zip(
                frame["utility_id_ferc1"].astype(int),
                frame["utility_id_eia"].astype(int),
                strict=True,
            )
        )
    return keys


def sample_strata(  # noqa: C901
    pool: pd.DataFrame, n_total: int, seed: int, *, top_up: bool = True
) -> pd.DataFrame:
    """Draw the stratified review sample from the candidate pool."""
    chosen: list[pd.DataFrame] = []

    def take(mask_fn, n: int, stratum: str, order_by: str | None = None) -> None:
        taken = pd.concat(chosen)[PAIR_KEY] if chosen else None
        available = pool
        if taken is not None:
            available = pool.merge(taken, on=PAIR_KEY, how="left", indicator=True)
            available = available[available["_merge"] == "left_only"].drop(
                columns="_merge"
            )
        candidates = available[mask_fn(available)]
        if order_by is None:
            picked = candidates.sample(min(n, len(candidates)), random_state=seed)
        else:
            picked = candidates.nlargest(n, order_by)
        logger.info(f"{stratum}: wanted {n}, found {len(picked)}")
        chosen.append(picked.assign(stratum=stratum))

    per_disagreement = round(n_total * FRACTION_DISAGREEMENT / 2)
    take(
        lambda df: df["exact_name_match"] & (df["match_probability"] < 0.9),
        per_disagreement,
        "exact name match, score < 0.9",
    )
    take(
        lambda df: ~df["exact_name_match"] & (df["match_probability"] >= 0.9),
        per_disagreement,
        "score >= 0.9, no exact name match",
    )
    take(
        lambda df: df["crosswalk_status"] == "linked",
        round(n_total * FRACTION_CROSSWALK),
        "linked in crosswalk",
    )
    n_num_bands = len(NUMERIC_YEAR_BANDS) - 1
    per_num_band = n_total * FRACTION_NUMERIC / n_num_bands
    for (low, high), weight in zip(
        zip(NUMERIC_YEAR_BANDS[:-1], NUMERIC_YEAR_BANDS[1:], strict=True),
        NUMERIC_BAND_WEIGHTS,
        strict=True,
    ):
        take(
            lambda df, low=low, high=high: (
                ~df["exact_name_match"]
                & (df["numeric_years_agree"] >= low)
                & (df["numeric_years_agree"] < high)
            ),
            round(per_num_band * weight),
            f"numeric agreement {low}-{high - 1 if high < 1000 else 'plus'} years",
        )
    n_sim_bands = len(SIMILARITY_BANDS) - 1
    per_sim_band = round(n_total * FRACTION_NAME_SIMILARITY / n_sim_bands)
    for low, high in zip(SIMILARITY_BANDS[:-1], SIMILARITY_BANDS[1:], strict=True):
        take(
            lambda df, low=low, high=high: (
                ~df["exact_name_match"]
                & (df["name_similarity"] >= low)
                & (df["name_similarity"] < high)
            ),
            per_sim_band,
            f"name similarity {low:.2f}-{min(high, 1):.2f}",
        )
    n_bands = len(SCORE_BANDS) - 1
    per_band = round(n_total * FRACTION_BANDS / n_bands)
    for low, high in zip(SCORE_BANDS[:-1], SCORE_BANDS[1:], strict=True):
        take(
            lambda df, low=low, high=high: (
                (df["match_probability"] >= low) & (df["match_probability"] < high)
            ),
            per_band,
            f"score {low:g}-{min(high, 1):g}",
        )
    if not top_up:
        return pd.concat(chosen)
    shortfall = n_total - sum(len(df) for df in chosen)
    if shortfall > 0:
        take(
            lambda df: ~df["exact_name_match"] & (df["numeric_years_agree"] >= 1),
            shortfall,
            "numeric agreement top-up (most years first)",
            order_by="numeric_years_agree",
        )
    shortfall = n_total - sum(len(df) for df in chosen)
    if shortfall > 0:
        take(
            lambda df: df["name_similarity"] >= SIMILARITY_BANDS[0],
            shortfall,
            "similar-name top-up (most similar first)",
            order_by="name_similarity",
        )
    shortfall = n_total - sum(len(df) for df in chosen)
    if shortfall > 0:
        take(lambda df: df["match_probability"] >= 0, shortfall, "random top-up")
    return pd.concat(chosen)


def add_review_columns(
    sample: pd.DataFrame, eia_df: pd.DataFrame, ferc_df: pd.DataFrame
) -> pd.DataFrame:
    """Attach each utility's most recent raw name and address for the reviewer."""
    display = {}
    for dataset, df, suffix in (("ferc", ferc_df, "ferc1"), ("eia", eia_df, "eia")):
        latest = (
            df.sort_values("report_year")
            .drop_duplicates(ID_COLS[dataset], keep="last")
            .set_index(ID_COLS[dataset])
        )
        display[suffix] = latest.rename(
            columns={
                "utility_name_raw": f"utility_name_{suffix}",
                "street_address_raw": f"street_{suffix}",
                "city_raw": f"city_{suffix}",
                "state_raw": f"state_{suffix}",
                "zip_raw": f"zip_{suffix}",
                "report_year": f"latest_year_{suffix}",
            }
        )[
            [
                f"utility_name_{suffix}",
                f"street_{suffix}",
                f"city_{suffix}",
                f"state_{suffix}",
                f"zip_{suffix}",
                f"latest_year_{suffix}",
            ]
        ]
    return sample.join(display["ferc1"], on="utility_id_ferc1").join(
        display["eia"], on="utility_id_eia"
    )


def add_numeric_review_columns(
    sample: pd.DataFrame, eia_df: pd.DataFrame, ferc_df: pd.DataFrame
) -> pd.DataFrame:
    """Show both sides' customers and sales in the year the numbers agree.

    Pairs with no agreeing year show the latest year both utilities reported.
    """
    ferc = ferc_df[["utility_id_ferc1", "report_year", "customers", "sales_mwh"]]
    eia = eia_df[["utility_id_eia", "report_year", "customers", "sales_mwh"]]
    shared = (
        sample[[*PAIR_KEY, "numeric_year"]]
        .merge(ferc, on="utility_id_ferc1")
        .merge(eia, on=["utility_id_eia", "report_year"], suffixes=("_ferc1", "_eia"))
    )
    latest = shared.groupby(PAIR_KEY)["report_year"].transform("max")
    shared = shared[shared["report_year"] == shared["numeric_year"].fillna(latest)]
    shared = shared.rename(columns={"report_year": "numeric_compare_year"}).drop(
        columns="numeric_year"
    )
    return sample.merge(shared, on=PAIR_KEY, how="left")


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--n-pairs",
    type=int,
    default=500,
    show_default=True,
    help="Number of utility pairs to sample for review.",
)
@click.option(
    "--min-probability",
    type=float,
    default=0.01,
    show_default=True,
    help="Lowest record-pair match probability kept from the model.",
)
@click.option(
    "--min-name-similarity",
    type=float,
    default=0.80,
    show_default=True,
    help="Lowest Jaro-Winkler name similarity for the unblocked name-match source.",
)
@click.option("--seed", type=int, default=1234, show_default=True)
@click.option(
    "--known-labels",
    type=click.Path(path_type=Path),
    multiple=True,
    default=[
        DEVTOOLS_DIR / "utility_match_training_data.csv",
        *sorted((DEVTOOLS_DIR / "ferc1-eia-glue").glob("label_candidates*.csv")),
    ],
    show_default=True,
    help="Existing label files whose pairs are skipped (repeat to add more).",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    default=DEVTOOLS_DIR / "ferc1-eia-glue" / "label_candidates.csv",
    show_default=True,
)
@click.option(
    "--top-up/--no-top-up",
    default=True,
    show_default=True,
    help="Pad the sample to --n-pairs with the best remaining pairs. Without it, "
    "only the informative strata are sampled and the file may be smaller.",
)
@click.option(
    "--nightly",
    is_flag=True,
    help="Read inputs from the nightly S3 build instead of $PUDL_OUTPUT.",
)
def main(
    n_pairs: int,
    min_probability: float,
    min_name_similarity: float,
    seed: int,
    known_labels: tuple[Path, ...],
    output: Path,
    top_up: bool,
    nightly: bool,
) -> None:
    """Write a stratified sample of utility pairs for manual true/false labelling."""
    ferc_features, eia_features = load_numeric_features(
        lambda name: load_table(name, nightly)
    )
    eia_df = prepare_records(
        load_table("out_eia__yearly_utilities", nightly), "eia", eia_features
    )
    ferc_df = prepare_records(
        load_table("core_ferc1__yearly_identification_certification", nightly),
        "ferc",
        ferc_features,
    )
    predictions = predict_unsupervised(eia_df, ferc_df, min_probability, seed)
    scored = aggregate_to_utility_pairs(predictions, eia_df, ferc_df)
    logger.info(f"{len(scored)} scored utility pairs.")

    # Exact-name pairs the model never scored still belong in the pool.
    pool = (
        scored.merge(exact_name_pairs(eia_df, ferc_df), on=PAIR_KEY, how="outer")
        .merge(
            similar_name_pairs(eia_df, ferc_df, min_name_similarity),
            on=PAIR_KEY,
            how="outer",
        )
        .merge(numeric_agreement_pairs(eia_df, ferc_df), on=PAIR_KEY, how="outer")
        .assign(
            exact_name_match=lambda df: (
                df["exact_name_match"].fillna(False).astype(bool)
            ),
            numeric_years_agree=lambda df: (
                df["numeric_years_agree"].fillna(0).astype(int)
            ),
            match_probability=lambda df: df["match_probability"].fillna(0.0),
            n_matching_years=lambda df: df["n_matching_years"].fillna(0).astype(int),
        )
        .astype({"utility_id_ferc1": "int64", "utility_id_eia": "int64"})
    )
    # Regenerating must not treat the previous version of the output as known labels.
    known = known_pair_keys(
        tuple(path for path in known_labels if path.resolve() != output.resolve())
    )
    is_known = [
        pair in known
        for pair in zip(pool["utility_id_ferc1"], pool["utility_id_eia"], strict=True)
    ]
    pool = pool[[not flag for flag in is_known]].reset_index(drop=True)
    pool["crosswalk_status"] = crosswalk_status(pool)
    logger.info(f"{len(pool)} candidate pairs after skipping {sum(is_known)} known.")

    sample = sample_strata(pool, n_pairs, seed, top_up=top_up)
    sample = add_review_columns(sample, eia_df, ferc_df)
    sample = add_numeric_review_columns(sample, eia_df, ferc_df)
    sample = sample.assign(is_match="", notes="", numeric_informed="").sample(
        frac=1, random_state=seed
    )
    first = [
        "utility_id_ferc1",
        "utility_name_ferc1",
        "utility_id_eia",
        "utility_name_eia",
        "is_match",
        "notes",
        "numeric_informed",
    ]
    sample = sample[first + [col for col in sample.columns if col not in first]]
    output.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(output, index=False)
    logger.info(f"Wrote {len(sample)} pairs to {output}")
    logger.info(f"\n{sample['stratum'].value_counts().to_string()}")


if __name__ == "__main__":
    main()
