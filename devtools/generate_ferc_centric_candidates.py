"""Generate FERC1-centric candidate pairs for building an unbiased utility label set.

Labelling is organised around a random sample of FERC Form 1 utilities. For every
sampled utility, a reviewer decides which EIA utilities (zero, one or several) are the
same entity. Every EIA utility that is not marked as a match is then a true negative
for that FERC utility, so precision and recall of any matching method can be measured
without the selection bias that comes from labelling pairs that one generator happens
to surface.

This script draws the sample and produces the candidate EIA utilities to review.

Sample
------
``--n-ferc`` utilities are drawn at random with a fixed seed. Utilities that report the
numeric features (customers and sales, see ``utility_match_features.py``) form one
stratum and the others a second one; at least ``--min-numeric-covered`` come from the
first so that numeric methods can be evaluated. Each row carries its stratum and
inclusion weight for population estimates. A fixed 70/30 train/test split is assigned
by hashing the FERC utility ID, so it never changes as labels are added.

To add more labels, run it again with ``--exclude-sample`` pointing at the earlier
``ferc_sample.csv`` files (and the same ``--split-seed``), a new ``--seed`` and
``--output-dir``. Utilities already sampled are skipped, and ``stratum_weight`` then
describes the combined sample (population size of the stratum divided by the number of
utilities sampled in it over all batches). Weights in the earlier files go stale, so
recompute them from the combined samples.

Candidate sources (the union is reviewed; all use every year of EIA data)
-------------------------------------------------------------------------
- ``name_exact``: identical cleaned name.
- ``name_jw``: top Jaro-Winkler matches on cleaned names (>= ``--min-name-similarity``).
- ``name_token``: top token-set (Jaccard) matches after expanding a few common
  abbreviations (see ``ABBREVIATIONS``).
- ``location``: same normalised street address and zip, or same zip plus a shared
  distinctive name token.
- ``numeric``: customers within 2% and sales within 3% in the same year, ranked by
  the number of agreeing years.
- ``model_numeric`` / ``model_name_address``: record pairs scored at or above
  ``--min-model-probability`` by unsupervised Splink models (trained with expectation
  maximisation, with no labels), with and without the numeric features. Optional
  (``--no-model``), because training takes a few minutes.

Existing labels and the crosswalk are never used to generate candidates.

Selection per FERC utility: keep every candidate found by two or more sources, by the
exact-name or model sources, or ranked in the top ``--top-per-source`` of any ranked
source; then drop the weakest single-source candidates beyond ``--max-candidates``.

Outputs (written to ``--output-dir``)
-------------------------------------
``ferc_sample.csv``
    One row per sampled FERC utility, with blank ``has_true_match`` /
    ``other_true_eia_ids`` / ``notes`` columns to fill in. Use ``other_true_eia_ids``
    to record a true match that no source surfaced (this measures candidate recall).
``candidates.csv``
    One row per candidate pair with names, addresses and years, and blank ``is_match``,
    ``notes`` and ``numeric_informed`` columns. It deliberately omits model scores,
    source flags and the numeric values, so a first review pass is blind to them.
``candidates_reveal.csv``
    The same pairs keyed by ``utility_id_ferc1`` / ``utility_id_eia`` with source
    flags, ranks, model probabilities and numeric values, for a second pass over
    unresolved pairs. Mark labels that depended on it with ``numeric_informed``.

Usage (from the repo root, with the ``dev`` pixi environment)::

    pixi run -e dev python devtools/generate_ferc_centric_candidates.py --nightly
    pixi run -e dev python devtools/generate_ferc_centric_candidates.py --nightly \
        --no-model --n-ferc 40 --output-dir /tmp/candidates
"""

import hashlib
import re
from pathlib import Path

import click
import duckdb
import pandas as pd
from generate_label_candidates import (
    PAIR_KEY,
    aggregate_to_utility_pairs,
    exact_name_pairs,
    load_table,
    numeric_agreement_pairs,
    predict_unsupervised,
    prepare_records,
    similar_name_pairs,
)
from utility_match_features import load_numeric_features

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

DEVTOOLS_DIR = Path(__file__).parent

# Expanded before token comparison. Both sides are expanded the same way.
ABBREVIATIONS = {
    "elec": "electric",
    "coop": "cooperative",
    "pub": "public",
    "serv": "service",
    "svc": "service",
    "assn": "association",
    "assoc": "association",
    "util": "utility",
    "utils": "utilities",
    "pwr": "power",
    "engy": "energy",
    "cnty": "county",
    "cty": "county",
    "membership": "member",
    "mem": "member",
}
STOPWORD_TOKENS = {"of", "the", "and", "for", "a", "an"}
# Too common to count as evidence of a shared name on their own.
GENERIC_TOKENS = {
    "electric",
    "power",
    "energy",
    "light",
    "utility",
    "utilities",
    "service",
    "public",
    "cooperative",
    "association",
    "member",
    "water",
    "gas",
    "city",
    "county",
    "rural",
    "generation",
    "transmission",
    "holdings",
    "systems",
}
STREET_SUFFIXES = {
    "street": "st",
    "avenue": "ave",
    "road": "rd",
    "boulevard": "blvd",
    "drive": "dr",
    "highway": "hwy",
    "parkway": "pkwy",
    "place": "pl",
    "north": "n",
    "south": "s",
    "east": "e",
    "west": "w",
}
RANKED_SOURCES = ("name_jw", "name_token", "location", "numeric")
TRAIN_PERCENT = 70


def tokenize(name: str) -> list[str]:
    """Split a cleaned name into sorted unique tokens, expanding abbreviations."""
    tokens: set[str] = set()
    for word in re.findall(r"[a-z0-9]+", str(name).lower()):
        for token in ABBREVIATIONS.get(word, word).split():
            if token not in STOPWORD_TOKENS:
                tokens.add(token)
    return sorted(tokens)


def normalise_street(street: object) -> str | None:
    """Lowercase a street address and abbreviate common words, or return None."""
    if pd.isna(street):
        return None
    text = re.sub(r"\b(suite|ste|floor|fl|room|rm)\b.*$", "", str(street).lower())
    words = re.findall(r"[a-z0-9]+", text)
    text = " ".join(STREET_SUFFIXES.get(word, word) for word in words)
    return text if len(text) >= 6 else None


def split_for(ferc_id: int, seed: int) -> str:
    """Assign a utility to ``train`` or ``test`` with a stable hash of its ID."""
    digest = hashlib.md5(f"{seed}-{ferc_id}".encode()).hexdigest()  # noqa: S324
    return "train" if int(digest, 16) % 100 < TRAIN_PERCENT else "test"


def draw_sample(
    ferc_df: pd.DataFrame,
    n_ferc: int,
    min_numeric_covered: int,
    seed: int,
    split_seed: int,
    previous: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Draw a stratified random sample of FERC utilities (numeric covered or not).

    Args:
        ferc_df: Prepared FERC records.
        n_ferc: Number of utilities to draw.
        min_numeric_covered: Minimum number to draw from the numeric-covered stratum.
        seed: Seed for the random draw.
        split_seed: Seed for the train/test hash (keep it fixed across batches).
        previous: Earlier samples (``ferc_sample.csv`` rows) whose utilities are
            excluded. Weights then describe the combined sample: the stratum's
            population size divided by the number sampled in all batches so far.
    """
    utilities = ferc_df.groupby("utility_id_ferc1", as_index=False).agg(
        numeric_covered=("customers", lambda col: bool(col.notna().any()))
    )
    utilities["stratum"] = utilities["numeric_covered"].map(
        {True: "numeric covered", False: "no numeric features"}
    )
    population = utilities["stratum"].value_counts()
    previous_counts = pd.Series(dtype="int64")
    if previous is not None:
        previous_counts = previous["stratum"].value_counts()
        utilities = utilities[
            ~utilities["utility_id_ferc1"].isin(previous["utility_id_ferc1"])
        ]
    available = utilities["stratum"].value_counts()
    n_covered_available = int(available.get("numeric covered", 0))
    n_covered = min(
        n_covered_available,
        max(min_numeric_covered, round(n_ferc * n_covered_available / len(utilities))),
    )
    n_other = min(n_ferc - n_covered, len(utilities) - n_covered_available)
    pieces = []
    for stratum_name, n_draw in (
        ("numeric covered", n_covered),
        ("no numeric features", n_other),
    ):
        stratum = utilities[utilities["stratum"] == stratum_name]
        n_total = n_draw + int(previous_counts.get(stratum_name, 0))
        picked = stratum.sample(n_draw, random_state=seed).assign(
            stratum_weight=int(population[stratum_name]) / max(n_total, 1)
        )
        pieces.append(picked)
    sample = pd.concat(pieces).sort_values("utility_id_ferc1").reset_index(drop=True)
    sample["split"] = [split_for(i, split_seed) for i in sample["utility_id_ferc1"]]
    return sample.drop(columns="numeric_covered")


def token_name_pairs(
    eia_df: pd.DataFrame, ferc_df: pd.DataFrame, top_k: int, min_jaccard: float = 0.34
) -> pd.DataFrame:
    """Top-``top_k`` EIA utilities per FERC utility by token-set Jaccard similarity."""

    def names(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
        out = df.dropna(subset=["utility_name"])[[id_col, "utility_name"]]
        out = out.drop_duplicates().astype({id_col: "int64"})
        out["tokens"] = out["utility_name"].map(tokenize)
        return out[out["tokens"].map(len) > 0][[id_col, "tokens"]]

    ferc_names = names(ferc_df, "utility_id_ferc1")
    eia_names = names(eia_df, "utility_id_eia")
    con = duckdb.connect()
    con.register("ferc_names", ferc_names)
    con.register("eia_names", eia_names)
    pairs = con.sql(
        """
        SELECT utility_id_ferc1, utility_id_eia, max(jaccard) AS token_similarity
        FROM (
            SELECT f.utility_id_ferc1, e.utility_id_eia,
                   len(list_intersect(f.tokens, e.tokens)) * 1.0
                   / len(list_distinct(list_concat(f.tokens, e.tokens))) AS jaccard
            FROM ferc_names f CROSS JOIN eia_names e
        )
        WHERE jaccard >= $min_jaccard
        GROUP BY utility_id_ferc1, utility_id_eia
        """,
        params={"min_jaccard": min_jaccard},
    ).df()
    return rank_per_ferc(pairs, "token_similarity", top_k, "name_token_rank")


def rank_per_ferc(
    pairs: pd.DataFrame, score_col: str, top_k: int, rank_col: str
) -> pd.DataFrame:
    """Rank EIA candidates within each FERC utility (1 = best) and keep the top k."""
    pairs = pairs.sort_values(
        ["utility_id_ferc1", score_col, "utility_id_eia"],
        ascending=[True, False, True],
    )
    pairs[rank_col] = pairs.groupby("utility_id_ferc1").cumcount() + 1
    return pairs[pairs[rank_col] <= top_k]


def location_pairs(eia_df: pd.DataFrame, ferc_df: pd.DataFrame) -> pd.DataFrame:
    """Pairs sharing a street address and zip, or a zip and a distinctive name token."""

    def places(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
        out = pd.DataFrame(
            {
                id_col: df[id_col].astype("int64"),
                "zip5": df["zip"].astype("string").str.extract(r"(\d{5})")[0],
                "street": df["street_address_raw"].map(normalise_street),
                "tokens": df["utility_name"].map(
                    lambda n: [t for t in tokenize(n) if t not in GENERIC_TOKENS]
                ),
            }
        )
        return out.dropna(subset=["zip5"]).drop_duplicates([id_col, "zip5", "street"])

    ferc = places(ferc_df, "utility_id_ferc1")
    eia = places(eia_df, "utility_id_eia")
    merged = ferc.merge(eia, on="zip5", suffixes=("_ferc1", "_eia"))
    same_street = (
        (
            merged["street_ferc1"].notna()
            & (merged["street_ferc1"] == merged["street_eia"])
        )
        .fillna(False)
        .astype(bool)
    )
    shared_token = [
        bool(set(a) & set(b))
        for a, b in zip(merged["tokens_ferc1"], merged["tokens_eia"], strict=True)
    ]
    merged["location_match"] = [
        "street+zip" if street else "zip+name token" for street in same_street
    ]
    merged = merged[same_street | pd.Series(shared_token, index=merged.index)]
    # A shared street address is strong evidence; zip plus a name token only counts
    # as one of several sources.
    merged["location_rank"] = (merged["location_match"] != "street+zip").map(
        {False: 1, True: 10}
    )
    return (
        merged.sort_values("location_rank")
        .drop_duplicates(PAIR_KEY)[[*PAIR_KEY, "location_match", "location_rank"]]
        .reset_index(drop=True)
    )


def model_pairs(
    eia_df: pd.DataFrame,
    ferc_df: pd.DataFrame,
    ferc_ids: set[int],
    min_probability: float,
    seed: int,
) -> pd.DataFrame:
    """Utility pairs scored by two unsupervised Splink models, for the sample."""
    out = None
    for column, numeric in (("model_numeric", True), ("model_name_address", False)):
        predictions = predict_unsupervised(
            eia_df, ferc_df, min_probability, seed, numeric=numeric
        )
        scored = aggregate_to_utility_pairs(predictions, eia_df, ferc_df)
        scored = scored[scored["utility_id_ferc1"].isin(ferc_ids)][
            [*PAIR_KEY, "match_probability"]
        ].rename(columns={"match_probability": column})
        out = scored if out is None else out.merge(scored, on=PAIR_KEY, how="outer")
    return out


def collect_candidates(
    sources: dict[str, pd.DataFrame], top_per_source: int, max_candidates: int
) -> pd.DataFrame:
    """Combine the sources, flag which found each pair, and select what to review."""
    merged = None
    for frame in sources.values():
        frame = frame.astype({"utility_id_ferc1": "int64", "utility_id_eia": "int64"})
        merged = (
            frame if merged is None else merged.merge(frame, on=PAIR_KEY, how="outer")
        )
    for col in ("model_numeric", "model_name_address"):
        if col not in merged:
            merged[col] = float("nan")
    merged["name_exact"] = merged["exact_name_match"].fillna(False).astype(bool)
    flags = pd.DataFrame(index=merged.index)
    flags["name_exact"] = merged["name_exact"]
    for source in RANKED_SOURCES:
        flags[source] = merged[f"{source}_rank"].notna()
    flags["model"] = (
        merged[["model_numeric", "model_name_address"]].max(axis=1).fillna(0) > 0
    )
    merged["n_sources"] = flags.sum(axis=1)
    merged["sources"] = flags.apply(
        lambda row: ",".join(name for name, hit in row.items() if hit), axis=1
    )
    ranks = merged[[f"{s}_rank" for s in RANKED_SOURCES]]
    merged["best_rank"] = ranks.min(axis=1).fillna(1e6)
    protected = (merged["n_sources"] >= 2) | merged["name_exact"] | flags["model"]
    merged["protected"] = protected
    keep = protected | (merged["best_rank"] <= top_per_source)
    merged = merged[keep].sort_values(
        ["utility_id_ferc1", "protected", "n_sources", "best_rank"],
        ascending=[True, False, False, True],
    )
    merged["candidate_rank"] = merged.groupby("utility_id_ferc1").cumcount() + 1
    return merged[(merged["candidate_rank"] <= max_candidates) | merged["protected"]]


def latest_info(df: pd.DataFrame, id_col: str, suffix: str) -> pd.DataFrame:
    """Names, latest raw address and years active for each utility."""
    df = df.sort_values("report_year")
    grouped = df.groupby(id_col)
    info = pd.DataFrame(
        {
            f"all_names_{suffix}": grouped["utility_name_raw"].agg(
                lambda col: "; ".join(dict.fromkeys(col.dropna().astype(str)))[:160]
            ),
            f"first_year_{suffix}": grouped["report_year"].min(),
            f"last_year_{suffix}": grouped["report_year"].max(),
        }
    )
    latest = grouped.tail(1).set_index(id_col)
    for raw, name in (
        ("utility_name_raw", f"name_{suffix}"),
        ("street_address_raw", f"street_{suffix}"),
        ("city_raw", f"city_{suffix}"),
        ("state_raw", f"state_{suffix}"),
        ("zip_raw", f"zip_{suffix}"),
    ):
        info[name] = latest[raw]
    return info


def numeric_values(
    pairs: pd.DataFrame, eia_df: pd.DataFrame, ferc_df: pd.DataFrame
) -> pd.DataFrame:
    """Both sides' customers and sales in the year used to compare each pair."""
    ferc = ferc_df[["utility_id_ferc1", "report_year", "customers", "sales_mwh"]]
    eia = eia_df[["utility_id_eia", "report_year", "customers", "sales_mwh"]]
    shared = (
        pairs[[*PAIR_KEY, "numeric_year"]]
        .merge(ferc, on="utility_id_ferc1")
        .merge(eia, on=["utility_id_eia", "report_year"], suffixes=("_ferc1", "_eia"))
        .dropna(subset=["customers_ferc1", "customers_eia"])
    )
    latest = shared.groupby(PAIR_KEY)["report_year"].transform("max")
    shared = shared[shared["report_year"] == shared["numeric_year"].fillna(latest)]
    return shared.rename(columns={"report_year": "numeric_compare_year"}).drop(
        columns="numeric_year"
    )


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--n-ferc", type=int, default=150, show_default=True)
@click.option("--min-numeric-covered", type=int, default=75, show_default=True)
@click.option("--seed", type=int, default=1234, show_default=True)
@click.option(
    "--split-seed",
    type=int,
    default=1234,
    show_default=True,
    help="Seed for the train/test hash. Keep it the same for every batch.",
)
@click.option(
    "--exclude-sample",
    type=click.Path(exists=True, path_type=Path),
    multiple=True,
    help="ferc_sample.csv from an earlier batch: its utilities are not drawn again "
    "and the stratum weights describe the combined sample.",
)
@click.option("--top-per-source", type=int, default=3, show_default=True)
@click.option("--max-candidates", type=int, default=10, show_default=True)
@click.option("--min-name-similarity", type=float, default=0.80, show_default=True)
@click.option("--min-model-probability", type=float, default=0.5, show_default=True)
@click.option(
    "--model/--no-model",
    default=True,
    show_default=True,
    help="Train the unsupervised Splink models and use their predictions.",
)
@click.option(
    "--report-crosswalk-recall",
    is_flag=True,
    help="Diagnostic only: log how many crosswalk partners the candidates contain. "
    "The crosswalk is never used to generate candidates.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=DEVTOOLS_DIR / "ferc1-eia-glue" / "fresh_labels",
    show_default=True,
)
@click.option(
    "--nightly",
    is_flag=True,
    help="Read inputs from the nightly S3 build instead of $PUDL_OUTPUT.",
)
def main(  # noqa: PLR0913, PLR0915
    n_ferc: int,
    min_numeric_covered: int,
    seed: int,
    split_seed: int,
    exclude_sample: tuple[Path, ...],
    top_per_source: int,
    max_candidates: int,
    min_name_similarity: float,
    min_model_probability: float,
    model: bool,
    report_crosswalk_recall: bool,
    output_dir: Path,
    nightly: bool,
) -> None:
    """Sample FERC1 utilities and write EIA candidate pairs to review for each."""
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
    previous = (
        pd.concat([pd.read_csv(path) for path in exclude_sample])
        if exclude_sample
        else None
    )
    sample = draw_sample(
        ferc_df, n_ferc, min_numeric_covered, seed, split_seed, previous
    )
    ferc_ids = set(sample["utility_id_ferc1"])
    ferc_sample_df = ferc_df[ferc_df["utility_id_ferc1"].isin(ferc_ids)]
    logger.info(
        f"Sampled {len(sample)} FERC utilities:\n{sample['stratum'].value_counts()}"
    )

    sources = {
        "name_exact": exact_name_pairs(eia_df, ferc_sample_df),
        "name_jw": rank_per_ferc(
            similar_name_pairs(eia_df, ferc_sample_df, min_name_similarity),
            "name_similarity",
            5,
            "name_jw_rank",
        ),
        "name_token": token_name_pairs(eia_df, ferc_sample_df, 5),
        "location": location_pairs(eia_df, ferc_sample_df),
    }
    numeric = numeric_agreement_pairs(eia_df, ferc_sample_df)
    sources["numeric"] = rank_per_ferc(
        numeric.assign(neg_years=-numeric["numeric_years_agree"]),
        "neg_years",
        5,
        "numeric_rank",
    ).drop(columns="neg_years")
    if model:
        sources["model"] = model_pairs(
            eia_df, ferc_df, ferc_ids, min_model_probability, seed
        )
    for name, frame in sources.items():
        logger.info(f"source {name}: {len(frame)} pairs")

    candidates = collect_candidates(sources, top_per_source, max_candidates)
    for col in ("name_similarity", "token_similarity"):
        if col not in candidates:
            candidates[col] = pd.NA
    logger.info(
        f"{len(candidates)} candidates for {candidates['utility_id_ferc1'].nunique()} "
        f"of {len(sample)} FERC utilities"
    )

    if report_crosswalk_recall:
        glue = pd.read_csv(
            Path(__file__).parents[1] / "src/pudl/package_data/glue/utility_id_pudl.csv"
        )
        ferc_side = glue.dropna(subset=["utility_id_ferc1"])[
            ["utility_id_pudl", "utility_id_ferc1"]
        ]
        eia_side = glue.dropna(subset=["utility_id_eia"])[
            ["utility_id_pudl", "utility_id_eia"]
        ]
        truth = (
            ferc_side.merge(eia_side, on="utility_id_pudl")[PAIR_KEY]
            .astype("int64")
            .drop_duplicates()
        )
        truth = truth[
            truth["utility_id_ferc1"].isin(ferc_ids)
            & truth["utility_id_eia"].isin(eia_df["utility_id_eia"])
        ]
        found = len(truth.merge(candidates[PAIR_KEY], on=PAIR_KEY))
        logger.info(
            f"Diagnostic: {found} of {len(truth)} crosswalk pairs for the sampled "
            f"utilities are in the candidates ({found / max(len(truth), 1):.0%})"
        )

    ferc_info = latest_info(ferc_df, "utility_id_ferc1", "ferc1")
    eia_info = latest_info(eia_df, "utility_id_eia", "eia")
    review = candidates.join(ferc_info, on="utility_id_ferc1").join(
        eia_info, on="utility_id_eia"
    )
    review = review.merge(
        sample[["utility_id_ferc1", "split"]], on="utility_id_ferc1", how="left"
    )
    review = review.assign(is_match="", notes="", numeric_informed="")
    blind_cols = [
        "utility_id_ferc1",
        "name_ferc1",
        "utility_id_eia",
        "name_eia",
        "is_match",
        "notes",
        "numeric_informed",
        "split",
        "all_names_ferc1",
        "all_names_eia",
        "street_ferc1",
        "city_ferc1",
        "state_ferc1",
        "zip_ferc1",
        "first_year_ferc1",
        "last_year_ferc1",
        "street_eia",
        "city_eia",
        "state_eia",
        "zip_eia",
        "first_year_eia",
        "last_year_eia",
        "name_similarity",
    ]
    reveal = review.merge(
        numeric_values(review, eia_df, ferc_df),
        on=PAIR_KEY,
        how="left",
    )
    reveal_cols = [
        *PAIR_KEY,
        "sources",
        "n_sources",
        "candidate_rank",
        "name_exact",
        "name_jw_rank",
        "name_token_rank",
        "token_similarity",
        "location_match",
        "location_rank",
        "numeric_rank",
        "numeric_years_agree",
        "model_numeric",
        "model_name_address",
        "numeric_compare_year",
        "customers_ferc1",
        "customers_eia",
        "sales_mwh_ferc1",
        "sales_mwh_eia",
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    review = review.sort_values(["utility_id_ferc1", "candidate_rank"])
    review[[c for c in blind_cols if c in review]].to_csv(
        output_dir / "candidates.csv", index=False
    )
    reveal.sort_values(["utility_id_ferc1", "candidate_rank"])[
        [c for c in reveal_cols if c in reveal]
    ].to_csv(output_dir / "candidates_reveal.csv", index=False)
    counts = candidates.groupby("utility_id_ferc1").size().rename("n_candidates")
    sample_out = (
        sample.join(ferc_info, on="utility_id_ferc1")
        .join(counts, on="utility_id_ferc1")
        .assign(has_true_match="", other_true_eia_ids="", notes="")
    )
    sample_out["n_candidates"] = sample_out["n_candidates"].fillna(0).astype(int)
    sample_out.to_csv(output_dir / "ferc_sample.csv", index=False)
    logger.info(f"Wrote candidates and sample to {output_dir}")


if __name__ == "__main__":
    main()
