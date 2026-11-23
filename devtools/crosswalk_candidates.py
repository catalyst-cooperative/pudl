"""Build and cache the candidate EIA utilities for every FERC1 utility.

The labelling dashboard (``utility_crosswalk_dashboard.py``) reads this cache. It is
written to ``$PUDL_OUTPUT/ferc1_eia_utility_candidates/<source>/``, where ``<source>`` is
``local`` (parquet files in ``$PUDL_OUTPUT``) or ``s3`` (the nightly build):

- ``candidates.parquet``: one row per (FERC1, EIA) candidate pair, with the sources that
  found it and the unsupervised Splink match probability.
- ``ferc_info.parquet`` / ``eia_info.parquet``: names, addresses, active years and the
  numeric features for every utility, used for display and for the name search.

Candidate sources: exact cleaned name, Jaro-Winkler and token-set name similarity,
shared location, agreement of customers and sales, and an **unsupervised** Splink model
(expectation maximisation, no labels) that uses the numeric features. No existing labels
or crosswalk are used.

Usage::

    pixi run -e dev python devtools/crosswalk_candidates.py            # local data
    pixi run -e dev python devtools/crosswalk_candidates.py --nightly  # S3 nightly
"""

import os
from pathlib import Path

import click
import pandas as pd
from generate_ferc_centric_candidates import (
    collect_candidates,
    latest_info,
    location_pairs,
    model_pairs,
    rank_per_ferc,
    token_name_pairs,
)
from generate_label_candidates import (
    exact_name_pairs,
    load_table,
    numeric_agreement_pairs,
    prepare_records,
    similar_name_pairs,
)
from utility_match_features import load_numeric_features

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

CACHE_DIRNAME = "ferc1_eia_utility_candidates"


SOURCES = ("local", "s3")
REQUIRED_TABLES = (
    "core_ferc1__yearly_identification_certification",
    "core_ferc1__yearly_operating_revenues_sched300",
    "out_eia__yearly_utilities",
    "core_eia861__yearly_sales",
)


def cache_dir(source: str = "s3") -> Path:
    """The candidate cache directory for a data source under ``$PUDL_OUTPUT``.

    Args:
        source: ``"local"`` (parquet files in ``$PUDL_OUTPUT``) or ``"s3"`` (the nightly
            build). The caches are kept apart because the two can differ.
    """
    if source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES}, got {source!r}")
    root = os.environ.get("PUDL_OUTPUT")
    if not root:
        raise RuntimeError("PUDL_OUTPUT is not set; it is where the cache is stored.")
    return Path(root) / CACHE_DIRNAME / source


def cache_exists(source: str = "s3") -> bool:
    """Whether all three cache files exist for ``source``."""
    directory = cache_dir(source)
    return all(
        (directory / name).exists()
        for name in ("candidates.parquet", "ferc_info.parquet", "eia_info.parquet")
    )


def missing_local_tables() -> list[str]:
    """Required tables that are not in ``$PUDL_OUTPUT/parquet`` (empty if all there)."""
    root = os.environ.get("PUDL_OUTPUT")
    if not root:
        return list(REQUIRED_TABLES)
    return [
        name
        for name in REQUIRED_TABLES
        if not (Path(root) / "parquet" / f"{name}.parquet").exists()
    ]


def _info_with_features(df: pd.DataFrame, id_col: str, suffix: str) -> pd.DataFrame:
    info = latest_info(df, id_col, suffix)
    # ``latest_info`` takes the last year's row, whose address is often blank even when
    # earlier years have one. Show the most recent non-blank value of each field.
    ordered = df.sort_values("report_year")
    for raw, name in (
        ("street_address_raw", f"street_{suffix}"),
        ("city_raw", f"city_{suffix}"),
        ("state_raw", f"state_{suffix}"),
        ("zip_raw", f"zip_{suffix}"),
    ):
        cleaned = ordered[raw].where(ordered[raw].astype("string").str.strip() != "")
        info[name] = cleaned.groupby(ordered[id_col]).last().reindex(info.index)
    features = (
        df.sort_values("report_year")
        .groupby(id_col)
        .agg(
            max_customers=("customers", "max"),
            max_sales_mwh=("sales_mwh", "max"),
        )
    )
    return info.join(features)


def _load_records(nightly: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepared (EIA, FERC1) records with the numeric features."""
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
    return eia_df, ferc_df


def build_info(*, nightly: bool) -> None:
    """Rewrite only the display info files (fast; leaves the candidates alone)."""
    eia_df, ferc_df = _load_records(nightly)
    out = cache_dir("s3" if nightly else "local")
    out.mkdir(parents=True, exist_ok=True)
    _info_with_features(ferc_df, "utility_id_ferc1", "ferc1").reset_index().to_parquet(
        out / "ferc_info.parquet", index=False
    )
    _info_with_features(eia_df, "utility_id_eia", "eia").reset_index().to_parquet(
        out / "eia_info.parquet", index=False
    )


def build_cache(
    *,
    nightly: bool,
    model: bool = True,
    top_per_source: int = 3,
    max_candidates: int = 10,
    min_name_similarity: float = 0.80,
    min_model_probability: float = 0.5,
    seed: int = 1234,
) -> Path:
    """Compute candidates for all FERC1 utilities and write the cache files."""
    eia_df, ferc_df = _load_records(nightly)
    ferc_ids = set(ferc_df["utility_id_ferc1"].astype(int))
    sources = {
        "name_exact": exact_name_pairs(eia_df, ferc_df),
        "name_jw": rank_per_ferc(
            similar_name_pairs(eia_df, ferc_df, min_name_similarity),
            "name_similarity",
            5,
            "name_jw_rank",
        ),
        "name_token": token_name_pairs(eia_df, ferc_df, 5),
        "location": location_pairs(eia_df, ferc_df),
    }
    numeric = numeric_agreement_pairs(eia_df, ferc_df)
    sources["numeric"] = rank_per_ferc(
        numeric.assign(neg_years=-numeric["numeric_years_agree"]),
        "neg_years",
        5,
        "numeric_rank",
    ).drop(columns="neg_years")
    if model:
        # Unsupervised: u values by random sampling, m values by expectation
        # maximisation. Uses the numeric comparisons.
        sources["model"] = model_pairs(
            eia_df, ferc_df, ferc_ids, min_model_probability, seed
        )
    for name, frame in sources.items():
        logger.info(f"source {name}: {len(frame)} pairs")
    candidates = collect_candidates(sources, top_per_source, max_candidates)
    keep = [
        "utility_id_ferc1",
        "utility_id_eia",
        "sources",
        "n_sources",
        "name_exact",
        "name_similarity",
        "token_similarity",
        "location_match",
        "numeric_years_agree",
        "model_numeric",
        "model_name_address",
    ]
    for col in keep:
        if col not in candidates:
            candidates[col] = pd.NA
    candidates = candidates[keep].astype(
        {"utility_id_ferc1": "int64", "utility_id_eia": "int64"}
    )
    out = cache_dir("s3" if nightly else "local")
    out.mkdir(parents=True, exist_ok=True)
    candidates.to_parquet(out / "candidates.parquet", index=False)
    _info_with_features(ferc_df, "utility_id_ferc1", "ferc1").reset_index().to_parquet(
        out / "ferc_info.parquet", index=False
    )
    _info_with_features(eia_df, "utility_id_eia", "eia").reset_index().to_parquet(
        out / "eia_info.parquet", index=False
    )
    logger.info(
        f"Wrote {len(candidates)} candidates for "
        f"{candidates['utility_id_ferc1'].nunique()} of {len(ferc_ids)} FERC utilities "
        f"to {out}"
    )
    return out


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--nightly",
    is_flag=True,
    help="Read inputs from the nightly S3 build instead of $PUDL_OUTPUT.",
)
@click.option("--model/--no-model", default=True, show_default=True)
@click.option("--seed", type=int, default=1234, show_default=True)
@click.option(
    "--info-only",
    is_flag=True,
    help="Only rewrite the names/addresses used for display (fast).",
)
def main(nightly: bool, model: bool, seed: int, info_only: bool) -> None:
    """Build the candidate cache used by the crosswalk labelling dashboard."""
    if info_only:
        build_info(nightly=nightly)
        return
    build_cache(nightly=nightly, model=model, seed=seed)


if __name__ == "__main__":
    main()
