# ruff: noqa: B018, C408, C901, D100, N803, N806
import marimo

__generated_with = "0.25.2"
app = marimo.App(width="full")


@app.cell
def _():
    import os
    import sys
    from pathlib import Path

    import marimo as mo
    import pandas as pd

    # The helper modules live next to this notebook.
    sys.path.insert(0, str(mo.notebook_dir()))
    import crosswalk_candidates as cc
    import crosswalk_store as cs

    CROSSWALK_DIR = Path(
        os.environ.get(
            "UTILITY_CROSSWALK_DIR",
            mo.notebook_dir() / "ferc1-eia-glue" / "utility_crosswalk",
        )
    )
    DECISIONS_PATH = CROSSWALK_DIR / "decisions.csv"
    CROSSWALK_PATH = CROSSWALK_DIR / "utility_crosswalk.csv"
    return CROSSWALK_PATH, DECISIONS_PATH, cc, cs, mo, pd


@app.cell
def _(cc, mo):
    _local_missing = cc.missing_local_tables()
    # Default to local data when every required table is there, otherwise to S3.
    use_s3 = mo.ui.switch(
        value=bool(_local_missing),
        label="Data source: **S3 nightly build** (off = local `$PUDL_OUTPUT`)",
    )
    use_s3
    return (use_s3,)


@app.cell(hide_code=True)
def _(CROSSWALK_PATH, DECISIONS_PATH, mo):
    mo.vstack(
        [
            mo.md(r"""
            # FERC Form 1 - EIA utility crosswalk labelling

            For each FERC1 utility, pick the EIA utilities that are the **same entity**
            (zero, one or several), then save. The next utility appears automatically.
            Decisions are written immediately, so you can close the notebook at any time.
            """),
            mo.accordion(
                {
                    "Labelling policy (read before you start)": mo.md(r"""
                    **Match (select it)**

                    - The same legal entity under a **different name** (a rename), including
                      names in the FERC utility's history shown under *all names*.
                    - A **successor**: the entity that took over after a merger or
                      reorganisation and kept the filing history.
                    - A **division or trade name** of the same legal entity (for example a
                      "(SD)" operating division, or a trade name like "We Energies").
                    - A **second EIA ID** for the same utility (duplicate records, an ID per
                      address, an ID per name variant). Select all of them.

                    **Not a match (leave unselected)**

                    - A **parent or holding company**, or a **subsidiary** of the FERC utility.
                    - A **sister company** (same parent, same address, different entity).
                    - A **similar name** that is a different company (another cooperative with a
                      common name, a different state).
                    - Generation, marketing or retail **affiliates**.

                    **If a FERC utility has no EIA record at all**
                    (many transmission LLCs and holding entities), press **No match**.

                    **When unsure**: choose your best call, tick **Uncertain**, and say why in the
                    notes. Use **Skip** to come back to it later.

                    **Blind first, then reveal.** Decide from names, addresses and years
                    first. Switch on **Show model scores and numbers** only if you cannot decide;
                    saving with it on marks the label `numeric_informed`, so it can be excluded
                    when evaluating models that use the same numbers.

                    **Can't see the match?** Use **Search EIA utilities** below the candidates.
                    The candidate list is generated automatically and can miss a renamed entity.
                    """)
                }
            ),
            mo.md(
                f"Writing to `{DECISIONS_PATH}` (decisions) and `{CROSSWALK_PATH}` "
                f"(crosswalk). Set `UTILITY_CROSSWALK_DIR` to change the location."
            ),
        ]
    )
    return


@app.cell
def _(cc, mo, use_s3):
    source = "s3" if use_s3.value else "local"
    _missing = cc.missing_local_tables() if source == "local" else []
    mo.stop(
        bool(_missing),
        mo.md(
            "**Local data is not available**: these tables are missing from "
            f"`$PUDL_OUTPUT/parquet`: {', '.join(f'`{t}`' for t in _missing)}. "
            "Switch the data source to S3 above, or materialise them locally."
        ).callout(kind="danger"),
    )
    mo.stop(
        cc.cache_exists(source),
        mo.md(
            f"Using the {source} candidate cache in `{cc.cache_dir(source)}`."
        ).callout(kind="success"),
    )
    build_button = mo.ui.run_button(
        label=f"Build {source} candidate cache (a few minutes)"
    )
    mo.vstack(
        [
            mo.md(
                f"**No {source} candidate cache yet** in `{cc.cache_dir(source)}`. It is "
                "built once from name, address, size and an unsupervised Splink model (no "
                "labels), and reused afterwards."
            ).callout(kind="warn"),
            build_button,
        ]
    )
    return build_button, source


@app.cell
def _(build_button, cc, mo, source):
    mo.stop(cc.cache_exists(source))
    mo.stop(not build_button.value)
    with mo.status.spinner(title="Building candidates (about 3 minutes)"):
        cc.build_cache(nightly=source == "s3")
    mo.md("Cache built. Re-run this notebook (or refresh) to start labelling.")
    return


@app.cell
def _(cc, mo, pd, source):
    mo.stop(not cc.cache_exists(source), mo.md("Waiting for the candidate cache."))
    _dir = cc.cache_dir(source)
    candidates = pd.read_parquet(_dir / "candidates.parquet")
    ferc_info = pd.read_parquet(_dir / "ferc_info.parquet").set_index(
        "utility_id_ferc1"
    )
    eia_info = pd.read_parquet(_dir / "eia_info.parquet").set_index("utility_id_eia")
    ferc_names = ferc_info["name_ferc1"].astype(str)
    eia_names = eia_info["name_eia"].astype(str)
    SOURCE_LABELS = {
        "name_exact": "exact name",
        "name_jw": "similar name",
        "name_token": "shared name words",
        "location": "same address",
        "numeric": "similar size",
        "model": "statistical model",
    }
    return (
        SOURCE_LABELS,
        candidates,
        eia_info,
        eia_names,
        ferc_info,
        ferc_names,
    )


@app.cell
def _(DECISIONS_PATH, cs, mo):
    get_decisions, set_decisions = mo.state(cs.load_decisions(DECISIONS_PATH))
    return get_decisions, set_decisions


@app.cell
def _(mo):
    order_select = mo.ui.dropdown(
        {"Largest first (customers)": "size", "FERC ID": "id"},
        value="Largest first (customers)",
        label="Review order",
    )
    include_skipped = mo.ui.switch(value=True, label="Revisit skipped at the end")
    mo.hstack([order_select, include_skipped], justify="start")
    return include_skipped, order_select


@app.cell
def _(cs, ferc_info, get_decisions, include_skipped, mo, order_select):
    _info = ferc_info.reset_index()
    if order_select.value == "size":
        _info = _info.sort_values(
            ["max_customers", "utility_id_ferc1"],
            ascending=[False, True],
            na_position="last",
        )
    else:
        _info = _info.sort_values("utility_id_ferc1")
    _all_ids = [int(i) for i in _info["utility_id_ferc1"]]
    decisions = get_decisions()
    queue = cs.pending_queue(_all_ids, decisions, include_skipped=include_skipped.value)
    current = queue[0] if queue else None
    _counts = decisions["outcome"].value_counts()
    mo.md(
        f"**{len(_all_ids) - len(queue)} of {len(_all_ids)} done** &nbsp;|&nbsp; "
        f"matched {int(_counts.get('matched', 0))}, no match "
        f"{int(_counts.get('no_match', 0))}, skipped {int(_counts.get('skipped', 0))}, "
        f"{len(queue)} left in the queue"
    )
    return current, decisions, queue


@app.cell
def _(current, ferc_info, mo, pd):
    mo.stop(
        current is None,
        mo.md("### All FERC utilities have been decided.").callout(kind="success"),
    )
    _f = ferc_info.loc[current]

    def _fmt(value: float) -> str:
        return "-" if pd.isna(value) else f"{value:,.0f}"

    mo.md(
        f"""
        ## FERC {current}: {_f["name_ferc1"]}

        - **All names:** {_f["all_names_ferc1"]}
        - **Address:** {_f["street_ferc1"]}, {_f["city_ferc1"]} {_f["state_ferc1"]} {_f["zip_ferc1"]}
        - **Active in FERC data:** {int(_f["first_year_ferc1"])}-{int(_f["last_year_ferc1"])}
        """
    )
    return


@app.cell
def _(SOURCE_LABELS, candidates, current, eia_info, mo, pd):
    mo.stop(current is None)

    def eia_table(frame: pd.DataFrame) -> pd.DataFrame:
        """Display columns for a set of EIA utilities (indexed by utility_id_eia)."""
        return pd.DataFrame(
            {
                "utility_id_eia": frame.index,
                "name": frame["name_eia"].to_numpy(),
                "all names": frame["all_names_eia"].to_numpy(),
                "address": (
                    frame["street_eia"].fillna("")
                    + ", "
                    + frame["city_eia"].fillna("")
                    + " "
                    + frame["state_eia"].fillna("")
                    + " "
                    + frame["zip_eia"].fillna("")
                ).to_numpy(),
                "years": (
                    frame["first_year_eia"].astype(int).astype(str)
                    + "-"
                    + frame["last_year_eia"].astype(int).astype(str)
                ).to_numpy(),
            }
        )

    _c = candidates[candidates["utility_id_ferc1"] == current].copy()
    _c = _c.sort_values(
        ["n_sources", "name_exact", "utility_id_eia"], ascending=[False, False, True]
    )
    _shown = eia_table(eia_info.loc[_c["utility_id_eia"]])

    def _found_by(src: str, location_match: object) -> str:
        labels = []
        for source in str(src).split(","):
            if source == "location":
                labels.append(
                    "same street + zip"
                    if location_match == "street+zip"
                    else "same zip + a shared name word"
                )
            elif source in SOURCE_LABELS:
                labels.append(SOURCE_LABELS[source])
        return ", ".join(labels)

    _shown["found by"] = [
        _found_by(src, loc)
        for src, loc in zip(_c["sources"], _c["location_match"], strict=True)
    ]
    candidate_table = mo.ui.table(
        _shown.reset_index(drop=True),
        selection="multi",
        page_size=15,
        show_download=False,
        label=f"Candidate EIA utilities for FERC {current} (select every true match)",
    )
    candidate_table
    return candidate_table, eia_table


@app.cell
def _(current, mo):
    mo.stop(current is None)
    search_box = mo.ui.text(
        placeholder="part of an EIA utility name, press Enter",
        label="Search EIA utilities",
        full_width=True,
    )
    search_box
    return (search_box,)


@app.cell
def _(eia_info, eia_table, mo, pd, search_box):
    _q = search_box.value.strip().lower()
    _hits = (
        eia_info[eia_info["all_names_eia"].str.lower().str.contains(_q, regex=False)]
        if _q
        else eia_info.iloc[0:0]
    )
    search_table = mo.ui.table(
        eia_table(_hits.head(40)) if len(_hits) else pd.DataFrame(),
        selection="multi",
        page_size=10,
        show_download=False,
        label=f"{len(_hits)} search results"
        + (" (showing 40)" if len(_hits) > 40 else ""),
    )
    search_table
    return (search_table,)


@app.cell
def _(current, mo):
    mo.stop(current is None)
    reveal = mo.ui.switch(label="Show model scores and numbers")
    uncertain = mo.ui.checkbox(label="Uncertain")
    notes = mo.ui.text_area(placeholder="notes (why uncertain, what you found)", rows=2)
    mo.hstack([reveal, uncertain, notes], justify="start", align="start")
    return notes, reveal, uncertain


@app.cell
def _(candidates, current, eia_info, ferc_info, mo, pd, reveal):
    mo.stop(current is None or not reveal.value)
    _f = ferc_info.loc[current]

    def _fmt(value: float) -> str:
        return "-" if pd.isna(value) else f"{value:,.0f}"

    _c = candidates[candidates["utility_id_ferc1"] == current].copy()
    _c["name_eia"] = eia_info.loc[_c["utility_id_eia"], "name_eia"].to_numpy()
    _c["model probability"] = _c[["model_numeric", "model_name_address"]].max(axis=1)
    _c["max customers (EIA)"] = eia_info.loc[
        _c["utility_id_eia"], "max_customers"
    ].to_numpy()
    _c["max sales MWh (EIA)"] = eia_info.loc[
        _c["utility_id_eia"], "max_sales_mwh"
    ].to_numpy()
    mo.vstack(
        [
            mo.md(
                f"""
                ### FERC {current} values

                - **Max customers:** {_fmt(_f["max_customers"])}
                - **Max sales (MWh):** {_fmt(_f["max_sales_mwh"])}

                `numeric_years_agree` below is the number of years in which customers
                (within 2%) and sales (within 3%) agree with this FERC utility.
                """
            ),
            mo.ui.table(
                _c[
                    [
                        "utility_id_eia",
                        "name_eia",
                        "model probability",
                        "numeric_years_agree",
                        "max customers (EIA)",
                        "max sales MWh (EIA)",
                    ]
                ].sort_values("model probability", ascending=False),
                selection=None,
                show_download=False,
            ),
        ]
    )
    return


@app.cell
def _(
    CROSSWALK_PATH,
    DECISIONS_PATH,
    candidate_table,
    cs,
    current,
    eia_names,
    ferc_names,
    get_decisions,
    mo,
    notes,
    reveal,
    search_table,
    set_decisions,
    uncertain,
):
    mo.stop(current is None)

    def _selected_ids() -> list[int]:
        _ids = set()
        for _table in (candidate_table, search_table):
            _value = _table.value
            if _value is not None and len(_value):
                _ids |= {int(i) for i in _value["utility_id_eia"]}
        return sorted(_ids)

    def _commit(outcome: str, eia_ids: list[int] | None = None) -> None:
        _new = cs.record_decision(
            get_decisions(),
            current,
            outcome,
            eia_ids,
            uncertain=uncertain.value,
            numeric_informed=bool(reveal.value) and outcome == "matched",
            notes=notes.value,
        )
        cs.save(_new, DECISIONS_PATH, CROSSWALK_PATH, ferc_names, eia_names)
        set_decisions(_new)

    def _save_match(_) -> None:
        _ids = _selected_ids()
        if _ids:
            _commit("matched", _ids)

    def _undo(_) -> None:
        _new, _ = cs.undo_last(get_decisions())
        cs.save(_new, DECISIONS_PATH, CROSSWALK_PATH, ferc_names, eia_names)
        set_decisions(_new)

    save_button = mo.ui.button(
        label="Save selected as match", kind="success", on_click=_save_match
    )
    no_match_button = mo.ui.button(
        label="No match", kind="danger", on_click=lambda _: _commit("no_match")
    )
    skip_button = mo.ui.button(
        label="Skip", kind="neutral", on_click=lambda _: _commit("skipped")
    )
    undo_button = mo.ui.button(label="Undo last decision", on_click=_undo)
    _n = len(_selected_ids())
    mo.vstack(
        [
            mo.md(
                f"{_n} EIA utilit{'y' if _n == 1 else 'ies'} selected "
                "(candidates and search results combined)."
            ),
            mo.hstack(
                [save_button, no_match_button, skip_button, undo_button],
                justify="start",
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(CROSSWALK_PATH, cs, decisions, eia_names, ferc_names, mo):
    _crosswalk = cs.build_crosswalk(decisions, ferc_names, eia_names)
    mo.accordion(
        {
            f"Crosswalk so far ({len(_crosswalk)} pairs, saved to {CROSSWALK_PATH.name})": (
                mo.ui.table(
                    _crosswalk, selection=None, page_size=10, show_download=True
                )
            )
        }
    )
    return


if __name__ == "__main__":
    app.run()
