"""Unit tests for pudl.analysis.operational_characteristics.

These tests build small synthetic hourly EPA CEMS frames in memory rather than
reading real CEMS data, so each one runs in a fraction of a second. They're
aimed at the module's core, easy-to-silently-break logic: quarter-window
arithmetic, run-length detection, load-factor binning, and the ramp-rate /
stable-bin statistics -- not at re-testing well-documented third-party
behavior (e.g. plain polars group-by/join semantics).
"""

from datetime import datetime, timedelta

import pandas as pd
import polars as pl
import pytest

from pudl.analysis.operational_characteristics import (
    _add_run_id_expr,
    _assert_required_quarters_available,
    _ordinal_to_quarter_start,
    _select_target_year_quarter,
    _year_quarter_to_ordinal,
    assign_groupwise_load_factor_bins,
    calculate_min_up_or_down_times,
    compute_heat_rate_at_max_load,
    compute_min_stable_heat_rates,
    compute_minimum_stable_bin,
    estimate_operational_characteristics_by_unit,
    filter_cems_for_heat_rate_analysis,
    filter_for_min_stable_bin,
    handle_adjustment_in_cems,
    prep_output_df,
    summarize_ramp_rates,
)

BASE_HOUR = datetime(2024, 1, 1, 0, 0)


def _hourly_cems_rows(
    unit: str, loads: list[float], state: str = "CO", start_hour: int = 0
) -> list[dict]:
    """Build one hour of synthetic hourly CEMS data per entry in ``loads``.

    ``heat_content_mmbtu`` is fixed at 10x the load so heat rate math stays
    simple and predictable (10.0 MMBtu/MWh) in tests that don't care about it.
    """
    return [
        {
            "plant_id_eia": 1,
            "plant_id_epa": 1,
            "emissions_unit_id_epa": unit,
            "operating_datetime_utc": BASE_HOUR + timedelta(hours=start_hour + i),
            "state": state,
            "operating_time_hours": 1.0,
            "gross_load_mw": load,
            "heat_content_mmbtu": load * 10.0,
        }
        for i, load in enumerate(loads)
    ]


# ---------------------------------------------------------------------------
# Quarter-window arithmetic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "year_quarter,expected_ordinal",
    [
        ("2024q1", 2024 * 4 + 0),
        ("2024q4", 2024 * 4 + 3),
        ("2023q4", 2023 * 4 + 3),
    ],
)
def test_year_quarter_to_ordinal(year_quarter, expected_ordinal):
    """Ordinal encoding must be monotonic and exactly recover year/quarter.

    This is the arithmetic that drives the trailing-quarters window. An
    off-by-one here would silently shift the whole analysis window by a
    quarter without raising any error.
    """
    assert _year_quarter_to_ordinal(year_quarter) == expected_ordinal


def test_year_quarter_ordinal_round_trip_across_year_boundary():
    """Incrementing an ordinal across a year boundary must roll year+quarter.

    2023q4 -> 2024q1 is the trickiest boundary in this arithmetic (both the
    year and the quarter change at once), so it's the one most likely to
    reveal a bug in the ordinal <-> (year, quarter) conversion.
    """
    ordinal = _year_quarter_to_ordinal("2023q4")
    assert _ordinal_to_quarter_start(ordinal) == pd.Timestamp("2023-10-01")
    assert _ordinal_to_quarter_start(ordinal + 1) == pd.Timestamp("2024-01-01")


# ---------------------------------------------------------------------------
# Target year-quarter selection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "year_quarters,expected",
    [
        # Production shape: many quarters available, including q4s. Must
        # pick the latest q4 so the window stays a whole-calendar-year one,
        # reproducing the historical behavior of this analysis. The gaps
        # here (no 2022q2/q3, no 2023q1-q3) are irrelevant to this function
        # -- it only ever needs max(), never checks continuity -- so this
        # also confirms gaps elsewhere in the list don't confuse it.
        (["2022q1", "2022q4", "2023q4", "2024q1", "2024q2"], "2023q4"),
        # Fast ETL / CI shape: only a single non-q4 quarter is available at
        # all. This is exactly the case that used to raise ValueError (empty
        # sequence passed to max()) before the fallback was added.
        (["2024q1"], "2024q1"),
        # Multiple non-q4 quarters available, still no q4: falls back to the
        # single most recent one rather than erroring.
        (["2023q3", "2024q1", "2024q2"], "2024q2"),
    ],
)
def test_select_target_year_quarter(year_quarters, expected):
    """Target selection must prefer the latest q4, falling back gracefully.

    This directly guards the bug that motivated this module's quarters
    refactor: picking a target quarter used to assume a q4 was always
    present, which crashed outright under the fast ETL / CI's restricted
    EPA CEMS data.
    """
    assert _select_target_year_quarter(year_quarters) == expected


def test_asserts_required_quarters_available_in_year_quarters():
    """Raise when the EPA CEMS data config doesn't cover the trailing window.

    ``EpaCemsDataConfig`` (see ``pudl.settings``) only validates that each
    configured year-quarter is a real partition and that there are no
    duplicates -- it does not require the list to be contiguous. So a config
    like the one below, with a gap from 2022q2 through 2023q3, is legal as
    far as Pydantic is concerned.

    Nothing downstream of the config would ever notice that gap on its own:
    ``_select_target_year_quarter`` only needs a max(), and
    ``filter_cems_for_heat_rate_analysis`` filters hourly CEMS data purely by
    timestamp range, with no visibility into which quarters were actually
    requested. Without an explicit check like this one, a discontinuous (or
    just too-short) ``year_quarters`` config would silently produce
    estimates from a partial window instead of failing loudly.
    """
    year_quarters = ["2022q1", "2022q4", "2023q4", "2024q1", "2024q2"]
    with pytest.raises(ValueError, match="2023q1"):
        # Trailing 6 quarters ending 2024q2 requires 2023q1-q3, none of
        # which are present in year_quarters.
        _assert_required_quarters_available(
            year_quarters, target_year_quarter="2024q2", num_quarters=6
        )


def test_asserts_required_quarters_available_passes_when_fully_covered():
    """The same check must not raise when the trailing window is fully covered."""
    year_quarters = ["2022q1", "2022q2", "2022q3", "2022q4", "2023q1"]
    _assert_required_quarters_available(
        year_quarters, target_year_quarter="2023q1", num_quarters=4
    )


# ---------------------------------------------------------------------------
# filter_cems_for_heat_rate_analysis
# ---------------------------------------------------------------------------


def test_filter_cems_window_boundaries_are_left_closed():
    """The trailing-quarter window must be [start, end) on operating_datetime_utc.

    The filter changed from a whole-year filter to a timestamp-range filter
    specifically to stay eligible for parquet predicate pushdown. A fencepost
    error in the new range bounds would silently include or exclude an extra
    hour of data at the quarter boundary.
    """
    rows = [
        {
            "plant_id_eia": 1,
            "plant_id_epa": 1,
            "emissions_unit_id_epa": "A",
            "operating_datetime_utc": ts,
            "state": "CO",
            "operating_time_hours": 1.0,
            "gross_load_mw": 10.0,
            "heat_content_mmbtu": 5.0,
        }
        for ts in [
            datetime(2023, 12, 31, 23, 0),  # just before window: excluded
            datetime(2024, 1, 1, 0, 0),  # window start: included
            datetime(2024, 3, 31, 23, 0),  # window end (last hour of Q1): included
            datetime(2024, 4, 1, 0, 0),  # just after window: excluded
        ]
    ]
    out = filter_cems_for_heat_rate_analysis(
        pl.LazyFrame(rows), final_year_quarter="2024q1", num_quarters=1
    ).collect()
    assert out["operating_datetime_utc"].to_list() == [
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 3, 31, 23, 0),
    ]


def test_filter_cems_num_quarters_spans_multiple_years():
    """num_quarters=12 (production default) must span exactly 3 full years.

    This is the concrete regression check that the quarters-based rewrite
    reproduces the old "3 full years ending in the target year" window,
    which the pre-refactor implementation got by filtering whole calendar
    years directly.
    """
    rows = [
        {
            "plant_id_eia": 1,
            "plant_id_epa": 1,
            "emissions_unit_id_epa": "A",
            "operating_datetime_utc": ts,
            "state": "CO",
            "operating_time_hours": 1.0,
            "gross_load_mw": 10.0,
            "heat_content_mmbtu": 5.0,
        }
        for ts in [
            datetime(2021, 12, 31, 23, 0),  # just before window: excluded
            datetime(2022, 1, 1, 0, 0),  # window start: included
            datetime(2024, 12, 31, 23, 0),  # window end: included
        ]
    ]
    out = filter_cems_for_heat_rate_analysis(
        pl.LazyFrame(rows), final_year_quarter="2024q4", num_quarters=12
    ).collect()
    assert out["operating_datetime_utc"].to_list() == [
        datetime(2022, 1, 1, 0, 0),
        datetime(2024, 12, 31, 23, 0),
    ]


def test_filter_cems_states_filter():
    """The optional states filter should keep only the requested states."""
    rows = _hourly_cems_rows("A", [10.0], state="CO") + _hourly_cems_rows(
        "A", [10.0], state="TX"
    )
    out = filter_cems_for_heat_rate_analysis(
        pl.LazyFrame(rows),
        final_year_quarter="2024q1",
        num_quarters=1,
        states=["CO"],
    ).collect()
    assert out["state"].to_list() == ["CO"]


def test_filter_cems_silently_returns_partial_data_when_a_quarter_is_missing():
    """This filter has no way to detect -- or complain about -- a gap in its input.

    It only knows the timestamp range it was asked for; it can't tell whether
    every quarter in that range was actually loaded into
    ``core_epacems__hourly_emissions``. Here the input is missing all of
    2023q2 (nominally the middle of a 4-quarter window), and the filter just
    returns the surrounding quarters with a hole in the middle, no error.

    This is why :func:`_assert_required_quarters_available` exists as a
    separate, config-level check: it's the thing that's actually supposed to
    catch a discontinuous or too-short EPA CEMS configuration, not this
    function, which by design only understands raw timestamps.
    """
    rows = [
        {
            "plant_id_eia": 1,
            "plant_id_epa": 1,
            "emissions_unit_id_epa": "A",
            "operating_datetime_utc": ts,
            "state": "CO",
            "operating_time_hours": 1.0,
            "gross_load_mw": 10.0,
            "heat_content_mmbtu": 5.0,
        }
        for ts in [
            datetime(2023, 1, 1, 0, 0),  # 2023q1: present
            datetime(2023, 10, 1, 0, 0),  # 2023q4: present
            # 2023q2 and 2023q3 are entirely absent from the input.
        ]
    ]
    out = filter_cems_for_heat_rate_analysis(
        pl.LazyFrame(rows), final_year_quarter="2023q4", num_quarters=4
    ).collect()
    assert out["operating_datetime_utc"].to_list() == [
        datetime(2023, 1, 1, 0, 0),
        datetime(2023, 10, 1, 0, 0),
    ]


# ---------------------------------------------------------------------------
# _add_run_id_expr
# ---------------------------------------------------------------------------


def test_add_run_id_expr_breaks_on_gap_state_change_and_bin_change():
    """A run must break on an hour gap, a state change, or a bin change.

    ``_add_run_id_expr`` is the shared basis for both the stable-bin run
    detection and the min up/down-time run detection, so a bug here would
    silently corrupt several output columns at once via seemingly-unrelated
    downstream functions.
    """
    rows = [
        # Hours 0-2: contiguous, same bin/state -> one run.
        {"unit": "A", "bin": "x", "state": "CO", "t": BASE_HOUR + timedelta(hours=0)},
        {"unit": "A", "bin": "x", "state": "CO", "t": BASE_HOUR + timedelta(hours=1)},
        {"unit": "A", "bin": "x", "state": "CO", "t": BASE_HOUR + timedelta(hours=2)},
        # Hour 3 is missing (a gap) -> new run at hour 4.
        {"unit": "A", "bin": "x", "state": "CO", "t": BASE_HOUR + timedelta(hours=4)},
        # State changes -> new run, even though bin and unit are unchanged.
        {"unit": "A", "bin": "x", "state": "TX", "t": BASE_HOUR + timedelta(hours=5)},
        # Bin changes -> new run, even though state and unit are unchanged.
        {"unit": "A", "bin": "y", "state": "TX", "t": BASE_HOUR + timedelta(hours=6)},
    ]
    df = pl.DataFrame(rows).rename({"t": "operating_datetime_utc"})
    out = df.with_columns(
        _add_run_id_expr(unit_cols=["unit", "bin"], state_col="state").alias("run_id")
    )
    assert out["run_id"].to_list() == [1, 1, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# assign_groupwise_load_factor_bins
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "load_factors",
    [
        [0.1, 0.15, 0.3, 0.35, 0.5, 0.65, 0.7, 0.85, 0.9, 1.0],
        [0.0, 0.02, 0.5, 0.98, 1.0],
        [0.2, 0.4, 0.6, 0.8],
    ],
)
def test_assign_groupwise_load_factor_bins_matches_pandas_cut(load_factors):
    """Polars bin edges/ranks must exactly match ``pandas.cut`` for the same data.

    ``assign_groupwise_load_factor_bins`` is a hand-vectorized reimplementation
    of ``pandas.cut(bins=10, right=True, include_lowest=False)`` (per unit),
    including a documented float-precision workaround for values that land
    exactly on a bin edge. This is the most complex, most bug-prone function
    in the module, so it's checked directly against the real pandas
    implementation rather than against hand-derived expected numbers.

    Comparing against ``retbins=True``'s raw bin edges (not against the
    ``.cat.categories`` interval labels) matters here: pandas.cut assigns
    values to bins using the raw, unrounded edges, but then cosmetically
    rounds the *displayed* interval labels to ``precision=3`` significant
    digits by default. Comparing against the rounded labels would fail on
    real edge values while telling us nothing about an actual bug.
    """
    values = pd.Series(load_factors)
    codes, pandas_bins = pd.cut(
        values, bins=10, right=True, include_lowest=False, labels=False, retbins=True
    )

    rows = [{"unit": "A", "lf": v, "state": "CO"} for v in load_factors]
    out = assign_groupwise_load_factor_bins(
        pl.LazyFrame(rows), unit_cols=["unit"], load_factor_col="lf"
    ).to_pandas()

    expected_lower = pandas_bins[codes]
    expected_upper = pandas_bins[codes + 1]
    # dense-rank pandas' raw codes the same way the polars implementation
    # dense-ranks load_factor_bin_lower, so only bins that actually appear in
    # the data get consecutive ranks.
    expected_rank = pd.Series(codes).rank(method="dense").astype("int64").to_numpy()

    assert out["load_factor_bin_lower"].to_numpy() == pytest.approx(expected_lower)
    assert out["load_factor_bin_upper"].to_numpy() == pytest.approx(expected_upper)
    assert out["load_factor_bin_rank"].to_numpy().tolist() == expected_rank.tolist()


def test_assign_groupwise_load_factor_bins_constant_load_is_all_null():
    """A unit with only one distinct load factor can't be binned at all.

    ``pandas.cut`` can't build 10 meaningful bins from a single repeated
    value, so this function should recognize that (via ``load_factor_nunique``)
    and leave the bin columns null rather than degenerate-binning everything
    into one bucket.
    """
    rows = [{"unit": "A", "lf": 0.5, "state": "CO"} for _ in range(5)]
    out = assign_groupwise_load_factor_bins(
        pl.LazyFrame(rows), unit_cols=["unit"], load_factor_col="lf"
    )
    assert out["load_factor_bin_lower"].is_null().all()
    assert out["load_factor_bin_rank"].is_null().all()


def test_assign_groupwise_load_factor_bins_are_independent_per_unit():
    """Each unit's bins must come from its own load factors, not the whole batch.

    This isn't a test of "can polars group by a column" -- it's a check that
    *this* function's several ``.over(unit_cols)`` window calls (for the
    per-unit min/max and rank) are all actually scoped to the unit, everywhere
    they need to be. Dropping just one of them would silently mix units
    together: e.g. computing ``lo``/``hi`` globally instead of per-unit would
    stretch unit A's narrow low-load-factor range out across unit B's much
    higher range, producing bin edges that don't match either unit's own
    ``pandas.cut`` result.

    Deliberately reuses the same pandas.cut-based expected-value technique as
    ``test_assign_groupwise_load_factor_bins_matches_pandas_cut``, but applied
    to two units at once: this is exactly what would fail if per-unit scoping
    were broken, and exactly what a looser "is roughly in the right range"
    assertion would miss, since two well-separated units' bins can still look
    individually plausible even when computed from the wrong (combined) range.
    """
    load_factors_by_unit = {
        "A": [0.0, 0.02, 0.05, 0.1],
        "B": [0.8, 0.85, 0.95, 1.0],
    }
    rows = [
        {"unit": unit, "lf": v, "state": "CO"}
        for unit, values in load_factors_by_unit.items()
        for v in values
    ]
    out = assign_groupwise_load_factor_bins(
        pl.LazyFrame(rows), unit_cols=["unit"], load_factor_col="lf"
    ).to_pandas()

    for unit, values in load_factors_by_unit.items():
        codes, pandas_bins = pd.cut(
            pd.Series(values),
            bins=10,
            right=True,
            include_lowest=False,
            labels=False,
            retbins=True,
        )
        unit_out = out[out["unit"] == unit].sort_values("lf")
        assert unit_out["load_factor_bin_lower"].to_numpy() == pytest.approx(
            pandas_bins[codes]
        )
        assert unit_out["load_factor_bin_upper"].to_numpy() == pytest.approx(
            pandas_bins[codes + 1]
        )
        # Each unit's own ranks must start at 1 -- if rank were computed
        # globally, unit B's ranks would instead continue upward from
        # wherever unit A's left off.
        assert unit_out["load_factor_bin_rank"].min() == 1


def test_assign_groupwise_load_factor_bins_handles_mixed_constant_and_varying_units():
    """A constant-load unit must not affect, or be affected by, a varying one.

    Guards against the eligibility check (``load_factor_nunique > 1``) leaking
    across units -- e.g. a missing ``.over(unit_cols)`` on ``n_unique`` could
    let unit B's real variation "unlock" binning for unit A even though A
    never varies, or vice versa null out B because A looks constant.
    """
    rows = [{"unit": "A", "lf": 0.5, "state": "CO"} for _ in range(5)] + [
        {"unit": "B", "lf": v, "state": "CO"} for v in [0.2, 0.4, 0.6, 0.8, 1.0]
    ]
    out = assign_groupwise_load_factor_bins(
        pl.LazyFrame(rows), unit_cols=["unit"], load_factor_col="lf"
    )
    unit_a = out.filter(pl.col("unit") == "A")
    unit_b = out.filter(pl.col("unit") == "B")
    assert unit_a["load_factor_bin_rank"].is_null().all()
    assert unit_b["load_factor_bin_rank"].is_not_null().all()


# ---------------------------------------------------------------------------
# summarize_ramp_rates
# ---------------------------------------------------------------------------


def _steady_ramp_rows(unit: str, n_points: int) -> list[dict]:
    """``n_points`` hourly rows with generation increasing by 1 MWh/hr."""
    return [
        {
            "unit": unit,
            "operating_datetime_utc": BASE_HOUR + timedelta(hours=i),
            "gen": float(i),
        }
        for i in range(n_points)
    ]


def test_summarize_ramp_rates_requires_at_least_20_observations():
    """Units with fewer than 20 valid ramp-rate observations are dropped entirely.

    ``.having(pl.len() >= 20)`` enforces a minimum sample size for the ramp
    statistics to be meaningful. This pins down the exact boundary (19 diffs
    dropped, 20 kept) so a future refactor can't accidentally shift it by one.
    """
    below_threshold = _steady_ramp_rows("below", 20)  # 19 diffs
    at_threshold = _steady_ramp_rows("at", 21)  # 20 diffs
    out = summarize_ramp_rates(
        pl.DataFrame(below_threshold + at_threshold),
        unit_cols=["unit"],
        generation_col="gen",
    )
    assert out["unit"].to_list() == ["at"]


def test_summarize_ramp_rates_drops_infinite_rate_from_duplicate_timestamp():
    """A duplicate timestamp (zero time_delta) must not poison the median.

    Dividing by a zero time_delta produces +/-inf ramp rates, which are
    explicitly replaced with null and dropped before computing the median.
    Without that step, a single duplicate timestamp -- not unheard of in raw
    hourly CEMS data -- would corrupt every downstream ramp-rate statistic
    for that unit.
    """
    rows = _steady_ramp_rows("A", 21)
    # Append a duplicate of the last timestamp with a wildly different value:
    # if the inf ramp rate it produces weren't dropped, it would appear in
    # the sorted list summarize_ramp_rates uses for the ramp_up_rate median.
    rows.append(
        {
            "unit": "A",
            "operating_datetime_utc": rows[-1]["operating_datetime_utc"],
            "gen": 999.0,
        }
    )
    out = summarize_ramp_rates(
        pl.DataFrame(rows), unit_cols=["unit"], generation_col="gen"
    )
    assert out.row(0, named=True)["ramp_up_rate"] == pytest.approx(1.0)
    assert out.row(0, named=True)["ramp_down_rate"] == pytest.approx(1.0)


def test_summarize_ramp_rates_medians_top_and_bottom_five_percent():
    """Ramp rate should be the median of the bottom/top 5% of hourly ramp rates.

    Uses a hand-constructed set of ramp rates where the top and bottom 5%
    are known in advance, so the expected medians can be computed by hand
    rather than re-deriving the function's own logic.
    """
    # 100 hourly points -> 99 diffs. Ramp rates: mostly 1.0, except for a
    # slow start (bottom 5%, ~5 points near 0.1) and a fast finish (top 5%,
    # ~5 points near 10.0).
    gens = [0.0]
    for _ in range(5):
        gens.append(gens[-1] + 0.1)  # slow ramp: bottom 5%
    for _ in range(89):
        gens.append(gens[-1] + 1.0)  # steady middle
    for _ in range(5):
        gens.append(gens[-1] + 10.0)  # fast ramp: top 5%
    rows = [
        {
            "unit": "A",
            "operating_datetime_utc": BASE_HOUR + timedelta(hours=i),
            "gen": g,
        }
        for i, g in enumerate(gens)
    ]
    out = summarize_ramp_rates(
        pl.DataFrame(rows), unit_cols=["unit"], generation_col="gen"
    )
    row = out.row(0, named=True)
    assert row["ramp_down_rate"] == pytest.approx(0.1)
    assert row["ramp_up_rate"] == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# handle_adjustment_in_cems (non-adjusted branch)
# ---------------------------------------------------------------------------


def test_handle_adjustment_in_cems_computes_expected_formulas():
    """Load factor, generation, and heat rate must match their exact formulas.

    Pure arithmetic, cheap to verify by hand -- guards against e.g. an
    accidentally inverted heat-rate ratio (MMBtu/MWh vs MWh/MMBtu), which
    would silently produce plausible-looking but wrong numbers everywhere
    downstream.
    """
    rows = [
        {
            "plant_id_epa": 1,
            "emissions_unit_id_epa": "A",
            "operating_datetime_utc": BASE_HOUR,
            "gross_load_mw": 50.0,
            "operating_time_hours": 1.0,
            "heat_content_mmbtu": 500.0,
        },
        {
            "plant_id_epa": 1,
            "emissions_unit_id_epa": "A",
            "operating_datetime_utc": BASE_HOUR + timedelta(hours=1),
            "gross_load_mw": 100.0,
            "operating_time_hours": 1.0,
            "heat_content_mmbtu": 900.0,
        },
    ]
    out, col_dict = handle_adjustment_in_cems(
        pl.LazyFrame(rows), unit_cols=["plant_id_epa", "emissions_unit_id_epa"]
    )
    result = out.collect()
    assert result["load_factor"].to_list() == pytest.approx([0.5, 1.0])
    assert result["gross_load_mwh"].to_list() == pytest.approx([50.0, 100.0])
    assert result["heat_rate_mmbtu_per_mwh"].to_list() == pytest.approx([10.0, 9.0])
    assert col_dict == {
        "load_factor_col": "load_factor",
        "generation_col": "gross_load_mwh",
        "heat_rate_col": "heat_rate_mmbtu_per_mwh",
        "max_load_col": "max_gross_load_mw",
    }


# ---------------------------------------------------------------------------
# prep_output_df
# ---------------------------------------------------------------------------


def test_prep_output_df_includes_every_unit_even_without_derived_data():
    """Every distinct unit must get exactly one output row, even with no bins yet.

    This is the documented invariant that lets units without enough data to
    support the full analysis (e.g. a constant-load unit) still show up in
    the final table with null derived values, instead of silently vanishing.
    """
    rows = [
        {"unit": "A", "plant_id_eia": 1, "state": "CO", "max_gross_load_mw": 100.0}
        for _ in range(3)
    ]
    out = prep_output_df(
        pl.DataFrame(rows), unit_cols=["unit"], max_load_col="max_gross_load_mw"
    )
    assert out["unit"].to_list() == ["A"]
    assert out["min_stable_load_factor"].is_null().all()


# ---------------------------------------------------------------------------
# compute_minimum_stable_bin
# ---------------------------------------------------------------------------


def _run_rows(
    unit: str, rank: int, lower: float, run_id: int, run_length: int
) -> list[dict]:
    return [
        {
            "unit": unit,
            "load_factor_bin_rank": rank,
            "load_factor_bin_lower": lower,
            "load_factor_bin": {"left": lower, "right": lower + 0.1},
            "bin_run_id": run_id,
        }
        for _ in range(run_length)
    ]


def test_compute_minimum_stable_bin_threshold_and_selection():
    """Only runs meeting the threshold qualify, and the lowest one is picked.

    Covers three behaviors at once: a run one hour short of the threshold is
    excluded, a run exactly at the threshold is included, and among multiple
    qualifying runs the *lowest* load-factor bin is selected as "minimum
    stable." Also checks that a unit with no qualifying run at all is simply
    absent from the result (left for the caller's join to backfill as null).
    """
    rows = (
        _run_rows("A", rank=2, lower=0.2, run_id=1, run_length=7)  # below threshold
        + _run_rows("A", rank=3, lower=0.3, run_id=2, run_length=8)  # at threshold
        + _run_rows("A", rank=4, lower=0.4, run_id=3, run_length=20)  # also qualifies
        + _run_rows("B", rank=2, lower=0.2, run_id=4, run_length=3)  # never qualifies
    )
    out = compute_minimum_stable_bin(
        pl.DataFrame(rows), unit_cols=["unit"], min_stable_consecutive_hours=8
    )
    assert out["unit"].to_list() == ["A"]
    assert out.row(0, named=True)["min_stable_bin_upper"] == 3
    assert out.row(0, named=True)["min_stable_load_factor"] == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# compute_heat_rate_at_max_load / compute_min_stable_heat_rates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "compute_fn,extra_args",
    [
        (compute_heat_rate_at_max_load, ()),
        (
            compute_min_stable_heat_rates,
            (
                pl.DataFrame(
                    {"unit": ["A"], "min_stable_bin": [{"left": 0.3, "right": 0.4}]}
                ),
            ),
        ),
    ],
)
def test_heat_rate_summary_takes_median_of_target_bin_only(compute_fn, extra_args):
    """Heat rate summaries must median only the target bin, ignoring other bins.

    Both functions select a specific bin (the highest-rank bin for max load,
    or the min-stable bin) and take the median heat rate within it. A record
    from a different bin (rank 1, heat rate 12.0) is included as a decoy to
    confirm it's excluded from the result.
    """
    rows = [
        {
            "unit": "A",
            "load_factor_bin_rank": 1,
            "load_factor_bin": {"left": 0.1, "right": 0.2},
            "hr": 12.0,
        },
        {
            "unit": "A",
            "load_factor_bin_rank": 3,
            "load_factor_bin": {"left": 0.3, "right": 0.4},
            "hr": 10.0,
        },
        {
            "unit": "A",
            "load_factor_bin_rank": 3,
            "load_factor_bin": {"left": 0.3, "right": 0.4},
            "hr": 8.0,
        },
    ]
    out = compute_fn(
        pl.DataFrame(rows), *extra_args, unit_cols=["unit"], heat_rate_col="hr"
    )
    assert out.row(0, named=True)[out.columns[-1]] == pytest.approx(9.0)


# ---------------------------------------------------------------------------
# filter_for_min_stable_bin
# ---------------------------------------------------------------------------


def test_filter_for_min_stable_bin_keeps_bin_at_or_above_minimum():
    """A record exactly at the minimum stable bin must be kept, not excluded.

    ``filter_for_min_stable_bin`` is used to define "up" time -- getting the
    boundary condition (>=, not >) wrong would misclassify every hour spent
    exactly at the minimum stable operating level as "down" time instead.
    """
    rows = [
        {
            "load_factor_bin": {"left": 0.3, "right": 0.4},
            "min_stable_bin": {"left": 0.3, "right": 0.4},
            "note": "equal",
        },
        {
            "load_factor_bin": {"left": 0.2, "right": 0.3},
            "min_stable_bin": {"left": 0.3, "right": 0.4},
            "note": "below",
        },
        {
            "load_factor_bin": {"left": 0.4, "right": 0.5},
            "min_stable_bin": {"left": 0.3, "right": 0.4},
            "note": "above",
        },
    ]
    out = filter_for_min_stable_bin(pl.DataFrame(rows))
    assert out["note"].to_list() == ["equal", "above"]


# ---------------------------------------------------------------------------
# calculate_min_up_or_down_times
# ---------------------------------------------------------------------------


def test_calculate_min_up_or_down_times_leaves_null_when_no_runs_exist():
    """A unit with zero 'down' runs must keep a null min_down_time_hours.

    Explicitly called out in a code comment as a fragile case: if a unit
    never goes below its minimum stable bin, the runs frame passed in is
    empty, and the subsequent left join is a no-op. This should leave the
    pre-existing null in place rather than erroring or coercing it to 0.
    """
    rows = [
        {
            "unit": "A",
            "operating_datetime_utc": BASE_HOUR + timedelta(hours=i),
            "load_factor_bin": {"left": 0.5, "right": 0.6},
            "min_stable_bin": {"left": 0.3, "right": 0.4},
        }
        for i in range(5)
    ]
    output = pl.DataFrame(
        {"unit": ["A"], "min_up_time_hours": [None], "min_down_time_hours": [None]}
    ).cast({"min_up_time_hours": pl.Float64, "min_down_time_hours": pl.Float64})

    out = calculate_min_up_or_down_times(
        output, pl.DataFrame(rows), unit_cols=["unit"], up_or_down="down"
    )
    assert out.row(0, named=True)["min_down_time_hours"] is None


# ---------------------------------------------------------------------------
# estimate_operational_characteristics_by_unit (end-to-end happy path)
# ---------------------------------------------------------------------------


def test_estimate_operational_characteristics_by_unit_happy_path():
    """The full per-unit pipeline should wire all of the above steps together.

    This isn't a substitute for the more targeted tests above -- it's a
    single smoke test confirming that composing them produces one row per
    unit, the expected column set/order, and that a unit with too little
    data to analyze (constant load) still appears with null derived values
    rather than being dropped or crashing the pipeline.
    """
    stable_unit_rows = _hourly_cems_rows(
        "A",
        [20.0] * 10 + [60.0] + [100.0] * 14,  # 25 hours, two stable plateaus
    )
    constant_load_unit_rows = _hourly_cems_rows("B", [50.0] * 5, start_hour=0)
    # give unit B a distinct plant_id_epa so it doesn't collide with unit A
    for row in constant_load_unit_rows:
        row["plant_id_epa"] = 2

    out = estimate_operational_characteristics_by_unit(
        pl.LazyFrame(stable_unit_rows + constant_load_unit_rows),
        min_stable_consecutive_hours=8,
    )

    assert out.columns == [
        "plant_id_epa",
        "emissions_unit_id_epa",
        "plant_id_eia",
        "state",
        "max_gross_load_mw",
        "min_stable_load_factor",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_load_factor_mmbtu_per_mwh",
        "ramp_up_rate_per_min",
        "ramp_down_rate_per_min",
    ]
    assert sorted(out["emissions_unit_id_epa"].to_list()) == ["A", "B"]

    by_unit = {row["emissions_unit_id_epa"]: row for row in out.to_dicts()}
    assert by_unit["A"]["min_stable_load_factor"] is not None
    assert by_unit["A"]["heat_rate_at_max_load_factor_mmbtu_per_mwh"] == pytest.approx(
        10.0
    )
    # Unit B never varies, so it can't be binned -- everything but the
    # identifying columns and max load should be null.
    assert by_unit["B"]["min_stable_load_factor"] is None
    assert by_unit["B"]["ramp_up_rate_per_min"] is None
    assert by_unit["B"]["max_gross_load_mw"] == pytest.approx(50.0)
