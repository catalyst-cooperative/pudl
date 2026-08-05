"""Unit tests for pudl.analysis.derived_plant_characteristics.

These tests use small, hand-constructed hourly EPA CEMS fixtures (rather than the
full ETL) so they run fast and pin down individual pieces of the algorithm:

- ``consecutive_run_ids``: the run-length building block used throughout.
- ``assign_groupwise_load_factor_bins`` vs. ``assign_load_factor_bins_vectorized``:
  the original per-unit ``pandas.cut`` fallback vs. its vectorized polars
  replacement, which should produce equivalent bin edges/ordinals.
- ``_summarize_ramp_rates`` vs. ``_summarize_ramp_rates_qcut``: the vectorized
  rank-based ramp-rate summary vs. a faithful reimplementation of the original
  script's ``pandas.qcut(duplicates="drop")`` approach. These are *not* expected
  to agree in general -- see notebooks/work-in-progress/sylvan-heat-rates.md for
  why -- so the tests here characterize the divergence rather than asserting
  equality.
- ``estimate_operational_characteristics_by_unit``: an end-to-end fixture with
  three units (a clean stable run, a unit that never sustains a stable run, and a
  constant-load unit) exercising the main branches of the pipeline.
"""

import datetime as dt
from collections.abc import Sequence

import numpy as np
import pandas as pd
import polars as pl
import pytest

from pudl.analysis.derived_plant_characteristics import (
    _add_run_id,
    _summarize_ramp_rates,
    _summarize_ramp_rates_qcut,
    assign_groupwise_load_factor_bins,
    assign_load_factor_bins_vectorized,
    consecutive_run_ids,
    estimate_operational_characteristics_by_unit,
    handle_adjustment_in_cems,
)

UNIT_COLS = ["plant_id_epa", "emissions_unit_id_epa"]


def _hourly_cems(
    gross_load_mw: Sequence[float | None],
    plant_id_epa: int = 1,
    emissions_unit_id_epa: str = "A",
    state: str = "CA",
    start: str = "2022-01-01",
    heat_rate: float = 7.5,
) -> pl.LazyFrame:
    """Build a minimal synthetic hourly CEMS LazyFrame for a single plant-unit.

    Hours are perfectly consecutive (no clock gaps), so run boundaries in tests
    built from this fixture are driven only by load-factor-bin membership and
    nulls, not by gaps in ``operating_datetime_utc``. ``heat_content_mmbtu`` is
    set to ``heat_rate * gross_load_mw`` so heat rate comes out constant and easy
    to reason about.
    """
    n = len(gross_load_mw)
    start_dt = dt.datetime.fromisoformat(start)
    timestamps = [start_dt + dt.timedelta(hours=i) for i in range(n)]
    return pl.LazyFrame(
        {
            "plant_id_eia": [plant_id_epa] * n,
            "plant_id_epa": [plant_id_epa] * n,
            "emissions_unit_id_epa": [emissions_unit_id_epa] * n,
            "operating_datetime_utc": timestamps,
            "year": [start_dt.year] * n,
            "state": [state] * n,
            "operating_time_hours": [1.0] * n,
            "gross_load_mw": gross_load_mw,
            "heat_content_mmbtu": [
                None if g is None else g * heat_rate for g in gross_load_mw
            ],
        }
    ).with_columns(
        # Real upstream CEMS data stores state as Categorical; matching that here
        # avoids a schema mismatch when assign_groupwise_load_factor_bins casts
        # only the "valid" (nunique > 1) rows to Categorical before concatenating
        # them back with the untouched "invalid" rows.
        pl.col("state").cast(pl.Categorical)
    )


def _to_df(frame: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    return frame.collect() if isinstance(frame, pl.LazyFrame) else frame


# ---------------------------------------------------------------------------
# consecutive_run_ids
# ---------------------------------------------------------------------------


def test_consecutive_run_ids_breaks_on_gap():
    """A single-hour gap should start a new run; a 1-hour cadence should not."""
    timestamps = [
        dt.datetime(2022, 1, 1, 0),
        dt.datetime(2022, 1, 1, 1),
        dt.datetime(2022, 1, 1, 2),
        # 2-hour gap here
        dt.datetime(2022, 1, 1, 5),
        dt.datetime(2022, 1, 1, 6),
    ]
    lf = pl.LazyFrame({"operating_datetime_utc": timestamps})
    run_ids = (
        lf.with_columns(consecutive_run_ids().alias("run_id"))
        .collect()["run_id"]
        .to_list()
    )
    assert run_ids == [1, 1, 1, 2, 2]


def test_consecutive_run_ids_boundary_run_length():
    """A run of exactly N hours should be a single run of length N."""
    n = 8
    timestamps = [dt.datetime(2022, 1, 1, 0) + dt.timedelta(hours=i) for i in range(n)]
    lf = pl.LazyFrame({"operating_datetime_utc": timestamps})
    run_lengths = (
        lf.with_columns(consecutive_run_ids().alias("run_id"))
        .group_by("run_id")
        .len()
        .collect()
    )
    assert run_lengths.height == 1
    assert run_lengths["len"][0] == n


def test_add_run_id_first_row_of_frame_starts_a_new_run():
    """Regression test: ``_add_run_id`` must treat row 0 as the start of a run.

    This used to be a real bug: the "same unit/bin and consecutive hour"
    boolean is built with ``pl.col(c).eq(pl.col(c).shift())``, which is null
    (not True/False) for the very first row of whatever frame it's called on,
    since there's no previous row to compare against. Unlike
    ``consecutive_run_ids()`` -- its sibling helper a few lines above in the
    same module, which explicitly does ``.fill_null(True)`` before
    ``cum_sum()`` -- ``_add_run_id`` didn't guard against this, so
    ``cum_sum()`` propagated a null for that first row instead of starting run
    0. That split a single real run into a spurious length-1 run (the first
    row) plus a shorter remainder, for whichever unit happened to sort first
    in the frame passed in -- very likely the root cause of the small,
    previously-unexplained ``min_up_time_hours``/``min_down_time_hours``
    divergence documented in ``notebooks/work-in-progress/sylvan-heat-rates.md``
    (~2.5% of California units).

    Fixed by moving ``.fill_null(True)`` onto the final combined "differs from
    previous row" expression (mirroring ``consecutive_run_ids()``) rather than
    onto each individually-shifted column -- filling a shifted column's own
    nulls with the bare boolean ``True`` breaks for non-boolean columns (see
    ``test_add_run_id_handles_struct_unit_cols`` below, which is exactly the
    call site -- binning by ``load_factor_bin``, a Struct column -- that a
    naive per-column ``fill_null(True)`` fix would crash on).
    """
    timestamps = [dt.datetime(2022, 1, 1, 0) + dt.timedelta(hours=i) for i in range(10)]
    df = pl.DataFrame(
        {
            "plant_id_epa": [1] * 10,
            "emissions_unit_id_epa": ["A"] * 10,
            "operating_datetime_utc": timestamps,
        }
    )
    # _add_run_id is annotated as taking a LazyFrame, but internally calls
    # .to_series(), which only exists on DataFrame -- an existing, unmodified
    # inconsistency between its type hint and implementation.
    run_id = _add_run_id(
        df,  # type: ignore[bad-argument-type]
        unit_cols=["plant_id_epa", "emissions_unit_id_epa"],
    )
    run_ids = df.with_columns(run_id.alias("run_id"))["run_id"].to_list()

    # The whole 10-hour block is one real run (cum_sum of a boolean series is
    # 1-indexed, same as consecutive_run_ids() above): row 0 should join that
    # one run, not split off into its own null/spurious run.
    assert run_ids == [1] * 10


def test_add_run_id_handles_struct_unit_cols():
    """``_add_run_id`` is also called with a Struct column in ``unit_cols``.

    ``estimate_operational_characteristics_by_unit`` calls
    ``_add_run_id(lf, unit_cols=unit_cols + ["load_factor_bin"])``, where
    ``load_factor_bin`` is a Struct (interval-like) column, to compute
    per-bin run lengths. This is a coverage test for that call shape: a naive
    fix for the row-0 bug above (filling each shifted column's nulls with a
    bare ``True``) raises ``InvalidOperationError: could not determine
    supertype of: [struct[2], bool]`` on this exact input, since a Struct
    column has no valid boolean fill value. The actual fix (filling the final
    combined boolean expression instead) has no such problem.
    """
    timestamps = [dt.datetime(2022, 1, 1, 0) + dt.timedelta(hours=i) for i in range(4)]
    df = pl.DataFrame(
        {
            "plant_id_epa": [1, 1, 1, 1],
            "emissions_unit_id_epa": ["A", "A", "A", "A"],
            "operating_datetime_utc": timestamps,
            "load_factor_bin": [
                {"left": 0.0, "right": 0.5},
                {"left": 0.0, "right": 0.5},
                {"left": 0.5, "right": 1.0},
                {"left": 0.5, "right": 1.0},
            ],
        }
    )
    run_id = _add_run_id(
        df,  # type: ignore[bad-argument-type]
        unit_cols=["plant_id_epa", "emissions_unit_id_epa", "load_factor_bin"],
    )
    run_ids = df.with_columns(run_id.alias("run_id"))["run_id"].to_list()
    assert run_ids == [1, 1, 2, 2]


# ---------------------------------------------------------------------------
# assign_groupwise_load_factor_bins vs. assign_load_factor_bins_vectorized
# ---------------------------------------------------------------------------


def _prepped_cems() -> tuple[pl.LazyFrame, dict]:
    """Two units with random-ish, non-degenerate load factor distributions."""
    rng = np.random.default_rng(42)
    unit_a = _hourly_cems(
        list(rng.uniform(1, 50, size=120)),
        plant_id_epa=1,
        emissions_unit_id_epa="A",
    )
    unit_b = _hourly_cems(
        list(rng.uniform(10, 100, size=80)),
        plant_id_epa=2,
        emissions_unit_id_epa="B",
    )
    cems = pl.concat([unit_a, unit_b])
    return handle_adjustment_in_cems(cems, UNIT_COLS, adjusted=False)


def test_vectorized_bins_match_pandas_cut_on_random_data():
    """The vectorized bin assignment should closely match pandas.cut per unit.

    Both methods should agree on which bin each row falls in (same dense-ranked
    ordinal) and on the bin edges to within floating-point tolerance. This is the
    empirical check gating whether assign_load_factor_bins_vectorized can safely
    replace the pandas fallback.
    """
    cems_working, col_dict = _prepped_cems()
    load_factor_col = col_dict["load_factor_col"]

    pandas_cut_result = _to_df(
        assign_groupwise_load_factor_bins(cems_working, UNIT_COLS, load_factor_col)
    ).sort(UNIT_COLS + ["operating_datetime_utc"])
    vectorized_result = _to_df(
        assign_load_factor_bins_vectorized(cems_working, UNIT_COLS, load_factor_col)
    ).sort(UNIT_COLS + ["operating_datetime_utc"])

    assert (
        pandas_cut_result["load_factor_bin_ordinal"].to_list()
        == vectorized_result["load_factor_bin_ordinal"].to_list()
    )

    left_diff = (
        pandas_cut_result["load_factor_bin_left"]
        - vectorized_result["load_factor_bin_left"]
    ).abs()
    right_diff = (
        pandas_cut_result["load_factor_bin_right"]
        - vectorized_result["load_factor_bin_right"]
    ).abs()
    # Allow a small floating-point tolerance -- pandas computes edges via
    # np.linspace (which accumulates float error across repeated addition)
    # while the vectorized version computes each edge algebraically from the
    # group min/width, so edge *values* can differ very slightly even when both
    # methods agree on which bin (ordinal) a row falls into, as asserted above.
    assert float(left_diff.max()) < 1e-3  # type: ignore[bad-argument-type]
    assert float(right_diff.max()) < 1e-3  # type: ignore[bad-argument-type]


def test_vectorized_bins_only_pad_lowest_bin_edge():
    """pandas.cut(bins=10) only pads bins[0], not every bin edge.

    Regression test for a bug found while writing this vectorized implementation: an
    earlier version spread the 0.1%-of-range padding across all ten bins (by padding the
    low edge and then dividing the padded span into 10 equal parts), which silently
    shifted every bin edge. pandas only shifts the very first edge, leaving bins 2-10 at
    their unpadded width.
    """
    values = list(range(11))  # 0..10, load_factor = value / 10 conceptually
    cems = _hourly_cems([float(v) for v in values])
    cems_working, col_dict = handle_adjustment_in_cems(cems, UNIT_COLS, adjusted=False)
    load_factor_col = col_dict["load_factor_col"]

    pandas_cut_result = _to_df(
        assign_groupwise_load_factor_bins(cems_working, UNIT_COLS, load_factor_col)
    ).sort("operating_datetime_utc")
    vectorized_result = _to_df(
        assign_load_factor_bins_vectorized(cems_working, UNIT_COLS, load_factor_col)
    ).sort("operating_datetime_utc")

    pd_edges = sorted(
        {
            (row["left"], row["right"])
            for row in pandas_cut_result["load_factor_bin"].to_list()
            if row is not None
        }
    )
    vec_edges = sorted(
        {
            (row["left"], row["right"])
            for row in vectorized_result["load_factor_bin"].to_list()
            if row is not None
        }
    )
    assert len(pd_edges) == len(vec_edges) == 10
    for (pd_left, pd_right), (vec_left, vec_right) in zip(
        pd_edges, vec_edges, strict=True
    ):
        assert pd_left == pytest.approx(vec_left, abs=1e-9)
        assert pd_right == pytest.approx(vec_right, abs=1e-9)
    # Only the very first (lowest) bin should be wider than the rest.
    widths = [right - left for left, right in pd_edges]
    assert widths[0] > widths[1]
    assert widths[1:] == pytest.approx([widths[1]] * len(widths[1:]), abs=1e-9)


# ---------------------------------------------------------------------------
# _summarize_ramp_rates vs. _summarize_ramp_rates_qcut
# ---------------------------------------------------------------------------


def _ramp_rate_input(
    gross_load_mw: Sequence[float | None],
) -> tuple[pl.LazyFrame, dict]:
    cems = _hourly_cems(gross_load_mw)
    return handle_adjustment_in_cems(cems, UNIT_COLS, adjusted=False)


def test_ramp_rate_methods_diverge_with_many_tied_deltas():
    """rank_split and qcut disagree when many hour-over-hour deltas are tied.

    This reproduces the realistic case (a unit idling at constant load most
    hours, with occasional ramps) where pandas.qcut(duplicates="drop") collapses
    the many zero-delta observations into fewer effective bins, while the
    rank-based top/bottom-5% split does not. See
    notebooks/work-in-progress/sylvan-heat-rates.md for the empirical basis of
    this divergence found against real CEMS data.
    """
    # Mostly flat at 10 MW (many zero deltas), with a handful of larger ramps.
    gross_load = [10.0] * 30 + [
        10.0,
        50.0,
        10.0,
        50.0,
        10.0,
        90.0,
        10.0,
        5.0,
        10.0,
        60.0,
    ]
    cems_working, col_dict = _ramp_rate_input(gross_load)

    rank_split = _to_df(
        _summarize_ramp_rates(cems_working, UNIT_COLS, col_dict["generation_col"])
    )
    qcut = _to_df(
        _summarize_ramp_rates_qcut(cems_working, UNIT_COLS, col_dict["generation_col"])
    )

    assert rank_split.height == 1
    assert qcut.height == 1
    rank_up = rank_split["ramp_up_rate"][0]
    qcut_up = qcut["ramp_up_rate"][0]
    rank_down = rank_split["ramp_down_rate"][0]
    qcut_down = qcut["ramp_down_rate"][0]
    assert rank_up is not None and qcut_up is not None
    # The two methods should differ meaningfully here -- if this starts failing,
    # it likely means one of the implementations changed, not that they've
    # converged coincidentally.
    assert rank_up != pytest.approx(qcut_up) or rank_down != pytest.approx(qcut_down)


def test_ramp_rate_methods_are_close_with_all_distinct_deltas():
    """With no tied ramp-rate values, duplicates="drop" never triggers.

    In that case pandas.qcut(q=20) and the rank-based top/bottom-5% split select
    nearly the same extreme observations, so the two methodologies should be
    close -- but not necessarily exactly equal, since qcut's quantile-fraction
    bin edges don't divide unevenly-sized samples into exactly the same
    equal-count groups that a strict rank-based head/tail split does. This test
    checks the divergence stays small (a few percent) here, in contrast to the
    tied-delta case above where it does not.
    """
    rng = np.random.default_rng(7)
    # Monotonically distinct gross loads -> guaranteed-distinct ramp rates.
    gross_load = list(np.cumsum(rng.uniform(0.5, 5, size=60)))
    cems_working, col_dict = _ramp_rate_input(gross_load)

    rank_split = _to_df(
        _summarize_ramp_rates(cems_working, UNIT_COLS, col_dict["generation_col"])
    )
    qcut = _to_df(
        _summarize_ramp_rates_qcut(cems_working, UNIT_COLS, col_dict["generation_col"])
    )

    assert rank_split["ramp_up_rate"][0] == pytest.approx(
        qcut["ramp_up_rate"][0], rel=0.05
    )
    assert rank_split["ramp_down_rate"][0] == pytest.approx(
        qcut["ramp_down_rate"][0], rel=0.05
    )


def test_ramp_rate_qcut_handles_too_few_observations():
    """qcut can't form 20 quantile bins from a handful of points, but shouldn't crash.

    The original script's pandas.qcut(q=20) still runs (with fewer effective
    bins) even when a unit has far fewer than 20 valid ramp-rate observations,
    unlike the vectorized method which drops such units entirely via
    ``.having(pl.len() >= 20)``. This test just pins down that the qcut
    reference implementation degrades gracefully rather than raising.
    """
    cems_working, col_dict = _ramp_rate_input([10.0, 20.0, 15.0, 25.0])
    qcut = _to_df(
        _summarize_ramp_rates_qcut(cems_working, UNIT_COLS, col_dict["generation_col"])
    )
    assert qcut.height == 1
    assert qcut["ramp_up_rate"][0] is not None

    rank_split = _to_df(
        _summarize_ramp_rates(cems_working, UNIT_COLS, col_dict["generation_col"])
    )
    # rank_split's `.having(len >= 20)` excludes small-sample units entirely.
    assert rank_split.height == 0


# ---------------------------------------------------------------------------
# estimate_operational_characteristics_by_unit (end-to-end fixture)
# ---------------------------------------------------------------------------


def _three_unit_fixture() -> pl.LazyFrame:
    """A hand-built fixture exercising the pipeline's main branches.

    - Unit ("1", "STABLE"): two load levels. The high level is held for two
      separate runs (10 and 12 consecutive hours), comfortably clearing the
      8-hour stable threshold; two down (null) gaps of length 3 and 1 give a
      known minimum down time; the two up runs give a known minimum up time.
      This unit sorts first within the fixture, which used to matter (before
      ``_add_run_id``'s first-row bug was fixed) because whichever unit sorted
      first in the pipeline's internal frames got a spurious length-1 run split
      off its first qualifying run -- see
      ``test_add_run_id_first_row_of_frame_starts_a_new_run``. No longer an
      issue, so this fixture no longer needs a sacrificial leading unit.
    - Unit ("2", "TOO_SHORT"): the same two load levels, but each run is only 3
      hours -- never long enough to register as a stable level, so the
      stable-level-dependent outputs should come back null while the
      max-load-factor heat rate (which doesn't depend on a stable level) should
      still be populated.
    - Unit ("3", "CONSTANT"): a single load level throughout (load_factor_nunique
      == 1), which should short-circuit to an all-null row except for
      max_gross_load_mw.
    """
    stable = _hourly_cems(
        [3.0, 3.0, 3.0, None] + [9.0] * 10 + [None, None, None] + [9.0] * 12 + [None],
        plant_id_epa=1,
        emissions_unit_id_epa="STABLE",
    )
    too_short = _hourly_cems(
        ([2.0, 2.0, 2.0, 8.0, 8.0, 8.0] * 4),
        plant_id_epa=2,
        emissions_unit_id_epa="TOO_SHORT",
    )
    constant = _hourly_cems(
        [5.0] * 20,
        plant_id_epa=3,
        emissions_unit_id_epa="CONSTANT",
    )
    return pl.concat([stable, too_short, constant])


@pytest.fixture(scope="module")
def three_unit_result() -> pd.DataFrame:
    cems = _three_unit_fixture()
    result = _to_df(
        estimate_operational_characteristics_by_unit(
            cems, min_stable_consecutive_hours=8
        )
    ).to_pandas()
    return result.set_index("emissions_unit_id_epa")


def test_stable_unit_min_up_and_down_times(three_unit_result):
    row = three_unit_result.loc["STABLE"]
    assert row["max_gross_load_mw"] == 9.0
    # Shortest of the two qualifying up-runs (10 and 12 hours).
    assert row["min_up_time_hours"] == 10
    # Shortest of the two down gaps (3 and 1 hours); a lone leading/trailing
    # null hour also counts as a length-1 down run.
    assert row["min_down_time_hours"] == 1
    assert row["min_stable_level"] is not None
    assert not np.isnan(row["min_stable_level"])


def test_stable_unit_heat_rates_are_constant_input_value(three_unit_result):
    row = three_unit_result.loc["STABLE"]
    # heat_content_mmbtu was constructed as 7.5 * gross_load_mw everywhere, so
    # every valid hourly heat rate is exactly 7.5.
    assert row["heat_rate_at_max_load_factor_mmbtu_per_mwh"] == pytest.approx(7.5)
    assert row["heat_rate_at_min_stable_level_mmbtu_per_mwh"] == pytest.approx(7.5)


def test_too_short_unit_has_no_stable_level_but_has_max_load_heat_rate(
    three_unit_result,
):
    row = three_unit_result.loc["TOO_SHORT"]
    assert row["max_gross_load_mw"] == 8.0
    assert row["min_stable_level"] is None or np.isnan(row["min_stable_level"])
    assert row["min_up_time_hours"] is None or np.isnan(row["min_up_time_hours"])
    assert row["min_down_time_hours"] is None or np.isnan(row["min_down_time_hours"])
    # Max-load heat rate doesn't depend on a stable level being found.
    assert row["heat_rate_at_max_load_factor_mmbtu_per_mwh"] == pytest.approx(7.5)
    assert row["heat_rate_at_min_stable_level_mmbtu_per_mwh"] is None or np.isnan(
        row["heat_rate_at_min_stable_level_mmbtu_per_mwh"]
    )


def test_constant_load_unit_is_all_null_except_max_load(three_unit_result):
    row = three_unit_result.loc["CONSTANT"]
    assert row["max_gross_load_mw"] == 5.0
    for col in [
        "min_stable_level",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_level_mmbtu_per_mwh",
    ]:
        assert row[col] is None or np.isnan(row[col])


def test_binning_method_config_produces_equivalent_end_to_end_output():
    """The vectorized and pandas_cut binning methods should agree end-to-end.

    Complements test_vectorized_bins_match_pandas_cut_on_random_data by checking
    equivalence after the full pipeline (stable-level detection, heat rates, up
    and down times), not just at the bin-assignment step.
    """
    cems = _three_unit_fixture()
    pandas_cut_result = _to_df(
        estimate_operational_characteristics_by_unit(
            cems,
            min_stable_consecutive_hours=8,
            load_factor_binning_method="pandas_cut",
        )
    ).sort(UNIT_COLS)
    vectorized_result = _to_df(
        estimate_operational_characteristics_by_unit(
            cems,
            min_stable_consecutive_hours=8,
            load_factor_binning_method="vectorized",
        )
    ).sort(UNIT_COLS)

    compare_cols = [
        "max_gross_load_mw",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_level_mmbtu_per_mwh",
    ]
    for col in compare_cols:
        pd_vals = pandas_cut_result[col].to_list()
        vec_vals = vectorized_result[col].to_list()
        for pd_val, vec_val in zip(pd_vals, vec_vals, strict=True):
            if pd_val is None or (isinstance(pd_val, float) and np.isnan(pd_val)):
                assert vec_val is None or (
                    isinstance(vec_val, float) and np.isnan(vec_val)
                )
            else:
                assert pd_val == pytest.approx(vec_val), col
