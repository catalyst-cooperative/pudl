# Sylvan heat rates: derived plant characteristics

This document orients a new maintainer to the `derived_plant_characteristics` project:
what the analysis does, where the polars rewrite still diverges from the original pandas
script, what's slow, and what to test before retiring the old script.

Companion scripts (read-only against local parquet, do not touch either the original
script or the new module) live next to this file in
`notebooks/work-in-progress/sylvan-heat-rates/`:

- `isolated_component_comparison.py` — runs the original per-unit pandas algorithm
  (copied verbatim) and the new polars pipeline against the **same** local CEMS
  rows, and diffs the results column by column. Has a real CLI now (`--help` for
  options); see "Comparing the two pipelines" below for why same-input comparison
  matters, and "Generating a current-vintage reference" below for usage.

## What the script does

For every EPA CEMS emissions unit (`plant_id_epa` + `emissions_unit_id_epa`), the
analysis turns three years of hourly gross-load and heat-input readings into a
handful of physically meaningful operating parameters:

- `max_gross_load_mw` — the unit's observed capacity (max hourly gross load).
- `min_stable_level` — the lowest load factor bin the unit can sustain for a
  meaningful stretch (default: 8+ consecutive hours) without shutting down.
- `min_up_time_hours` / `min_down_time_hours` — shortest observed runs at/above
  the stable level, and shortest observed outages.
- `heat_rate_at_max_load_factor_mmbtu_per_mwh` / `..._at_min_stable_level_...` —
  fuel efficiency at full output vs. minimum stable output.
- `ramp_up_rate_fraction_of_max_gross_load_per_min` / `ramp_down_rate_...` — how
  fast the unit can change output, expressed as a fraction of its capacity.

These are the kinds of parameters production-cost models (e.g. dispatch/capacity
expansion models) need per generator and that aren't reported anywhere in EIA-860.
The original script also builds an "ADJUSTED" variant that rescales EPA CEMS gross
load into an estimated EIA-923 net generation using a plant-level linear/constant
fit — that half of the pipeline hasn't been ported to polars yet (it's stubbed out,
commented, at the bottom of `derived_plant_characteristics.py`).

### The core algorithm, per unit

1. **Bin hourly load factor into 10 bins.** `load_factor = gross_load_mw /
   max_gross_load_mw` for that unit, then `pd.cut(load_factor, bins=10, right=True,
   include_lowest=False)` — ten *equal-width* bins spanning that unit's own
   observed min/max load factor (not a fixed 0–1 scale).

   ```python
   load_factor_bin = pd.cut(
       load_factor, bins=10, right=True, include_lowest=False
   )
   ```

2. **Find the minimum stable level.** Walk the bins from lowest to highest (skipping the
   lowest bin, which is treated as "off/startup" noise) and return the first bin
   containing a run of ≥8 *consecutive clock hours* (a gap of more than 1 hour breaks
   the run):

   ```python
   run_id = hours.diff().dt.total_seconds().div(3600).ne(1).cumsum()
   stable = d.groupby(run_id).size().max() >= consecutive_hours
   ```

3. **Min up/down time.** Among hours at or above the stable bin, find the *shortest*
   unbroken run (that's the minimum observed up-time); among hours with a null load
   factor (the unit was off), find the shortest unbroken run of outage hours.

4. **Heat rates.** Median `heat_rate_mmbtu_per_mwh` within the top bin and within the
   min-stable bin.

5. **Ramp rates.** Hour-over-hour `Δgross_load_mwh / Δhours`, split into 20 quantile
   bins (`pd.qcut(..., q=20)`), and take the median ramp rate within the bottom bin
   (ramp-down) and top bin (ramp-up).

The original script does this with a Python `for` loop over every `(plant_id_epa,
emissions_unit_id_epa)` pair, each iteration re-filtering a full in-memory pandas
DataFrame — O(units × rows). The new module vectorizes all five steps using polars
group-by/window expressions across *all* units at once. It also has a fully
expression-based replacement for the `pd.cut` binning step
(`assign_load_factor_bins_vectorized`, now the default) — see "Performance" below
for the speedup and "A/B-testable code paths" for how to compare it against the
original per-unit pandas fallback that's still available alongside it.

## Comparing the two pipelines

`isolated_component_comparison.py` runs the original pandas per-unit algorithm against
the same local parquet rows fed to the new pipeline. Results for CA, 2022–2025 (current
vintage), 245 plant-unit pairs, with the `_add_run_id` bug (see below) already fixed:

| column | units compared | >1% relative diff | >10% relative diff |
|---|---|---|---|
| `max_gross_load_mw` | 240 | 0 | 0 |
| `heat_rate_at_max_load_factor_mmbtu_per_mwh` | 240 | 0 | 0 |
| `min_stable_level` | 240 | 1 | 1 |
| `heat_rate_at_min_stable_level_mmbtu_per_mwh` | 240 | 1 | 0 |
| `min_down_time_hours` | 240 | 5 | 5 |
| `min_up_time_hours` | 240 | 7 | 7 |
| `ramp_up_rate_fraction_of_max_gross_load_per_min` | 240 | 43 | 10 |
| `ramp_down_rate_fraction_of_max_gross_load_per_min` | 240 | 54 | 8 |

Given identical inputs, the two pipelines agree almost exactly on capacity, min stable
level, and heat rates. Two real divergences are worth understanding before retiring the
old script:

### 1. Ramp rate binning method differs (the significant one)

The original script bins ramp rates with `pd.qcut(ramp_rate, q=20, duplicates="drop")` —
a genuine 20-*quantile* split, where `duplicates="drop"` collapses adjacent bin edges
that land on the same value (very common here: a lot of hours have `ramp_rate == 0`, or
the unit has too few distinct values to make 20 clean quantiles). It then takes the
median of *whichever bin ends up lowest/highest after collapsing*.

The new module's `_summarize_ramp_rates` instead sorts the ramp rates and takes the
median of the bottom/top `n/20` rows by **rank** — always exactly 5% of the row count,
with no duplicate-collapsing behavior:

```python
bin_expression = (pl.len() / 20).cast(pl.Int64)
ramp_down_rate = pl.col("ramp_rate").sort().head(bin_expression).median()
ramp_up_rate = pl.col("ramp_rate").sort().tail(bin_expression).median()
```

These are similar in spirit but not equivalent whenever there are ties at the extremes —
which is the normal case for ramp rate data dominated by near-zero deltas. This is why
~17% of units differ by more than 1% in ramp rate outputs even with identical input
rows. It's not really a bug — it's a reinterpretation of "bottom/top bin" — but it is a
difference in methodology. Also note `_summarize_ramp_rates` silently drops any unit
with fewer than 20 ramp-rate observations (`.having(pl.len() >= 20)`), while the
original `pd.qcut` would still produce *some* answer (with fewer effective bins) for
small-sample units. That's a second, smaller behavioral change bundled into the same
function.

### 2. `min_up_time_hours`/`min_down_time_hours` disagree for a handful of units — fixed one real bug, but it wasn't the whole story

`_add_run_id` had a small bug: it built its "same unit/bin and consecutive hour" boolean
with `pl.col(c).eq(pl.col(c).shift())`, which is `null` (not `True`/`False`) for the
very first row of whatever frame it's called on, since `.shift()` has nothing to compare
against there. Its sibling helper, `consecutive_run_ids()` (a few lines above it in the
same file), guards against exactly this with `.fill_null(True)` before `.cum_sum()` —
`_add_run_id` didn't, so `cum_sum()` propagated that `null` instead of starting run 0,
splitting the first row of a run off into its own spurious length-1 "run" for whichever
plant-unit happened to sort first in whatever frame was passed in. Fixed by moving
`.fill_null(True)` onto the final combined expression (mirroring
`consecutive_run_ids()`) rather than onto each individually-shifted column. This bug is
pinned down by a regression test
(`test_add_run_id_first_row_of_frame_starts_a_new_run`).

**However:** re-running the same-input comparison after the fix produced the same
divergence counts as before it (7/240 and 5/240 units, identical `max_abs_diff` values).
There is some other cause of the `min_up_time_hours`/ `min_down_time_hours` divergence
in real data. The real cause of most of these mismatches is still open. One lead worth
checking: `filter_for_min_stable_bin` compares `load_factor_bin`'s struct fields
(`left`/`right`) against `min_stable_bin`'s with `>=`, which should be logically
equivalent to an ordinal comparison for well-formed bins, but floating-point precision
differences introduced by the various joins/casts in the pipeline could cause a row
whose bin is numerically identical to fail that comparison by a hair — excluding one
hour from the middle of what should be a longer run and splitting it into two shorter
ones. Several of the mismatched units show a suspicious "off by exactly 1 hour" pattern
(e.g. original 2.0 vs. new 1.0, or 4.0 vs. 1.0) consistent with this, but it hasn't been
confirmed.

### Minor, cosmetic difference

The original script rounded heat rates and ramp fractions to 2 decimal places before
output — that rounding was cosmetic and has intentionally been dropped in the new
pipeline; full precision is kept and can be formatted at presentation time as needed.

## Performance: the pandas fallback is gone by default

The original bottleneck here was the `pd.cut` fallback in
`assign_groupwise_load_factor_bins`: it forced a full `.collect()` of the entire
filtered CEMS LazyFrame into memory as pandas, then ran `groupby(...).apply(pd.cut)` — a
Python-level loop over every unit, the same O(units) pattern the original script used,
just moved one level deeper. That's now been replaced by
`assign_load_factor_bins_vectorized`, a fully expression-based polars equivalent using
per-unit window aggregates instead of a per-unit Python loop, validated against
`pd.cut`'s exact edge-padding behavior by dedicated unit tests (see "Test suite" below)
and now the **default** `load_factor_binning_method`. The original `pd.cut` fallback is
still available (`load_factor_binning_method="pandas_cut"`) for A/B comparison, but
nothing depends on it by default anymore. Both paths still collect to an eager
`pl.DataFrame` right after binning, same as before.

**Measured effect, isolating just the binning step** (same input, 5 repetitions, median
reported):

| | CA, wall-clock | CA, peak Python-heap | CO, wall-clock | CO, peak Python-heap |
|---|---|---|---|---|
| `pandas_cut` | 20.3s | 1029 MB | 6.7s | 289 MB |
| `vectorized` | 1.3s | 1.1 MB | 0.3s | 0.5 MB |

That's a ~15-20x speedup and several-hundred-fold reduction in Python-heap
allocation from removing the one remaining pandas step. (Note: the memory numbers
undercount `vectorized`'s true memory use, since `tracemalloc` only tracks
Python-heap allocations, not polars' native/Rust-side memory used by either
path.)

### What this changes about the fan-out question

Given how fast and memory-light the vectorized pipeline is per state — even California,
one of the largest states, computes in about a second — the per-asset overhead of
Dagster's own scheduling/subprocess machinery outweighs the benefit that comes from
parallelizing across states via 51 separate assets.
`operational_characteristics_factory` (the per-state asset factory) plus
`out_epacems__yearly_operational_characteristics` (the combiner) still exist and still
work, but an alternative `out_epacems__yearly_operational_characteristics_single_asset`
that defines one asset that loops over every state internally (materializing each
state's result as it goes, same as the fan-out does per-asset) and writes one output,
instead of fanning out through Dagster's `AssetIn()` kwargs machinery. Measured: the
full 51-asset fan-out (forced fresh via `dg launch --assets
"+out_epacems__yearly_operational_characteristics"`) took ~102s; the single-asset loop
took ~62s — most of that gap is genuine per-step Dagster overhead
(subprocess/step-worker startup × 51), not computation, which is already fast either way
once the pandas binning fallback is gone. Both were verified to produce byte-for-byte
identical output (4,223/4,223 rows, all 8 metrics, zero divergence) against each other.

If a calculation of these operational characteristics on a per-year basis is added
later, the single-asset-loop pattern would be much simpler and more efficient than
fanning out through Dagster's asset graph with thousands of assets in the DAG.

### Other smaller improvements

- `filter_cems_for_heat_rate_analysis` explicitly `.select()`s only the 9
  columns it needs before filtering, important since
  `core_epacems__hourly_emissions` is one of the largest tables in PUDL.
- `estimate_operational_characteristics_by_unit` still interleaves lazy and
  eager operations -- `cems_working` becomes an eager `pl.DataFrame`
  immediately after binning (both methods), and everything downstream
  operates on that. Keeping the whole pipeline lazy end to end (a single
  unmaterialized query plan from the initial filter through to one final
  `.collect()`) was tried and reverted -- see above -- but remains a
  legitimate future option if the per-asset resource profile ever becomes a
  real constraint at full 50-state/multi-year scale.

## Generating a current-vintage reference, and how long an exhaustive comparison takes

`isolated_component_comparison.py` (in this directory) is the tool for this: it runs the
*original* per-unit pandas algorithm and the *new* polars pipeline against the same
local-parquet rows, for every plant-unit pair matching the configured state(s)/years,
and reports column-by-column divergence stats plus a full row-level CSV.

It's a real CLI now (`click`-based), defaulting to the **current** data vintage (3
years through the end of 2025, matching what
`out_epacems__yearly_operational_characteristics` actually materializes today via
`max_full_year`), and to California:

```bash
# See all options
pixi run python notebooks/work-in-progress/sylvan-heat-rates/isolated_component_comparison.py --help

# Default: CA, 2022-2025 (current vintage)
pixi run python notebooks/work-in-progress/sylvan-heat-rates/isolated_component_comparison.py

# A different vintage or state set
pixi run python notebooks/work-in-progress/sylvan-heat-rates/isolated_component_comparison.py \
  --final-year 2024 --num-years 3 --states CA,TX
```

Each run writes a vintage-stamped CSV (e.g. `isolated_comparison_full_CA_2022-2025.csv`)
rather than overwriting a single fixed filename, so you can keep multiple
vintages/state-sets side by side for comparison as you iterate on methodology decisions.
This intentionally stays outside the pytest unit suite (per the project's fixture
constraints, anything touching real ETL-scale local parquet data doesn't belong in
`tests/unit/`) — it's a standalone script for exactly this kind of
investigative/validation work.

**Timing:** the exhaustive comparison for **all 245 California plant-unit pairs**, 3
years of hourly data (2022-2025, ~8.5 million hourly CEMS rows), running *both* the
original per-unit pandas algorithm *and* the new polars pipeline and diffing them,
takes about a minute wall-clock. Nearly all of that is the pandas per-unit reference
loop -- it's the same O(units) pattern as the original script, and it's what this
script exists to check against, not something to optimize. The new pipeline's own
contribution to that minute is on the order of a second or two (see "Performance"
above). Extrapolating: all 50 states' current-vintage data would be considerably more
(California is a large but not dominant share of total EPA CEMS units), but this
confirms the comparison methodology itself is fast enough to run
interactively/iteratively per state while you're deciding on binning methodologies,
rather than needing a full 50-state batch job for every iteration.

## Test suite (implemented)

`tests/unit/analysis/derived_plant_characteristics_test.py` now exists and passes
(14 tests, `pixi run pytest --no-cov tests/unit/analysis/derived_plant_characteristics_test.py`).
It covers:

1. `consecutive_run_ids` — gap handling and an exact-threshold run length.
2. `assign_groupwise_load_factor_bins` vs. `assign_load_factor_bins_vectorized`
   (see "A/B-testable code paths" below) — **this is the test suite's direct
   answer to "do we have tests demonstrating the polars expression-based
   binning behaves exactly like `pd.cut`?"**: bin ordinals and edges agree on
   random data; a dedicated regression test pins down that `pandas.cut` only
   pads `bins[0]`, not all ten bin edges (a real bug caught while building the
   vectorized version, before it ever shipped).
3. `_summarize_ramp_rates` (rank-split) vs. `_summarize_ramp_rates_qcut` (the
   original's `pandas.qcut(q=20, duplicates="drop")`, faithfully reimplemented) —
   one test shows they diverge meaningfully with tied/duplicate deltas (the
   realistic case), one shows they're close (not exact) with no ties, and one
   pins down that `qcut` degrades gracefully on <20 observations where
   `rank_split`'s `.having(len >= 20)` drops the unit entirely.
4. **Two tests for the (now-fixed) `_add_run_id` bug**:
   `test_add_run_id_first_row_of_frame_starts_a_new_run` demonstrates the fix
   (row 0 correctly joins run 1, rather than coming back `null`/spurious); and
   `test_add_run_id_handles_struct_unit_cols` is a coverage test for the
   specific call shape (`unit_cols` including a Struct column,
   `load_factor_bin`) that an earlier, incorrect fix attempt crashed on. Both
   are retained as a demonstration/regression suite, not just as a historical
   record of the bug.
5. `compute_stable_runs`/`calculate_min_up_or_down_times` end-to-end, via a
   3-unit fixture (`_three_unit_fixture`) covering: a unit with a clean
   8+-hour stable run and known min up/down times; a unit whose runs are
   always too short to register a stable level (checking that
   `heat_rate_at_max_load_factor` still populates even though
   stable-level-dependent outputs stay null); and a constant-load unit
   (`load_factor_nunique == 1`) that should come back all-null except
   `max_gross_load_mw`. (This fixture used to need a sacrificial leading
   "decoy" unit to absorb the `_add_run_id` frame-start bug so it didn't
   contaminate the other assertions -- no longer necessary now that the bug is
   fixed, so it's been simplified back down to just the three units that
   matter.)
6. An end-to-end equivalence check between the `pandas_cut` and `vectorized`
   binning methods through the *full* pipeline (not just the bin-assignment
   step).

**Not yet implemented, per your instruction:** dbt data validation tests on the
materialized table (row-count-in-range per state, `min_stable_level` between 0 and 1,
ramp fractions within a sane per-minute bound, primary-key not-null, etc.). Worth adding
once the asset is wired into the real pipeline and you've settled on binning
methodologies.

## A/B-testable code paths (implemented, non-destructive)

`derived_plant_characteristics.py` exposes two config fields on
`HEAT_RATE_ANALYSIS_CONFIG_SCHEMA`. **Both now default to the new, recommended
behavior** (flipped from their original "keep everything unchanged" defaults,
for demonstration purposes, now that the empirical case for `vectorized` is
strong and `rank_split` is the intended long-term direction for ramp rates):

- **`load_factor_binning_method`**: `"vectorized"` (default) routes through
  `assign_load_factor_bins_vectorized()` — the fully expression-based
  replacement for the per-unit `pd.cut` fallback discussed
  above. `"pandas_cut"` routes through the original
  `assign_groupwise_load_factor_bins()`, kept for A/B comparison. Both produce
  the identical output schema, so either is a true drop-in for the other.
- **`ramp_rate_binning_method`**: `"rank_split"` (default) routes through the
  existing `_summarize_ramp_rates()`. `"qcut"` routes through
  `_summarize_ramp_rates_qcut()` — a faithful reimplementation of the original
  script's `pandas.qcut(q=20, duplicates="drop")` per-unit binning, kept for
  A/B comparison.

Both are threaded through `estimate_operational_characteristics_by_unit(...,
load_factor_binning_method=..., ramp_rate_binning_method=...)` -- whose own
Python-level defaults match the config schema's, so calling it directly
(scripts, tests, notebooks) also demonstrates the new behavior by default --
and through the Dagster asset config, so you can run any of the 2×2
combinations either by calling the function directly (fastest way to iterate)
or via a `dg launch` config override. The original code paths are untouched —
nothing was deleted or rewritten in place, only the defaults changed.

### Empirical basis for choosing between the methods

After both fixes, on real CA data (final_year=2024, num_years=3, 247 units),
`pandas_cut` and `vectorized` binning agree exactly on every downstream output except
`min_stable_level`, which differs by at most 0.0005 for 9/240 units (tiny
floating-point-scale noise between `np.linspace`-computed edges and the vectorized
algebraic edges, not a methodology difference). That's a strong empirical case for
`vectorized` as a full replacement — see "How long would an exhaustive comparison take?"
below for how to extend this check to every unit, not just California.

The ramp-rate methods are a different story: `rank_split` and `qcut` regularly disagree
by more than 10% for units with many tied/duplicate ramp-rate values (the normal case),
because `qcut(duplicates="drop")` collapses tied quantile edges in a data-dependent way
that a strict rank-based split doesn't reproduce. This is a genuine methodology choice,
not a bug to fix — see the recommendation below.

### Does `rank_split`'s `.having(len >= 20)` drop actually matter in practice?

Checked directly against real EPA CEMS data (3 years, current vintage): for
**California, no** — 0 of 240 units with a valid load factor fall under 20 ramp-rate
observations; the smallest has 187. But **nationwide, yes, for a small tail**: 38 of
3,796 units across all 50 states (~1%) have fewer than 20 ramp-rate observations, with
the smallest having just 1. A unit with 1-4 valid hourly transitions across 3 years of
data is barely operating at all, which suggests that it is a "mostly unusable / invalid
unit anyway" rather than a real loss of otherwise-good data.
