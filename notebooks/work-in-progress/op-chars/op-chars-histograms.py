import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import os

    import marimo as mo
    import matplotlib.pyplot as plt
    import matplotx
    import numpy as np
    import polars as pl

    PUDL_OUTPUT = os.environ["PUDL_OUTPUT"]

    # Large-format style for full-column / 4K-legible plots: onedark theme
    # plus text sized for a 3000px+-wide figure rather than a default-size one.
    PLOT_STYLE = [
        matplotx.styles.onedark,
        {
            "font.size": 22,
            # Figure suptitle and per-axes titles pinned to the same absolute
            # size (26.4pt -- what figure.titlesize's default 'large' keyword
            # happened to resolve to) rather than axes.titlesize's previous,
            # larger 32pt, so the two title tiers read as the same size.
            "axes.titlesize": 26.4,
            "figure.titlesize": 26.4,
            "axes.labelsize": 26,
            "xtick.labelsize": 20,
            "ytick.labelsize": 20,
            "legend.fontsize": 20,
        },
    ]

    HIST_COLOR = "#61afef"
    return HIST_COLOR, PLOT_STYLE, PUDL_OUTPUT, mo, np, pl, plt


@app.cell
def _(mo):
    mo.md("""
    ## `out_epacems__yearly_operational_characteristics` histograms
    """)
    return


@app.cell
def _(PUDL_OUTPUT, pl):
    op_chars = pl.read_parquet(
        f"{PUDL_OUTPUT}/parquet/out_epacems__yearly_operational_characteristics.parquet"
    )
    NUMERIC_COLS = [
        "max_gross_load_mw",
        "min_stable_load_factor",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_load_factor_mmbtu_per_mwh",
        "ramp_up_rate_per_min",
        "ramp_down_rate_per_min",
    ]
    op_chars.select(NUMERIC_COLS).describe()
    return NUMERIC_COLS, op_chars


@app.cell
def _(HIST_COLOR, NUMERIC_COLS, PLOT_STYLE, op_chars, plt):
    N_BINS = 50

    with plt.style.context(PLOT_STYLE):
        hist_fig, hist_axes = plt.subplots(
            4, 2, figsize=(20, 28), dpi=150
        )  # 4 rows x 2 cols, one panel per numeric column

        for _col, _ax in zip(NUMERIC_COLS, hist_axes.flat, strict=True):
            _vals = op_chars[_col].drop_nulls().to_numpy()
            _ax.hist(_vals, bins=N_BINS, color=HIST_COLOR, alpha=0.85)
            _ax.set_title(_col)
            _ax.set_ylabel("Count")
            _ax.grid(True, alpha=0.25)

        hist_fig.suptitle(
            f"{op_chars.height} units -- {N_BINS}-bin histograms of "
            "operational characteristics"
        )
        hist_fig.tight_layout()

    hist_fig
    return


@app.cell
def _(HIST_COLOR, PLOT_STYLE, np, op_chars, plt):
    HR_COLS = [
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_load_factor_mmbtu_per_mwh",
    ]
    HR_MIN_BOUND = 5.4
    HR_MAX_BOUND = 25.0
    HR_XLIM = (0, 30)
    HR_N_BINS = 100
    hr_bin_edges = np.linspace(*HR_XLIM, HR_N_BINS + 1)

    with plt.style.context(PLOT_STYLE):
        hr_fig, hr_axes = plt.subplots(
            2, 1, figsize=(20, 14), dpi=150, sharex=True
        )  # full-width, stacked

        for _col, _ax in zip(HR_COLS, hr_axes, strict=True):
            _vals = op_chars[_col].drop_nulls().to_numpy()
            _ax.hist(_vals, bins=hr_bin_edges, color=HIST_COLOR, alpha=0.85)
            _ax.axvline(
                HR_MIN_BOUND,
                color="#e06c75",
                linestyle="--",
                linewidth=2,
                label=f"min plausible ({HR_MIN_BOUND})",
            )
            _ax.axvline(
                HR_MAX_BOUND,
                color="#e5c07b",
                linestyle="--",
                linewidth=2,
                label=f"max plausible ({HR_MAX_BOUND})",
            )
            _ax.set_xlim(*HR_XLIM)
            _ax.set_title(_col)
            _ax.set_ylabel("Count")
            _ax.grid(True, alpha=0.25)
            _ax.legend(frameon=False, loc="upper right")

        hr_axes[-1].set_xlabel("Heat rate (MMBtu/MWh)")
        hr_fig.suptitle(
            f"Heat rate distributions with plausible bounds "
            f"({HR_MIN_BOUND}-{HR_MAX_BOUND} MMBtu/MWh)"
        )
        hr_fig.tight_layout()

    hr_fig
    return


@app.cell
def _():
    return


@app.cell
def _(HIST_COLOR, PLOT_STYLE, np, op_chars, plt):
    UPDOWN_COLS = [
        "min_up_time_hours",
        "min_down_time_hours",
    ]
    UPDOWN_N_BINS = 50

    # Shared bin edges across both panels (spanning both columns' combined
    # range) so bin widths match and the two histograms are directly comparable.
    _updown_vals = {
        _col: op_chars[_col].drop_nulls().to_numpy() for _col in UPDOWN_COLS
    }
    _updown_min = min(_v.min() for _v in _updown_vals.values())
    _updown_max = max(_v.max() for _v in _updown_vals.values())
    updown_bin_edges = np.linspace(_updown_min, _updown_max, UPDOWN_N_BINS + 1)

    with plt.style.context(PLOT_STYLE):
        updown_fig, updown_axes = plt.subplots(
            2, 1, figsize=(20, 14), dpi=150, sharex=True
        )  # full-width, stacked

        for _col, _ax in zip(UPDOWN_COLS, updown_axes, strict=True):
            _ax.hist(
                _updown_vals[_col], bins=updown_bin_edges, color=HIST_COLOR, alpha=0.85
            )
            _ax.set_yscale("log")
            _ax.set_title(_col)
            _ax.set_ylabel("Count (log scale)")
            _ax.grid(True, alpha=0.25)

        updown_axes[-1].set_xlabel("Hours")
        updown_fig.suptitle("Min up-time / down-time distributions")
        updown_fig.tight_layout()

    updown_fig
    return


if __name__ == "__main__":
    app.run()
