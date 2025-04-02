"""Test timeseries plotting functions."""

import pandas as pd

from pudl.analysis.timeseries_evaluation import (
    plot_correlation,
    plot_imputation,
)


def test_plot_imputation(pudl_io_manager, mocker, asset_value_loader):
    """Test that plot function doesn't error."""
    mocker.patch("pudl.analysis.timeseries_evaluation.plt.show")
    eia930_sub = asset_value_loader.load_asset_value(
        "out_eia930__hourly_subregion_demand"
    )

    idx_cols = [
        "balancing_authority_code_eia",
        "balancing_authority_subregion_code_eia",
    ]
    reported_col = "demand_reported_mwh"
    imputed_col = "demand_imputed_pudl_mwh"

    plot_imputation(
        eia930_sub,
        idx_cols=idx_cols,
        idx_vals=("SWPP", "OPPD"),
        start_date="2019-11-01",
        end_date="2019-12-31",
        reported_col=reported_col,
        imputed_col=imputed_col,
    )


def test_plot_correlation(pudl_io_manager, mocker, asset_value_loader):
    """Test that plot function doesn't error."""
    mocker.patch("pudl.analysis.timeseries_evaluation.plt.show")
    eia930_sub = asset_value_loader.load_asset_value(
        "out_eia930__hourly_subregion_demand"
    )
    eia930_ops = asset_value_loader.load_asset_value("core_eia930__hourly_operations")

    eia930_bas = eia930_ops.loc[
        :, ["balancing_authority_code_eia", "datetime_utc", "demand_reported_mwh"]
    ].set_index(["balancing_authority_code_eia", "datetime_utc"])
    eia930_sub_agg = (
        eia930_sub.groupby(["balancing_authority_code_eia", "datetime_utc"])[
            "demand_reported_mwh"
        ]
        .sum()
        .to_frame()
    )
    eia930_both = pd.merge(
        eia930_bas,
        eia930_sub_agg,
        left_index=True,
        right_index=True,
        how="inner",
        suffixes=("_ba", "_sub"),
    )
    all_bas = list(eia930_both.index.get_level_values(0).unique())

    plot_correlation(
        eia930_both.reset_index(),
        idx_cols=["balancing_authority_code_eia"],
        idx_vals=all_bas,
        timeseries_x="demand_reported_mwh_ba",
        timeseries_y="demand_reported_mwh_sub",
        xlabel="BA Reported Demand [MWh]",
        ylabel="Aggregated Subregion Demand [MWh]",
        title="Correlation between BA Reported Demand and Aggregated Subregion Demand",
        xylim=(1e3, 2e5),
        alpha=0.1,
    )
