"""Methods for estimating redacted EIA-923 fuel price information."""
import logging
from collections import OrderedDict
from typing import Any, Literal, TypedDict

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sklearn.compose import make_column_selector, make_column_transformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from pudl.helpers import add_fips_ids, date_merge
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

logger = logging.getLogger(__name__)


STATE_TO_CENSUS_REGION = POLITICAL_SUBDIVISIONS.set_index('subdivision_code')['division_code_us_census'].to_dict()

class FuelPriceAgg(TypedDict):
    """A data structure for storing fuel price aggregation arguments."""

    agg_cols: list[str]
    fuel_group_eiaepm: Literal[
        "all",
        "coal",
        "natural_gas",
        "other_gas",
        "petroleum",
        "petroleum_coke",
    ]


FUEL_PRICE_AGGS: OrderedDict[str, FuelPriceAgg] = OrderedDict(
    {
        # The most precise estimator we have right now
        "state_esc_month": {
            "agg_cols": ["state", "energy_source_code", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        # Good for coal, since price varies much more with location than time
        "state_esc_year": {
            "agg_cols": ["state", "energy_source_code", "report_year"],
            "fuel_group_eiaepm": "coal",
        },
        # Good for oil products, because prices are consistent geographically
        "region_esc_month": {
            "agg_cols": ["census_region", "energy_source_code", "report_date"],
            "fuel_group_eiaepm": "petroleum",
        },
        # Less fuel specificity, but still precise date and location
        "state_fgc_month": {
            "agg_cols": ["state", "fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        # Less location and fuel specificity
        "region_fgc_month": {
            "agg_cols": ["census_region", "fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "region_fgc_year": {
            "agg_cols": ["census_region", "fuel_group_eiaepm", "report_year"],
            "fuel_group_eiaepm": "all",
        },
        "national_esc_month": {
            "agg_cols": ["energy_source_code", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "national_fgc_month": {
            "agg_cols": ["fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "national_fgc_year": {
            "agg_cols": ["fuel_group_eiaepm", "report_year"],
            "fuel_group_eiaepm": "all",
        },
    }
)
"""Fuel price aggregations ordered by precedence for filling missing values.

Precendece was largely determined by which aggregations resulted in the best
reproduction of reported fuel prices, as measured by the interquartile range of the
normalized difference between the estimate and the reported value:

(estimated fuel price - reported fuel price) / reported_fuel_price
"""


def weighted_median(df: pd.DataFrame, data: str, weights: str, dropna=True) -> float:
    """Calculate the median of the data column, weighted by the weights column.

    Suitable for use with df.groupby().apply().

    Args:
        df: DataFrame containing both the data whose weighted median we want to
            calculate, and the weights to use.
        data: Label of the column containing the data.
        weights: Label of the column containing the weights.
        dropna: If True, ignore rows where either data or weights are NA. If False, any
            NA value in either data or weights means the weighted median is also NA.

    Returns:
        A single weighted median value, or NA.
    """
    if dropna:
        df = df.dropna(subset=[data, weights])
    if df.empty | df[[data, weights]].isna().any(axis=None):
        return np.nan
    df = df.loc[:, [data, weights]].sort_values(data)
    midpoint = 0.5 * df[weights].sum()
    if (df[weights] > midpoint).any():
        w_median = df.loc[df[weights].idxmax(), data]
    else:
        cs_weights = df[weights].cumsum()
        idx = np.where(cs_weights <= midpoint)[0][-1]
        if cs_weights.iloc[idx] == midpoint:
            w_median = df[data].iloc[idx : idx + 2].mean()
        else:
            w_median = df[data].iloc[idx + 1]
    return w_median


def weighted_modified_zscore(
    df: pd.DataFrame,
    data: str,
    weights: str,
    dropna: bool = True,
) -> pd.Series:
    """Calculate the modified z-score using a weighted median.

    Args:
        df: DataFrame containing the data whose weighted modified z-score we want to
            calculate, and the weights to use when calculating median values.
        data: Label of the column containing the data.
        weights: Label of the column containing the weights.
        dropna: Whether to drop NA values when calculating medians. Passed through
            to :func:`weighted_median`

    Returns:
        Series with the same index as the input DataFrame,
    """
    wm = weighted_median(df, data=data, weights=weights, dropna=dropna)
    delta = (df[data] - wm).abs()
    return (0.6745 * delta) / delta.median()


def aggregate_price_median(
    frc: pd.DataFrame,
    aggs: OrderedDict[FuelPriceAgg] | None = None,
    agg_mod_zscore: list[str] | None = None,
    max_mod_zscore: float = 5.0,
    debug: bool = False,
) -> pd.DataFrame:
    """Fill in missing fuel prices with median values using various aggregations.

    Aggregations are done across space (state, census region, country), time (month or
    year), and fuel groups (coal, petroleum, natural gas).

    Args:
        frc: a Fuel Receipts and Costs dataframe from EIA 923.
        aggs: Ordered sequence of fuel price aggregations to apply.
        mod_zscore_agg: Columns to group by when identifying fuel, location, or time
            period specific outlying fuel prices.
        max_mod_zscore: The modified z-score beyond which a fuel price will be
            considered an outlier, get removed, and be filled in.
        debug: If True, retain intermediate columns used in the calculation.

    Returns:
        A Fuel Receipts and Costs table that includes fuel price estimates for all
        missing records and replaced outliers.
    """
    if aggs is None:
        aggs = FUEL_PRICE_AGGS
    if agg_mod_zscore is None:
        agg_mod_zscore = ["report_year", "fuel_group_eiaepm"]

    logger.info("Filling in missing fuel prices using weighted median values.")

    frc = frc.assign(
        report_year=lambda x: x.report_date.dt.year,
        census_region=lambda x: x.state.map(STATE_TO_CENSUS_REGION),
        fuel_cost_per_mmbtu_wm=np.nan,
        fuel_mmbtu_total=lambda x: x.fuel_received_units * x.fuel_mmbtu_per_unit,
    )

    # Identify outlying fuel prices using modified z-score and set them to NA
    mod_zscore = frc.groupby(agg_mod_zscore).apply(
        weighted_modified_zscore, data="fuel_cost_per_mmbtu", weights="fuel_mmbtu_total"
    )
    mod_zscore.index = mod_zscore.index.droplevel(level=agg_mod_zscore)
    frc["mod_zscore"] = mod_zscore
    frc["outlier"] = np.where(frc["mod_zscore"] > max_mod_zscore, True, False)
    frc.loc[frc["outlier"], "fuel_cost_per_mmbtu"] = np.nan
    frc["filled_by"] = np.where(frc["fuel_cost_per_mmbtu"].notna(), "original", pd.NA)

    n_outliers = sum(frc.outlier)
    n_samples = len(frc)
    frac_out = n_outliers / n_samples
    logger.info(
        f"Labeled {n_outliers}/{n_samples} fuel price records ({frac_out:0.2%}) as "
        f"outliers with mod_zscore > {max_mod_zscore}"
    )
    frac_mmbtu_out = (
        frc.loc[frc.outlier, "fuel_mmbtu_total"].sum() / frc.fuel_mmbtu_total.sum()
    )
    logger.info(f"Outliers account for {frac_mmbtu_out:0.2%} of all delivered MMBTU.")

    for agg in aggs:
        agg_cols = aggs[agg]["agg_cols"]
        fgc = aggs[agg]["fuel_group_eiaepm"]
        wm = frc.groupby(agg_cols).apply(
            weighted_median, data="fuel_cost_per_mmbtu", weights="fuel_mmbtu_total"
        )
        wm.name = agg
        frc = frc.merge(
            wm.to_frame().reset_index(), how="left", on=agg_cols, validate="many_to_one"
        )
        frc[agg + "_err"] = (
            frc[agg] - frc.fuel_cost_per_mmbtu
        ) / frc.fuel_cost_per_mmbtu
        mask = (
            # Only apply estimates to fuel prices that are still missing
            (frc.fuel_cost_per_mmbtu_wm.isna())
            # Using records where the current aggregation has a value
            & (frc[agg].notna())
            # Selectively apply to a single fuel group, if specified:
            & (True if fgc == "all" else frc.fuel_group_eiaepm == fgc)
        )
        # Label that record with the aggregation used to fill it:
        frc.loc[mask, "filled_by"] = agg
        # Finally, fill in the value:
        frc.loc[mask, "fuel_cost_per_mmbtu_wm"] = frc.loc[mask, agg]
        logger.info(
            f"Filled in {sum(mask)} missing fuel prices with {agg} "
            f"aggregation for fuel group {fgc}."
        )
    # Unless debugging, remove the columns used to fill missing fuel prices
    if not debug:
        cols_to_drop = list(aggs)
        cols_to_drop += list(c + "_err" for c in cols_to_drop)
        cols_to_drop += ["report_year", "census_region"]
        frc = frc.drop(columns=cols_to_drop)

    return frc


################################################################################
# scikit-learn approach with Hist Gradient Boosted Regressor
################################################################################


def load_pudl_features(
    pudl_engine: sa.engine.Engine, dp1_engine: sa.engine.Engine
) -> pd.DataFrame:
    """Assemble a dataframe of information relevant to fuel price imputation."""
    query = """
    SELECT
        frc.plant_id_eia,
        frc.report_date,
        frc.contract_type_code,
        frc.contract_expiration_date,
        frc.energy_source_code,
        frc.fuel_type_code_pudl,
        frc.mine_id_pudl,
        frc.supplier_name,
        frc.fuel_received_units,
        frc.fuel_mmbtu_per_unit,
        frc.sulfur_content_pct,
        frc.ash_content_pct,
        frc.mercury_content_ppm,
        frc.fuel_cost_per_mmbtu,
        frc.primary_transportation_mode_code,
        frc.secondary_transportation_mode_code,
        frc.natural_gas_transport_code,
        frc.natural_gas_delivery_contract_type_code,
        frc.moisture_content_pct,
        frc.chlorine_content_ppm,
        frc.fuel_group_code,
        -- for joining with annual data
        date(strftime('%Y', frc.report_date) || '-01-01') as first_day_of_report_year,

        mine.mine_name,
        mine.mine_type_code,
        mine.mine_id_msha,
        mine.county_id_fips as mine_county_id_fips,
        mine.state as mine_state,

        entity.latitude,
        entity.longitude,
        entity.state,
        entity.county,

        -- plants_eia860 is reported annually
        plants_860.ferc_cogen_status,
        plants_860.ferc_exempt_wholesale_generator,
        plants_860.ferc_small_power_producer,
        plants_860.iso_rto_code,
        plants_860.sector_name_eia,
        plants_860.natural_gas_pipeline_name_1,
        plants_860.natural_gas_pipeline_name_2,
        plants_860.natural_gas_pipeline_name_3,
        plants_860.regulatory_status_code,
        plants_860.water_source
    FROM fuel_receipts_costs_eia923 as frc
    LEFT JOIN coalmine_eia923 as mine
        USING (mine_id_pudl)
    LEFT JOIN plants_entity_eia as entity
        USING (plant_id_eia)
    LEFT JOIN plants_eia860 as plants_860
        on frc.plant_id_eia = plants_860.plant_id_eia
            AND first_day_of_report_year = plants_860.report_date
    ;
    """
    logger.info("Loading fuel receipts and costs data.")
    frc = pd.read_sql(query, pudl_engine)

    logger.info("Inferring coal mine locations via Census DP1.")
    mine_locations = (
        pd.read_sql(
            "county_2010census_dp1",
            dp1_engine,
            columns=["geoid10", "intptlat10", "intptlon10"],
        )
        .assign(
            mine_longitude=lambda x: pd.to_numeric(x.intptlon10),
            mine_latitude=lambda x: pd.to_numeric(x.intptlat10),
        )
        .rename(columns={"geoid10": "mine_county_id_fips"})
        .drop(columns=["intptlon10", "intptlat10"])
        .convert_dtypes(convert_floating=False)
    )
    frc = frc.merge(mine_locations, on="mine_county_id_fips", how="left")

    return frc


def haversine(lon1, lat1, lon2, lat2):
    """Calculate angular distance in radians between two points on a sphere."""
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    hav = (
        np.sin((lat2 - lat1) / 2.0) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2.0) ** 2
    )
    return 2 * np.arcsin(np.sqrt(hav))


def _cast_categorical(df: pd.DataFrame, max_cardinality=2**8) -> pd.DataFrame:
    categorical_cols = df.select_dtypes("object").nunique() < max_cardinality
    categorical_cols = list(categorical_cols[categorical_cols].index)
    df.loc[:, categorical_cols] = df.loc[:, categorical_cols].astype("category")
    return None


def _add_features(frc: pd.DataFrame) -> pd.DataFrame:
    out = (
        frc.pipe(add_fips_ids)
        .assign(
            # Remove 225 totally ridiculous outliers that skew the results
            fuel_cost_per_mmbtu_clipped=lambda x: x.fuel_cost_per_mmbtu.clip(
                lower=0, upper=1000
            ),
            # avoid confusion with fuel_cost_per_mmbtu_clipped, will drop original
            fuel_cost_per_mmbtu_raw=lambda x: x.fuel_cost_per_mmbtu,
            # Numerical representation of elapsed time
            elapsed_days=lambda x: (x.report_date - x.report_date.min()).dt.days,
            contract_expiration_date=lambda x: pd.to_datetime(
                x.contract_expiration_date
            ),
            # Time until current contract expires
            remaining_contract_days=lambda x: (
                x.contract_expiration_date - x.report_date
            ).dt.days,
            # Categorical months, to capture cyclical seasonal variability
            report_month=lambda x: x.report_date.dt.month,
            # Larger geographic area more likely to have lots of records
            census_region=lambda x: x.state.map(STATE_TO_CENSUS_REGION),
            # Need the total MMBTU for weighting the importance of the record
            # May also be predictive -- small deliveries seem more likely to be expensive
            fuel_received_mmbtu=lambda x: x.fuel_received_units * x.fuel_mmbtu_per_unit,
            mine_plant_same_state=lambda x: (x.state == x.mine_state).fillna(False),
            mine_plant_same_county=lambda x: (
                x.county_id_fips == x.mine_county_id_fips
            ).fillna(False),
            mine_distance_km=lambda x: haversine(
                x.longitude, x.latitude, x.mine_longitude, x.mine_latitude
            ),
        )
        .drop(
            columns="fuel_cost_per_mmbtu"
        )  # avoid confusion with _clipped. Still available as _raw
        .convert_dtypes(convert_floating=False, convert_integer=False)
        .astype(
            {
                "mine_id_msha": float,
                "plant_id_eia": int,
            }
        )
    )

    # sklearn doesn't like pd.StringDType
    str_cols = list(out.select_dtypes("string").columns)
    out.loc[:, str_cols] = out.loc[:, str_cols].astype("object").replace(pd.NA, np.nan)
    _cast_categorical(out)
    return out


class FRCImputer:
    """A class to impute EIA fuel prices with a Gradient Boosting Regressor."""

    def __init__(
        self,
        features: list[str] | None = None,
        target_col: str = "fuel_cost_per_mmbtu_clipped",
        weight_col: str = "fuel_received_mmbtu",
        regressor_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the imputer with good-enough hyperparameters."""
        if regressor_kwargs is None:
            self._regressor_kwargs = dict(
                loss="absolute_error",
                random_state=42,
                # hyperparams
                max_iter=1000,
                learning_rate=0.1,
                max_depth=7,
                max_leaf_nodes=2**7,
                min_samples_leaf=25,
            )
        else:
            self._regressor_kwargs = regressor_kwargs
        if features is None:
            self._features = ["asdf"]
        else:
            self._features = features
        self.estimator = None
        self._target_col = target_col
        self._weight_col = weight_col
        self._to_impute = None
        return

    def _make_pipeline(self):
        ord_enc = make_column_transformer(
            (
                OrdinalEncoder(
                    handle_unknown="use_encoded_value", unknown_value=np.nan
                ),
                make_column_selector(dtype_include=["category", "string"]),
            ),
            remainder="passthrough",
        )
        self.estimator = Pipeline(
            [
                ("ord_enc", ord_enc),
                ("hist_gbr", HistGradientBoostingRegressor(**self._regressor_kwargs)),
            ]
        )
        return

    def _train_model(self, frc: pd.DataFrame) -> None:
        self._to_impute = frc.loc[:, self._target_col].isna()
        X = frc.loc[~self._to_impute, self._features]  # noqa: N806
        y = frc.loc[~self._to_impute, self._target_col]
        sample_weight = frc.loc[~self._to_impute, self._weight_col]

        if "categorical_features" not in self._regressor_kwargs.keys():
            self._regressor_kwargs["categorical_features"] = X.columns.get_indexer(
                X.select_dtypes("category").columns
            )
        if self.estimator is None:
            self._make_pipeline()

        self.estimator.fit(
            X=X,
            y=y,
            hist_gbr__sample_weight=sample_weight,
        )

    def impute(self, frc: pd.DataFrame) -> pd.DataFrame:
        """Train the model on input data and return predictions."""
        self._train_model(frc)
        prediction = pd.Series(
            self.estimator.predict(frc.loc[:, self._features]), index=frc.index
        )
        out = pd.DataFrame(
            {
                self._target_col
                + "_filled": frc.loc[:, self._target_col].fillna(prediction),
                self._target_col + "_is_imputed": self._to_impute,
                self._target_col + "_imputed_values": prediction,
            },
            index=frc.index,
        )
        return out


def add_error_cols(df: pd.DataFrame, *, target_col: str, prediction_col: str) -> None:
    """Add absolute and relative error metrics to the dataframe for evaluation."""
    df["error"] = df.loc[:, prediction_col] - df.loc[:, target_col]
    df["rel_error"] = df.loc[:, "error"] / df.loc[:, target_col]
    return
