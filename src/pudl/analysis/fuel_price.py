"""Methods for estimating redacted EIA-923 fuel price information."""
import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.compose import make_column_selector, make_column_transformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
import sqlalchemy as sa

from pudl.metadata.enums import STATE_TO_CENSUS_REGION
from pudl.helpers import date_merge, add_fips_ids

logger = logging.getLogger(__name__)


def load_pudl_features(engine: sa.engine.Engine) -> pd.DataFrame:
    monthly_query = """
    -- SQLite
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

        mine.mine_name,
        mine.mine_type_code,
        mine.county_id_fips as mine_county_id_fips,
        mine.state as mine_state,

        sources.fuel_group_eiaepm,

        entity.ferc_cogen_status,
        entity.ferc_exempt_wholesale_generator,
        entity.ferc_small_power_producer,
        entity.iso_rto_code,
        entity.latitude,
        entity.longitude,
        entity.state,
        entity.county,
        entity.sector_name_eia
    FROM fuel_receipts_costs_eia923 as frc
    LEFT JOIN coalmine_eia923 as mine
        USING (mine_id_pudl)
    LEFT JOIN energy_sources_eia as sources
        on sources.code = frc.energy_source_code
    LEFT JOIN plants_entity_eia as entity
        USING (plant_id_eia)
    ;
    """
    frc = pd.read_sql(monthly_query, engine)
    annual_query = """
    SELECT
        plant_id_eia,
        report_date,
        natural_gas_pipeline_name_1,
        natural_gas_pipeline_name_2,
        natural_gas_pipeline_name_3,
        regulatory_status_code,
        water_source
    FROM plants_eia860
    ;        
    """
    plant_info = pd.read_sql(annual_query, engine)
    frc = date_merge(left=frc, right=plant_info, on=["plant_id_eia"], how="left")
    return frc


def _cast_categorical(df: pd.DataFrame, max_cardinality=2**8) -> pd.DataFrame:
    categorical_cols = df.select_dtypes("object").nunique() < 2**8
    categorical_cols = list(categorical_cols[categorical_cols].index)
    df.loc[:, categorical_cols] = df.loc[:, categorical_cols].astype("category")
    return None


def _add_features(frc: pd.DataFrame) -> pd.DataFrame:
    out = (
        frc.pipe(add_fips_ids)
        .assign(
            # Remove 225 totally ridiculous outliers that skew the results
            fuel_cost_per_mmbtu_clipped=lambda x: x.fuel_cost_per_mmbtu.clip(lower=0, upper=1000),
            # avoid confusion with fuel_cost_per_mmbtu_clipped, will drop original
            fuel_cost_per_mmbtu_raw=lambda x: x.fuel_cost_per_mmbtu,
            # Numerical representation of elapsed time
            elapsed_days=lambda x: (x.report_date - x.report_date.min()).dt.days,
            contract_expiration_date=lambda x: pd.to_datetime(x.contract_expiration_date),
            # Time until current contract expires
            remaining_contract_days=lambda x: (x.contract_expiration_date - x.report_date).dt.days,
            # Categorical months, to capture cyclical seasonal variability
            report_month=lambda x: x.report_date.dt.month,
            # Larger geographic area more likely to have lots of records
            census_region=lambda x: x.state.map(STATE_TO_CENSUS_REGION),
            # Need the total MMBTU for weighting the importance of the record
            # May also be predictive -- small deliveries seem more likely to be expensive
            fuel_received_mmbtu=lambda x: x.fuel_received_units * x.fuel_mmbtu_per_unit,
            mine_plant_same_state=lambda x: (x.state == x.mine_state).fillna(False),
            mine_plant_same_county=lambda x: (x.county_id_fips == x.mine_county_id_fips).fillna(False),
        )
        .drop(columns="fuel_cost_per_mmbtu")  # avoid confusion with _clipped. Still available as _raw
        .astype(
            {
                "plant_id_eia": int,
            }
        )
    )
    # sklearn doesn't like pd.StringDType
    str_cols = list(out.select_dtypes("string").columns)
    out.loc[:, str_cols] = out.loc[:, str_cols].astype("object").replace(pd.NA, np.nan)
    _cast_categorical(out)
    return out


class FRCImputer(object):
    def __init__(
        self,
        features: Optional[List[str]] = None,
        target_col: str = "fuel_cost_per_mmbtu_clipped",
        weight_col: str = "fuel_received_mmbtu",
        regressor_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
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
                OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=np.nan),
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
        X = frc.loc[~self._to_impute, self._features]
        y = frc.loc[~self._to_impute, self._target_col]
        sample_weight = frc.loc[~self._to_impute, self._weight_col]

        if "categorical_features" not in self._regressor_kwargs.keys():
            self._regressor_kwargs["categorical_features"] = X.columns.get_indexer(X.select_dtypes("category").columns)
        if self.estimator is None:
            self._make_pipeline()

        self.estimator.fit(
            X=X,
            y=y,
            hist_gbr__sample_weight=sample_weight,
        )

    def impute(self, frc: pd.DataFrame) -> pd.DataFrame:
        self._train_model(frc)
        prediction = pd.Series(self.estimator.predict(frc.loc[:, self._features]), index=frc.index)
        out = pd.DataFrame(
            {
                self._target_col + "_filled": frc.loc[:, self._target_col].fillna(prediction),
                self._target_col + "_is_imputed": self._to_impute,
                self._target_col + "_imputed_values": prediction,
            },
            index=frc.index,
        )
        return out


def add_error_cols(df: pd.DataFrame, *, target_col: str, prediction_col: str) -> None:
    df["error"] = df.loc[:, prediction_col] - df.loc[:, target_col]
    df["rel_error"] = df.loc[:, "error"] / df.loc[:, target_col]
    return
