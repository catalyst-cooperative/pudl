"""Identify (plant_id_eia, report_date) pairs that only appear in
_core_eia860__emissions_control_equipment and nowhere else among the harvestable
assets, using the materialized DataFrames stored by Dagster's FilesystemIOManager.

Before PR #5048, _core_eia860__emissions_control_equipment returned a DataFrame
with `report_year` (integer) but no `report_date`, so the plant harvester silently
skipped it. PR #5048 added .pipe(pudl.helpers.convert_to_date) at the top of the
transform, creating `report_date`, and the table is now included in the harvest.

The new rows in core_eia860__scd_plants are (plant_id_eia, report_date) pairs that
appear EXCLUSIVELY in _core_eia860__emissions_control_equipment and not in any other
harvestable table — those are plant-years that no other EIA table reported.

This script loads the intermediate assets from Dagster's FilesystemIOManager
(pickle files in $DAGSTER_HOME/storage/) to do the key-level comparison.
"""

import os
import pickle
from pathlib import Path

import pandas as pd

DAGSTER_HOME = Path(os.environ["DAGSTER_HOME"])
STORAGE = DAGSTER_HOME / "storage"

# The full set of harvestable_assets as defined in transform/eia.py
HARVESTABLE_ASSETS = [
    "_core_eia860__boiler_cooling",
    "_core_eia860__boiler_emissions_control_equipment_assn",
    "_core_eia923__boiler_fuel",
    "_core_eia860__boiler_generator_assn",
    "_core_eia860__boiler_stack_flue",
    "_core_eia860__boilers",
    "_core_eia923__coalmine",
    "_core_eia860__emissions_control_equipment",
    "_core_eia923__energy_storage",
    "_core_eia923__fuel_receipts_costs",
    "_core_eia923__generation",
    "_core_eia923__generation_fuel",
    "_core_eia923__generation_fuel_nuclear",
    "_core_eia860__generators",
    "_core_eia860__generators_energy_storage",
    "_core_eia860__generators_wind",
    "_core_eia860__generators_solar",
    "_core_eia860__generators_multifuel",
    "_core_eia860__ownership",
    "_core_eia860__plants",
    "_core_eia860__utilities",
]


def load_asset(name: str) -> pd.DataFrame | None:
    """Load a materialized Dagster asset from FilesystemIOManager pickle storage."""
    path = STORAGE / name
    if not path.exists():
        return None
    with Path(path).open("rb") as f:
        return pickle.load(f)


def asset_pairs(df: pd.DataFrame) -> set:
    """Extract unique (plant_id_eia, report_date) pairs from a DataFrame."""
    if "plant_id_eia" not in df.columns or "report_date" not in df.columns:
        return set()
    df = df[["plant_id_eia", "report_date"]].dropna()
    df["report_date"] = pd.to_datetime(df["report_date"]).dt.normalize()
    return set(zip(df["plant_id_eia"], df["report_date"]))


emce_keys: set = set()
other_keys: set = set()

for asset_name in HARVESTABLE_ASSETS:
    df = load_asset(asset_name)
    if df is None:
        print(f"  MISSING: {asset_name}")
        continue
    pairs = asset_pairs(df)
    has_date = "report_date" in df.columns
    has_year = "report_year" in df.columns
    date_info = f"report_date={'yes' if has_date else 'no'}, report_year={'yes' if has_year else 'no'}"
    print(f"  {asset_name}: {len(pairs)} unique pairs  ({date_info})")
    if asset_name == "_core_eia860__emissions_control_equipment":
        emce_keys = pairs
    else:
        other_keys |= pairs

emce_only_keys = emce_keys - other_keys
print(
    f"\n(plant_id_eia, report_date) pairs found ONLY in "
    f"_core_eia860__emissions_control_equipment: {len(emce_only_keys)}"
)
for plant_id, date in sorted(emce_only_keys):
    print(f"  plant_id_eia={plant_id}  report_date={date.date()}")

# Cross-check against current scd_plants (from parquet)
PARQUET = Path(os.environ["PUDL_OUTPUT"]) / "parquet"
scd_plants = pd.read_parquet(
    PARQUET / "core_eia860__scd_plants.parquet",
    columns=["plant_id_eia", "report_date"],
)
scd_plants["report_date"] = pd.to_datetime(scd_plants["report_date"]).dt.normalize()
scd_keys = set(zip(scd_plants["plant_id_eia"], scd_plants["report_date"]))
print(f"\ncore_eia860__scd_plants total rows: {len(scd_plants)}")
confirmed_in_scd = emce_only_keys & scd_keys
print(
    f"Of the EMCE-only pairs, confirmed present in core_eia860__scd_plants: "
    f"{len(confirmed_in_scd)}"
)
