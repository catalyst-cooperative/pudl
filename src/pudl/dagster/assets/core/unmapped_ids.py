"""Dagster assets identifying FERC1/EIA plant & utility IDs missing manual mapping.

FERC Form 1 and EIA plants and utilities are linked together using a manually curated
ID mapping spreadsheet (see :mod:`pudl.glue.ferc1_eia`). As new years of data are
added, new plants and utilities show up that haven't been manually mapped yet. The
assets defined here identify those unmapped IDs so they can be reviewed and added to
the mapping spreadsheet -- see :doc:`/dev/pudl_id_mapping` for the overall process.

We expect these assets to always be empty in a healthy ETL run; the asset checks
registered against them in :mod:`pudl.dagster.asset_checks` fail the run if any of
them contain rows. Each is also written out as a CSV under ``$PUDL_OUTPUT`` so the
unmapped IDs are easy to find and use when updating the mapping spreadsheet.
"""

import dagster as dg
import pandas as pd

import pudl.glue.ferc1_eia as glue_ferc1_eia
import pudl.logging_helpers
from pudl.extract.ferc1 import FERC1_DBF_SQLITE_ASSET_KEY, FERC1_XBRL_SQLITE_ASSET_KEY
from pudl.transform.ferc1 import ferc1_transform_asset_factory

logger = pudl.logging_helpers.get_logger(__name__)


def _lower_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Lower-case all string values so case differences don't break ID matching."""
    return df.map(lambda x: x.lower() if isinstance(x, str) else x)


def _write_unmapped_ids_csv(
    context: dg.AssetExecutionContext, name: str, df: pd.DataFrame
) -> None:
    """Write an unmapped-IDs dataframe to ``$PUDL_OUTPUT`` as a CSV for manual mapping."""
    df.to_csv(context.resources.pudl_paths.output_file(f"{name}.csv"))


#############################################
# Raw inputs, wired up as normal Dagster deps
#############################################


@dg.asset(
    deps=[FERC1_DBF_SQLITE_ASSET_KEY],
    required_resource_keys={"ferc1_dbf_sqlite_io_manager"},
)
def _util_ids_ferc1_raw_dbf(context) -> pd.DataFrame:
    """Utility IDs reported in the raw FERC1 DBF database, for manual ID mapping."""
    engine = context.resources.ferc1_dbf_sqlite_io_manager.engine
    return glue_ferc1_eia.get_util_ids_ferc1_raw_dbf(engine).pipe(_lower_strings)


@dg.asset(
    deps=[FERC1_XBRL_SQLITE_ASSET_KEY],
    required_resource_keys={"ferc1_xbrl_sqlite_io_manager"},
)
def _util_ids_ferc1_raw_xbrl(context) -> pd.DataFrame:
    """Utility IDs reported in the raw FERC1 XBRL database, for manual ID mapping."""
    engine = context.resources.ferc1_xbrl_sqlite_io_manager.engine
    return glue_ferc1_eia.get_util_ids_ferc1_raw_xbrl(engine).pipe(_lower_strings)


# Generic re-transforms of the FERC1 plant tables that skip the FK/validity checks the
# real core_ferc1__* transformers apply -- necessary because plants that haven't been
# manually assigned a plant_id_pudl yet would otherwise get dropped before we can list
# them for mapping. Named distinctly from the real core_ferc1__* assets they parallel
# so the two don't collide in the asset graph.
_generic_ferc1_plant_transform_assets = [
    ferc1_transform_asset_factory(
        table_name,
        glue_ferc1_eia.GenericPlantFerc1TableTransformer,  # type: ignore[bad-argument-type]
        io_manager_key=None,
        convert_dtypes=False,
        generic=True,
        name=f"_ferc1_generic__{table_name}",
    )
    for table_name in glue_ferc1_eia.GENERIC_FERC1_PLANT_TABLES
]


@dg.asset(
    ins={
        f"_ferc1_generic__{table_name}": dg.AssetIn(f"_ferc1_generic__{table_name}")
        for table_name in glue_ferc1_eia.GENERIC_FERC1_PLANT_TABLES
    }
)
def _plants_ferc1_raw(**generic_plant_tables: pd.DataFrame) -> pd.DataFrame:
    """All raw FERC1 plants across DBF & XBRL, compiled for manual ID mapping."""
    return glue_ferc1_eia.compile_plants_ferc1_raw(
        list(generic_plant_tables.values())
    ).pipe(_lower_strings)


# core_eia923 tables with a plant_id_eia field, discovered from Resource metadata at
# definition time (no data is read) so this dependency is explicit in the asset graph
# instead of read off disk at run time.
_eia923_plant_id_tables = glue_ferc1_eia.get_core_eia923_plant_id_tables()


@dg.asset(
    ins={table_name: dg.AssetIn(table_name) for table_name in _eia923_plant_id_tables}
)
def _eia923_plant_ids(**eia923_dfs: pd.DataFrame) -> set:
    """All ``plant_id_eia`` values that appear in the core EIA-923 tables."""
    return glue_ferc1_eia.get_core_eia923_plant_ids(eia923_dfs)


########################
# Unmapped plants/utils
########################


@dg.multi_asset(
    outs={
        "missing_plant_id_pudl_in_plants_ferc1": dg.AssetOut(),
        "missing_plants_in_plants_ferc1": dg.AssetOut(),
        "missing_plants_in_plants_eia": dg.AssetOut(),
    },
    required_resource_keys={"pudl_paths"},
)
def unmapped_plants(
    context: dg.AssetExecutionContext,
    core_pudl__entity_plants_pudl: pd.DataFrame,
    core_pudl__assn_ferc1_pudl_plants: pd.DataFrame,
    core_pudl__assn_eia_pudl_plants: pd.DataFrame,
    _plants_ferc1_raw: pd.DataFrame,
    out_eia__yearly_plants: pd.DataFrame,
    out_eia__yearly_generators: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Identify FERC1 & EIA plants that appear in the data but aren't PUDL-mapped."""
    entity_plants_pudl = _lower_strings(core_pudl__entity_plants_pudl)
    assn_ferc1_pudl_plants = _lower_strings(core_pudl__assn_ferc1_pudl_plants)
    assn_eia_pudl_plants = _lower_strings(core_pudl__assn_eia_pudl_plants)
    plants_eia_pudl_db = _lower_strings(out_eia__yearly_plants)
    plants_eia_labeled = _lower_strings(
        glue_ferc1_eia.label_plants_eia(
            out_eia__yearly_plants, out_eia__yearly_generators
        )
    )

    # Should only ever find rows here if a plant is in the mapping sheet without a
    # plant_id_pudl -- there's no other table to pull mapping-relevant columns from.
    missing_plant_id_pudl = glue_ferc1_eia.get_missing_ids(
        entity_plants_pudl, assn_ferc1_pudl_plants, ["plant_id_pudl"]
    )
    missing_plant_id_pudl_in_plants_ferc1 = pd.DataFrame(index=missing_plant_id_pudl)

    missing_plants_ferc1 = glue_ferc1_eia.get_missing_ids(
        assn_ferc1_pudl_plants,
        _plants_ferc1_raw,
        ["utility_id_ferc1", "plant_name_ferc1"],
    )
    missing_plants_in_plants_ferc1 = (
        glue_ferc1_eia.label_missing_ids_for_manual_mapping(
            missing_plants_ferc1, _plants_ferc1_raw
        )
    )

    missing_plants_eia = glue_ferc1_eia.get_missing_ids(
        assn_eia_pudl_plants, plants_eia_pudl_db, ["plant_id_eia"]
    )
    missing_plants_in_plants_eia = glue_ferc1_eia.label_missing_ids_for_manual_mapping(
        missing_plants_eia, plants_eia_labeled
    )

    outputs = {
        "missing_plant_id_pudl_in_plants_ferc1": missing_plant_id_pudl_in_plants_ferc1,
        "missing_plants_in_plants_ferc1": missing_plants_in_plants_ferc1,
        "missing_plants_in_plants_eia": missing_plants_in_plants_eia,
    }
    for name, df in outputs.items():
        _write_unmapped_ids_csv(context, name, df)
    return (
        missing_plant_id_pudl_in_plants_ferc1,
        missing_plants_in_plants_ferc1,
        missing_plants_in_plants_eia,
    )


@dg.multi_asset(
    outs={
        "missing_utility_id_pudl_in_utilities_ferc1": dg.AssetOut(),
        "missing_utility_id_ferc1_in_utilities_ferc1_dbf": dg.AssetOut(),
        "missing_utility_id_ferc1_in_utilities_ferc1_xbrl": dg.AssetOut(),
        "missing_utility_id_ferc1_in_plants_ferc1": dg.AssetOut(),
        "missing_utility_id_ferc1_xbrl_in_raw_xbrl": dg.AssetOut(),
        "missing_utility_id_ferc1_dbf_in_raw_dbf": dg.AssetOut(),
        "missing_utility_id_eia_in_utilities_eia": dg.AssetOut(),
    },
    required_resource_keys={"pudl_paths"},
)
def unmapped_utilities(
    context: dg.AssetExecutionContext,
    core_pudl__entity_utilities_pudl: pd.DataFrame,
    core_pudl__assn_ferc1_pudl_utilities: pd.DataFrame,
    core_pudl__assn_ferc1_dbf_pudl_utilities: pd.DataFrame,
    core_pudl__assn_ferc1_xbrl_pudl_utilities: pd.DataFrame,
    core_pudl__assn_ferc1_pudl_plants: pd.DataFrame,
    core_pudl__assn_eia_pudl_utilities: pd.DataFrame,
    _util_ids_ferc1_raw_dbf: pd.DataFrame,
    _util_ids_ferc1_raw_xbrl: pd.DataFrame,
    out_eia__yearly_utilities: pd.DataFrame,
    out_eia__yearly_generators: pd.DataFrame,
    _eia923_plant_ids: set,
    core_eia860__scd_generators: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Identify FERC1 & EIA utilities that appear in the data but aren't PUDL-mapped."""
    entity_utilities_pudl = _lower_strings(core_pudl__entity_utilities_pudl)
    assn_ferc1_pudl_utilities = _lower_strings(core_pudl__assn_ferc1_pudl_utilities)
    assn_ferc1_dbf_pudl_utilities = _lower_strings(
        core_pudl__assn_ferc1_dbf_pudl_utilities
    )
    assn_ferc1_xbrl_pudl_utilities = _lower_strings(
        core_pudl__assn_ferc1_xbrl_pudl_utilities
    )
    assn_ferc1_pudl_plants = _lower_strings(core_pudl__assn_ferc1_pudl_plants)
    assn_eia_pudl_utilities = _lower_strings(core_pudl__assn_eia_pudl_utilities)

    utilities_ferc1_dbf_labeled = glue_ferc1_eia.label_utilities_ferc1_dbf(
        assn_ferc1_dbf_pudl_utilities, _util_ids_ferc1_raw_dbf
    )
    utilities_ferc1_xbrl_labeled = glue_ferc1_eia.label_utilities_ferc1_xbrl(
        assn_ferc1_xbrl_pudl_utilities, _util_ids_ferc1_raw_xbrl
    )

    missing_utility_id_pudl = glue_ferc1_eia.get_missing_ids(
        entity_utilities_pudl, assn_ferc1_pudl_utilities, ["utility_id_pudl"]
    )
    missing_utility_id_pudl_in_utilities_ferc1 = (
        glue_ferc1_eia.label_missing_ids_for_manual_mapping(
            missing_utility_id_pudl, assn_ferc1_pudl_utilities
        )
    )

    missing_utility_id_ferc1_dbf = glue_ferc1_eia.get_missing_ids(
        assn_ferc1_pudl_utilities, assn_ferc1_dbf_pudl_utilities, ["utility_id_ferc1"]
    )
    missing_utility_id_ferc1_in_utilities_ferc1_dbf = (
        glue_ferc1_eia.label_missing_ids_for_manual_mapping(
            missing_utility_id_ferc1_dbf, utilities_ferc1_dbf_labeled
        )
    )

    missing_utility_id_ferc1_xbrl = glue_ferc1_eia.get_missing_ids(
        assn_ferc1_pudl_utilities, assn_ferc1_xbrl_pudl_utilities, ["utility_id_ferc1"]
    )
    missing_utility_id_ferc1_in_utilities_ferc1_xbrl = (
        glue_ferc1_eia.label_missing_ids_for_manual_mapping(
            missing_utility_id_ferc1_xbrl, utilities_ferc1_xbrl_labeled
        )
    )

    missing_utility_id_ferc1_plants = glue_ferc1_eia.get_missing_ids(
        assn_ferc1_pudl_utilities, assn_ferc1_pudl_plants, ["utility_id_ferc1"]
    )
    missing_utility_id_ferc1_in_plants_ferc1 = (
        glue_ferc1_eia.label_missing_ids_for_manual_mapping(
            missing_utility_id_ferc1_plants, assn_ferc1_pudl_plants
        )
    )

    missing_utility_id_ferc1_xbrl_raw = glue_ferc1_eia.get_missing_ids(
        assn_ferc1_xbrl_pudl_utilities,
        _util_ids_ferc1_raw_xbrl,
        ["utility_id_ferc1_xbrl"],
    )
    missing_utility_id_ferc1_xbrl_in_raw_xbrl = (
        glue_ferc1_eia.label_missing_ids_for_manual_mapping(
            missing_utility_id_ferc1_xbrl_raw, _util_ids_ferc1_raw_xbrl
        )
    )

    missing_utility_id_ferc1_dbf_raw = glue_ferc1_eia.get_missing_ids(
        assn_ferc1_dbf_pudl_utilities,
        _util_ids_ferc1_raw_dbf,
        ["utility_id_ferc1_dbf"],
    )
    missing_utility_id_ferc1_dbf_in_raw_dbf = (
        glue_ferc1_eia.label_missing_ids_for_manual_mapping(
            missing_utility_id_ferc1_dbf_raw, _util_ids_ferc1_raw_dbf
        )
    )

    util_recent_cap = glue_ferc1_eia.get_utility_most_recent_capacity(
        core_eia860__scd_generators
    )
    missing_utility_id_eia_in_utilities_eia = glue_ferc1_eia.get_util_ids_eia_unmapped(
        out_eia__yearly_utilities=out_eia__yearly_utilities,
        out_eia__yearly_generators=out_eia__yearly_generators,
        utilities_eia_mapped=assn_eia_pudl_utilities,
        eia923_plant_ids=_eia923_plant_ids,
        util_recent_cap=util_recent_cap,
    )

    outputs = {
        "missing_utility_id_pudl_in_utilities_ferc1": missing_utility_id_pudl_in_utilities_ferc1,
        "missing_utility_id_ferc1_in_utilities_ferc1_dbf": missing_utility_id_ferc1_in_utilities_ferc1_dbf,
        "missing_utility_id_ferc1_in_utilities_ferc1_xbrl": missing_utility_id_ferc1_in_utilities_ferc1_xbrl,
        "missing_utility_id_ferc1_in_plants_ferc1": missing_utility_id_ferc1_in_plants_ferc1,
        "missing_utility_id_ferc1_xbrl_in_raw_xbrl": missing_utility_id_ferc1_xbrl_in_raw_xbrl,
        "missing_utility_id_ferc1_dbf_in_raw_dbf": missing_utility_id_ferc1_dbf_in_raw_dbf,
        "missing_utility_id_eia_in_utilities_eia": missing_utility_id_eia_in_utilities_eia,
    }
    for name, df in outputs.items():
        _write_unmapped_ids_csv(context, name, df)
    return (
        missing_utility_id_pudl_in_utilities_ferc1,
        missing_utility_id_ferc1_in_utilities_ferc1_dbf,
        missing_utility_id_ferc1_in_utilities_ferc1_xbrl,
        missing_utility_id_ferc1_in_plants_ferc1,
        missing_utility_id_ferc1_xbrl_in_raw_xbrl,
        missing_utility_id_ferc1_dbf_in_raw_dbf,
        missing_utility_id_eia_in_utilities_eia,
    )


UNMAPPED_ID_ASSET_NAMES: tuple[str, ...] = (
    "missing_plant_id_pudl_in_plants_ferc1",
    "missing_plants_in_plants_ferc1",
    "missing_plants_in_plants_eia",
    "missing_utility_id_pudl_in_utilities_ferc1",
    "missing_utility_id_ferc1_in_utilities_ferc1_dbf",
    "missing_utility_id_ferc1_in_utilities_ferc1_xbrl",
    "missing_utility_id_ferc1_in_plants_ferc1",
    "missing_utility_id_ferc1_xbrl_in_raw_xbrl",
    "missing_utility_id_ferc1_dbf_in_raw_dbf",
    "missing_utility_id_eia_in_utilities_eia",
)
"""Names of every unmapped-ID output asset, for use in :mod:`pudl.dagster.asset_checks`."""
