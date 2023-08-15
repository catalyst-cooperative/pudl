"""Extract FERC Form 1 data from SQLite DBs derived from original DBF or XBRL files.

The FERC Form 1 data is available in two primary formats, spanning different years. The
early digital data (1994-2020) was distributed using annual Visual FoxPro databases.
Starting in 2021, the agency moved to using XBRL (a dialect of XML) published via an
RSS feed one filing at a time. First we convert both of those difficult to use original
formats into relational databases (currently stored in SQLite). We use those
databases as the starting point for our extensive cleaning and reorganization of a small
portion of the available tables into a well normalized database that covers all the
years of available data. The complete input databases are published separately to
provide users access to all of the original tables, since we've only been able to
clean up a small subset of them.

The conversion from both DBF and XBRL to SQLite is coordinated by the
:mod:`pudl.convert.ferc_to_sqlite` script. The code for the XBRL to SQLite conversion
is used across all the modern FERC forms, and is contained in a standalone package:

https://github.com/catalyst-cooperative/ferc-xbrl-extractor

The code for converting the older FERC 1 DBF files into an SQLite DB is contained in
this module.

One challenge with both of these data sources is that each year of data is treated as a
standalone resource by FERC. The databases are not explicitly linked together across
years. Over time the structure of the Visual FoxPro DB has changed as new tables and
fields have been added. In order to be able to use the data to do analyses across many
years, we need to bring all of it into a unified structure. These structural changes
have only ever been additive -- more recent versions of the DBF databases contain all
the tables and fields that existed in earlier versions.

PUDL uses the most recently released year of DBF data (2020) as a template for the
database schema, since it is capable of containing all the= fields and tables found in
the other years.  The structure of the database is also informed by other documentation
we have been able to compile over the years from the FERC website and other sources.
Copies of these resoruces are included in the :doc:`FERC Form 1 data source
documentation </data_sources/ferc1>`

Using this inferred structure PUDL creates an SQLite database mirroring the FERC
database using :mod:`sqlalchemy`. Then we use a python package called `dbfread
<https://dbfread.readthedocs.io/en/latest/>`__ to extract the data from the DBF tables,
and insert it virtually unchanged into the SQLite database.

Note that many quantities in the Visual FoxPro databases are tied not just to a
particular table and column, but to a row number within an individual filing, and
those row numbers have changed slowly over the years for some tables as rows have been
added or removed from the form. The ``f1_row_lit_tbl`` table contains a record of these
changes, and can be used to align reported quantities across time.

The one significant change we make to the raw input data is to ensure that there's a
master table of the all the respondent IDs and respondent names. All the other tables
refer to this table. Unlike the other tables the ``f1_respondent_id`` table has no
``report_year`` and so it represents a merge of all the years of data. In the event that
the name associated with a given respondent ID has changed over time, we retain the most
recently reported name.

Note that there are a small number of respondent IDs that **do not** appear in any year
of the ``f1_respondent_id`` table, but that **do** appear in the data tables. We add
these observed but not directly reported IDs to the ``f1_respondent_id`` table and have
done our best to identify what utility they correspond to based on the assets associated
with those respondent IDs.

This SQLite compilation of the original FERC Form 1 databases accommodates all
116 tables from all the published years of DBF data (1994-2020) and takes up about 1GB
of space on disk. You can interact with the most recent development version of this
database online at:

https://data.catalyst.coop/ferc1
"""
import json
from itertools import chain
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import sqlalchemy as sa
from dagster import (
    AssetKey,
    Field,
    SourceAsset,
    asset,
    build_init_resource_context,
    build_input_context,
)

import pudl
from pudl.extract.dbf import (
    FercDbfExtractor,
    PartitionedDataFrame,
    add_key_constraints,
    deduplicate_by_year,
)
from pudl.helpers import EnvVar
from pudl.io_managers import (
    FercDBFSQLiteIOManager,
    FercXBRLSQLiteIOManager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
)
from pudl.settings import DatasetsSettings, FercToSqliteSettings, GenericDatasetSettings

logger = pudl.logging_helpers.get_logger(__name__)

TABLE_NAME_MAP_FERC1: dict[str, dict[str, str]] = {
    "fuel_ferc1": {
        "dbf": "f1_fuel",
        "xbrl": "steam_electric_generating_plant_statistics_large_plants_fuel_statistics_402",
    },
    "plants_steam_ferc1": {
        "dbf": "f1_steam",
        "xbrl": "steam_electric_generating_plant_statistics_large_plants_402",
    },
    "plants_small_ferc1": {
        "dbf": "f1_gnrt_plant",
        "xbrl": "generating_plant_statistics_410",
    },
    "plants_hydro_ferc1": {
        "dbf": "f1_hydro",
        "xbrl": "hydroelectric_generating_plant_statistics_large_plants_406",
    },
    "plants_pumped_storage_ferc1": {
        "dbf": "f1_pumped_storage",
        "xbrl": "pumped_storage_generating_plant_statistics_large_plants_408",
    },
    "plant_in_service_ferc1": {
        "dbf": "f1_plant_in_srvce",
        "xbrl": "electric_plant_in_service_204",
    },
    "purchased_power_ferc1": {
        "dbf": "f1_purchased_pwr",
        "xbrl": "purchased_power_326",
    },
    "electric_energy_sources_ferc1": {
        "dbf": "f1_elctrc_erg_acct",
        "xbrl": "electric_energy_account_401a",
    },
    "electric_energy_dispositions_ferc1": {
        "dbf": "f1_elctrc_erg_acct",
        "xbrl": "electric_energy_account_401a",
    },
    "utility_plant_summary_ferc1": {
        "dbf": "f1_utltyplnt_smmry",
        "xbrl": "summary_of_utility_plant_and_accumulated_provisions_for_depreciation_amortization_and_depletion_200",
    },
    "transmission_statistics_ferc1": {
        "dbf": "f1_xmssn_line",
        "xbrl": "transmission_line_statistics_422",
    },
    "electric_operating_expenses_ferc1": {
        "dbf": "f1_elc_op_mnt_expn",
        "xbrl": "electric_operations_and_maintenance_expenses_320",
    },
    "balance_sheet_liabilities_ferc1": {
        "dbf": "f1_bal_sheet_cr",
        "xbrl": "comparative_balance_sheet_liabilities_and_other_credits_110",
    },
    "balance_sheet_assets_ferc1": {
        "dbf": "f1_comp_balance_db",
        "xbrl": "comparative_balance_sheet_assets_and_other_debits_110",
    },
    "income_statement_ferc1": {
        "dbf": ["f1_income_stmnt", "f1_incm_stmnt_2"],
        "xbrl": "statement_of_income_114",
    },
    "retained_earnings_ferc1": {
        "dbf": "f1_retained_erng",
        "xbrl": "retained_earnings_118",
    },
    "retained_earnings_appropriations_ferc1": {
        "dbf": "f1_retained_erng",
        "xbrl": "retained_earnings_appropriations_118",
    },
    "depreciation_amortization_summary_ferc1": {
        "dbf": "f1_dacs_epda",
        "xbrl": "summary_of_depreciation_and_amortization_charges_section_a_336",
    },
    "electric_plant_depreciation_changes_ferc1": {
        "dbf": "f1_accumdepr_prvsn",
        "xbrl": "accumulated_provision_for_depreciation_of_electric_utility_plant_changes_section_a_219",
    },
    "electric_plant_depreciation_functional_ferc1": {
        "dbf": "f1_accumdepr_prvsn",
        "xbrl": "accumulated_provision_for_depreciation_of_electric_utility_plant_functional_classification_section_b_219",
    },
    "electric_operating_revenues_ferc1": {
        "dbf": "f1_elctrc_oper_rev",
        "xbrl": "electric_operating_revenues_300",
    },
    "cash_flow_ferc1": {
        "dbf": "f1_cash_flow",
        "xbrl": "statement_of_cash_flows_120",
    },
    "electricity_sales_by_rate_schedule_ferc1": {
        "dbf": "f1_sales_by_sched",
        "xbrl": [
            "sales_of_electricity_by_rate_schedules_account_440_residential_304",
            "sales_of_electricity_by_rate_schedules_account_442_commercial_304",
            "sales_of_electricity_by_rate_schedules_account_442_industrial_304",
            "sales_of_electricity_by_rate_schedules_account_444_public_street_and_highway_lighting_304",
            "sales_of_electricity_by_rate_schedules_account_445_other_sales_to_public_authorities_304",
            "sales_of_electricity_by_rate_schedules_account_446_sales_to_railroads_and_railways_304",
            "sales_of_electricity_by_rate_schedules_account_448_interdepartmental_sales_304",
            "sales_of_electricity_by_rate_schedules_account_4491_provision_for_rate_refunds_304",
            "sales_of_electricity_by_rate_schedules_account_totals_304",
        ],
    },
    "other_regulatory_liabilities_ferc1": {
        "dbf": "f1_othr_reg_liab",
        "xbrl": "other_regulatory_liabilities_account_254_278",
    },
}
"""A mapping of PUDL DB table names to their XBRL and DBF source table names."""


class Ferc1DbfExtractor(FercDbfExtractor):
    """Wrapper for running the foxpro to sqlite conversion of FERC1 dataset."""

    DATASET = "ferc1"
    DATABASE_NAME = "ferc1.sqlite"

    def get_settings(
        self, global_settings: FercToSqliteSettings
    ) -> GenericDatasetSettings:
        """Returns settings for FERC Form 1 DBF dataset."""
        return global_settings.ferc1_dbf_to_sqlite_settings

    def finalize_schema(self, meta: sa.MetaData) -> sa.MetaData:
        """Modifies schema before it's written to sqlite database.

        This marks f1_responent_id.respondent_id as a primary key and adds foreign key
        constraints on all tables with respondent_id column.
        """
        return add_key_constraints(
            meta, pk_table="f1_respondent_id", column="respondent_id"
        )

    def postprocess(self):
        """Applies final transformations on the data in sqlite database.

        This identifies respondents that are referenced by other tables but that are not
        in the f1_respondent_id tables. These missing respondents are inserted back into
        f1_respondent_id table.
        """
        self.add_missing_respondents()

    PUDL_RIDS: dict[int, str] = {
        514: "AEP Texas",
        519: "Upper Michigan Energy Resources Company",
        522: "Luning Energy Holdings LLC, Invenergy Investments",
        529: "Tri-State Generation and Transmission Association",
        531: "Basin Electric Power Cooperative",
    }
    """Missing FERC 1 Respondent IDs for which we have identified the respondent."""

    def add_missing_respondents(self):
        """Add missing respondents to f1_respondent_id table.

        Some respondent_ids are referenced by other tables, but are not listed in the
        f1_respondent_id table. This function finds all of these missing respondents and
        backfills them into the f1_respondent_id table.
        """
        # add the missing respondents into the respondent_id table.
        reported_ids = set(
            pd.read_sql_table(
                "f1_respondent_id",
                self.sqlite_engine,
            ).respondent_id.unique()
        )
        missing_ids = self.get_observed_respondents().difference(reported_ids)

        # Construct minimal records of the observed unknown respondents. Some of these
        # are identified in the PUDL_RIDS map, others are still unknown.
        records = []
        for rid in missing_ids:
            entry = {"respondent_id": rid}
            known_name = self.PUDL_RIDS.get(rid, None)
            if known_name:
                entry["respondent_name"] = f"{known_name} (PUDL determined)"
            else:
                entry["respondent_name"] = f"Missing Respondent {rid}"
            records.append(entry)

        # Write missing respondents back into SQLite.
        with self.sqlite_engine.begin() as conn:
            conn.execute(
                self.sqlite_meta.tables["f1_respondent_id"].insert().values(records)
            )

    def get_observed_respondents(self) -> set[int]:
        """Compile the set of all observed respondent IDs found in the FERC 1 database.

        A significant number of FERC 1 respondent IDs appear in the data tables, but not
        in the f1_respondent_id table. In order to construct a self-consistent database
        with we need to find all of those missing respondent IDs and inject them into
        the table when we clone the database.

        Returns:
            Every respondent ID reported in any of the FERC 1 DB tables.
        """
        observed = set()
        for table in self.sqlite_meta.tables.values():
            if "respondent_id" in table.columns:
                observed = observed.union(
                    set(
                        pd.read_sql_table(
                            table.name, self.sqlite_engine, columns=["respondent_id"]
                        ).respondent_id
                    )
                )
        return observed

    def aggregate_table_frames(
        self, table_name: str, dfs: list[PartitionedDataFrame]
    ) -> pd.DataFrame | None:
        """Deduplicates records in f1_respondent_id table."""
        if table_name == "f1_respondent_id":
            return deduplicate_by_year(dfs, "respondent_id")
        else:
            return super().aggregate_table_frames(table_name, dfs)


###########################################################################
# Functions for extracting ferc1 tables from SQLite to PUDL
###########################################################################


# DAGSTER ASSETS
def create_raw_ferc1_assets() -> list[SourceAsset]:
    """Create SourceAssets for raw ferc1 tables.

    SourceAssets allow you to access assets that are generated elsewhere.
    In our case, the xbrl and dbf database are created in a separate dagster Definition.

    Returns:
        A list of ferc1 SourceAssets.
    """
    # Deduplicate the table names because f1_elctrc_erg_acct feeds into multiple pudl tables.
    dbfs = (v["dbf"] for v in TABLE_NAME_MAP_FERC1.values())
    flattened_dbfs = chain.from_iterable(
        x if isinstance(x, list) else [x] for x in dbfs
    )
    dbf_table_names = tuple(set(flattened_dbfs))
    raw_ferc1_dbf_assets = [
        SourceAsset(
            key=AssetKey(f"raw_ferc1_dbf__{table_name}"),
            io_manager_key="ferc1_dbf_sqlite_io_manager",
        )
        for table_name in dbf_table_names
    ]

    # Create assets for the duration and instant tables
    xbrls = (v["xbrl"] for v in TABLE_NAME_MAP_FERC1.values())
    flattened_xbrls = chain.from_iterable(
        x if isinstance(x, list) else [x] for x in xbrls
    )
    xbrls_with_periods = chain.from_iterable(
        (f"{tn}_instant", f"{tn}_duration") for tn in flattened_xbrls
    )
    xbrl_table_names = tuple(set(xbrls_with_periods))
    raw_ferc1_xbrl_assets = [
        SourceAsset(
            key=AssetKey(f"raw_ferc1_xbrl__{table_name}"),
            io_manager_key="ferc1_xbrl_sqlite_io_manager",
        )
        for table_name in xbrl_table_names
    ]
    return raw_ferc1_dbf_assets + raw_ferc1_xbrl_assets


raw_ferc1_assets = create_raw_ferc1_assets()

# TODO (bendnorman): The metadata asset could be improved.
# Select the subset of metadata entries that pudl is actually processing.
# Could also create an IO manager that pulls from the metadata based on the
# asset name.


@asset(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    },
)
def raw_xbrl_metadata_json(context) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Extract the FERC 1 XBRL Taxonomy metadata we've stored as JSON.

    Returns:
        A dictionary keyed by PUDL table name, with an instant and a duration entry
        for each table, corresponding to the metadata for each of the respective instant
        or duration tables from XBRL if they exist. Table metadata is returned as a list
        of dictionaries, each of which can be interpreted as a row in a tabular
        structure, with each row annotating a separate XBRL concept from the FERC 1
        filings. If there is no instant/duration table, an empty list is returned
        instead.
    """
    metadata_path = (
        Path(context.op_config["pudl_output_path"])
        / "ferc1_xbrl_taxonomy_metadata.json"
    )
    with open(metadata_path) as f:
        xbrl_meta_all = json.load(f)

    valid_tables = {
        table_name: xbrl_table
        for table_name in TABLE_NAME_MAP_FERC1
        if (xbrl_table := TABLE_NAME_MAP_FERC1.get(table_name, {}).get("xbrl"))
        is not None
    }

    def squash_period(xbrl_table: str | list[str], period, xbrl_meta_all):
        if type(xbrl_table) is str:
            xbrl_table = [xbrl_table]
        return [
            metadata
            for table in xbrl_table
            for metadata in xbrl_meta_all.get(f"{table}_{period}", [])
            if metadata
        ]

    xbrl_meta_out = {
        table_name: {
            "instant": squash_period(xbrl_table, "instant", xbrl_meta_all),
            "duration": squash_period(xbrl_table, "duration", xbrl_meta_all),
        }
        for table_name, xbrl_table in valid_tables.items()
    }

    return xbrl_meta_out


# Ferc extraction functions for devtool notebook testing
def extract_dbf_generic(
    table_names: list[str],
    io_manager: FercDBFSQLiteIOManager,
    dataset_settings: DatasetsSettings,
) -> pd.DataFrame:
    """Combine multiple raw dbf tables into one.

    Args:
        table_names: The name of the raw dbf tables you want to combine
            under dbf. These are the tables you want to combine.
        io_manager: IO Manager that extracts tables from ferc1.sqlite as dataframes.
        dataset_settings: object containing desired years to extract.

    Return:
        Concatenation of all tables in table_names as a dataframe.
    """
    tables = []
    for table_name in table_names:
        context = build_input_context(
            asset_key=AssetKey(table_name),
            upstream_output=None,
            resources={"dataset_settings": dataset_settings},
        )
        tables.append(io_manager.load_input(context))
    return pd.concat(tables)


def extract_xbrl_generic(
    table_names: list[str],
    io_manager: FercXBRLSQLiteIOManager,
    dataset_settings: DatasetsSettings,
    period: Literal["duration", "instant"],
) -> pd.DataFrame:
    """Combine multiple raw dbf tables into one.

    Args:
        table_names: The name of the raw dbf tables you want to combine
            under xbrl. These are the tables you want to combine.
        io_manager: IO Manager that extracts tables from ferc1.sqlite as dataframes.
        dataset_settings: object containing desired years to extract.
        period: Either duration or instant, specific to xbrl data.

    Return:
        Concatenation of all tables in table_names as a dataframe.
    """
    tables = []
    for table_name in table_names:
        full_xbrl_table_name = f"{table_name}_{period}"
        context = build_input_context(
            asset_key=AssetKey(full_xbrl_table_name),
            upstream_output=None,
            resources={"dataset_settings": dataset_settings},
        )
        tables.append(io_manager.load_input(context))
    return pd.concat(tables)


def extract_dbf(dataset_settings: DatasetsSettings) -> dict[str, pd.DataFrame]:
    """Coordinates the extraction of all FERC Form 1 tables into PUDL.

    This function is not used in the dagster ETL and is only intended
    to be used in notebooks for debugging the FERC Form 1 transforms.

    Args:
        dataset_settings: object containing desired years to extract.

    Returns:
        A dictionary of DataFrames, with the names of PUDL database tables as the keys.
        These are the raw unprocessed dataframes, reflecting the data as it is in the
        FERC Form 1 DB, for passing off to the data tidying and cleaning functions found
        in the :mod:`pudl.transform.ferc1` module.
    """
    ferc1_dbf_raw_dfs = {}

    io_manager_init_context = build_init_resource_context(
        resources={"dataset_settings": dataset_settings}
    )
    io_manager = ferc1_dbf_sqlite_io_manager(io_manager_init_context)

    for table_name, raw_table_mapping in TABLE_NAME_MAP_FERC1.items():
        dbf_table_or_tables = raw_table_mapping["dbf"]
        if not isinstance(dbf_table_or_tables, list):
            dbf_tables = [dbf_table_or_tables]
        else:
            dbf_tables = dbf_table_or_tables

        ferc1_dbf_raw_dfs[table_name] = extract_dbf_generic(
            dbf_tables, io_manager, dataset_settings
        )
    return ferc1_dbf_raw_dfs


def extract_xbrl(
    dataset_settings: DatasetsSettings,
) -> dict[str, dict[Literal["duration", "instant"], pd.DataFrame]]:
    """Coordinates the extraction of all FERC Form 1 tables into PUDL from XBRL data.

    This function is not used in the dagster ETL and is only intended
    to be used in notebooks for debugging the FERC Form 1 transforms.

    Args:
        dataset_settings: object containing desired years to extract.

    Returns:
        A dictionary where keys are the names of the PUDL database tables, values are
        dictionaries of DataFrames coresponding to the instant and duration tables from
        the XBRL derived FERC 1 database.
    """
    ferc1_xbrl_raw_dfs = {}

    io_manager_init_context = build_init_resource_context(
        resources={"dataset_settings": dataset_settings}
    )
    io_manager = ferc1_xbrl_sqlite_io_manager(io_manager_init_context)

    for table_name, raw_table_mapping in TABLE_NAME_MAP_FERC1.items():
        xbrl_table_or_tables = raw_table_mapping["xbrl"]
        if not isinstance(xbrl_table_or_tables, list):
            xbrl_tables = [xbrl_table_or_tables]
        else:
            xbrl_tables = xbrl_table_or_tables

        ferc1_xbrl_raw_dfs[table_name] = {}

        for period in ("duration", "instant"):
            ferc1_xbrl_raw_dfs[table_name][period] = extract_xbrl_generic(
                xbrl_tables, io_manager, dataset_settings, period
            )
    return ferc1_xbrl_raw_dfs
