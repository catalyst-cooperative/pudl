"""Module to perform data cleaning functions on EIA176 data tables."""

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetIn,
    AssetOut,
    Output,
    asset,
    asset_check,
    multi_asset,
)

from pudl.analysis.record_linkage.name_cleaner import CompanyNameCleaner
from pudl.helpers import cleanstrings_series, simplify_columns
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)
name_cleaner = CompanyNameCleaner()

DROP_OPERATING_STATES = (
    "fed. gulf of mexico",
    "mexico",
    "other",
)

CONTINUATION_LINES_ALLOWED_NON_SUBDIVISION_CODES = {
    # EIA's query-system user guide describes the raw REF_CODE field as the code
    # associated with continuation cells, but does not publish a complete list of
    # country/reference codes. These are non-state/province/territory REF_CODE
    # values observed in the raw EIA-176 continuation lines. They are not normalized
    # to ISO-3166: some match ISO alpha-2 country codes, while others are
    # EIA-specific codes like CN for Canada, C2 for China, I2 for India, and JA for
    # Japan.
    "operating_state": {"FX", "MX"},
    "reference_state": {
        "AC",
        "AG",
        "AT",
        "AU",
        "BB",
        "BE",
        "BF",
        "BG",
        "BL",
        "C2",
        "C4",
        "CN",
        "CL",
        "DR",
        "EG",
        "EL",
        "ES",
        "FI",
        "FR",
        "FX",
        "GM",
        "GQ",
        "GR",
        "HA",
        "HR",
        "I2",
        "II",
        "IS",
        "IT",
        "JA",
        "JD",
        "JM",
        "KR",
        "KU",
        "LH",
        "M6",
        "MR",
        "MX",
        "MY",
        "NG",
        "NO",
        "OM",
        "OO",
        "PK",
        "PL",
        "PM",
        "PT",
        "QR",
        "RP",
        "RU",
        "SG",
        "SN",
        "TH",
        "TD",
        "TU",
        "TW",
        "UA",
        "UK",
        "XX",
        "YE",
    },
}

UNKNOWN_TYPES = (
    "",
    ".",
    "0",
    "na",
    "nan",
    "n/a",
    "n.a.",
    "none",
    "not applicable",
    "not available",
)

SUPPLEMENTAL_GASEOUS_FUEL_TYPE_MAP = {
    "air_injection": ["air injection", "air injections"],
    "biomass": ["biomass"],
    "biomass_gas": ["biomass gas"],
    "blast_furnace_gas": ["blast furnance"],
    "coke_oven_gas": ["coke oven", "coke oven gas"],
    "gas_holders": ["gas holders"],
    "leaks_condensate": ["leaks condensate"],
    "line_pressure": ["line pressure", "line pressure chan"],
    "manufactured_gas": ["manufactured gas"],
    "natural_gas": ["natural gas"],
    "other": ["other"],
    "propane_air": ["propane air"],
    "refinery_gas": ["refinery gas"],
    "truck": ["truck"],
    "unknown": [
        "no label - bad cod",
        "no label - bad code 9004",
        "no label - bad code 9090",
        *UNKNOWN_TYPES,
    ],
    "vented_flared": ["vented flared"],
}

OTHER_DISPOSITION_TYPE_MAP = {
    "del_items_from_2001_form": ["del items from 2001 form"],
    "franchise_gas": ["franchise gas"],
    "gas_holders": ["gas holders"],
    "leaks_condensate": ["leaks condensate"],
    "line_pressure": ["line pressure"],
    "lost_leaks_condensate": ["lost leaks condensate"],
    "migration": ["migration"],
    "natural_gas": ["natural gas"],
    "other": ["other"],
    "plant_fuel": ["plant fuel"],
    "plant_thermal_reduction": ["plant ptr (extraction los"],
    "propane_air": ["propane air"],
    "rail_or_barge": ["rail or barge"],
    "refinery_gas": ["refinery gas"],
    "truck": ["truck"],
    "unknown": [
        "no label - bad code",
        "no label - bad code 9001",
        "no label - bad code 9006",
        *UNKNOWN_TYPES,
    ],
    "vented_flared": ["vented flared"],
}


@multi_asset(
    outs={
        "_core_eia176__yearly_company_data": AssetOut(),
        "_core_eia176__yearly_aggregate_data": AssetOut(),
    },
)
def _core_eia176__numeric_data(
    raw_eia176__numeric_data: pd.DataFrame,
) -> tuple[Output, Output]:
    """Process EIA 176 custom report data into company and aggregate outputs.

    Take raw dataframe produced by querying all forms from the EIA 176 custom report
    and return two wide tables with primary keys and one column per variable.

    One table with data for each year and company, one with state- and US-level
    aggregates per year.

    """
    raw_eia176__numeric_data = raw_eia176__numeric_data.astype(
        {"report_year": int, "value": float}
    )

    aggregate_primary_key = ["report_year", "operating_state"]
    company_primary_key = aggregate_primary_key + ["operator_id_eia", "operator_name"]
    company_drop_columns = ["form_line_numbers", "unit_type", "line"]
    # We must drop 'id' here and cannot use it as primary key because its
    # arbitrary/duplicate in aggregate records. 'id' is a reliable ID only in the context
    # of granular company data
    aggregate_drop_columns = company_drop_columns + ["operator_id_eia", "operator_name"]

    # Clean up text columns to ensure string matching is precise
    for col in ["operator_name", "operating_state"]:
        raw_eia176__numeric_data[col] = (
            raw_eia176__numeric_data[col].str.strip().str.lower()
        )

    long_company = raw_eia176__numeric_data.loc[
        raw_eia176__numeric_data.operator_name != "total of all companies"
    ]
    wide_company = get_wide_table(
        long_table=long_company.drop(columns=company_drop_columns),
        primary_key=company_primary_key,
    )

    long_aggregate = raw_eia176__numeric_data.loc[
        raw_eia176__numeric_data.operator_name == "total of all companies"
    ]
    wide_aggregate = get_wide_table(
        long_table=long_aggregate.drop(columns=aggregate_drop_columns).dropna(
            subset=aggregate_primary_key
        ),
        primary_key=aggregate_primary_key,
    )

    return (
        Output(output_name="_core_eia176__yearly_company_data", value=wide_company),
        Output(output_name="_core_eia176__yearly_aggregate_data", value=wide_aggregate),
    )


def get_wide_table(long_table: pd.DataFrame, primary_key: list[str]) -> pd.DataFrame:
    """Take a 'long' or entity-attribute-value table and return a wide table with one column per attribute/variable."""
    unstacked = long_table.set_index(primary_key + ["variable_name"]).unstack(
        level="variable_name"
    )
    unstacked.columns = unstacked.columns.droplevel(0)
    unstacked = simplify_columns(unstacked)  # Clean up column names
    unstacked.columns.name = None  # gets rid of "item" name of columns index
    return unstacked.reset_index()


def _subdivision_code_map(
    core_pudl__codes_subdivisions: pd.DataFrame,
) -> pd.Series:
    """Map subdivision names and codes to canonical two-letter codes."""
    code_map = pd.concat(
        [
            core_pudl__codes_subdivisions.assign(
                key=lambda x: x["subdivision_name"].str.strip().str.casefold()
            ),
            core_pudl__codes_subdivisions.assign(
                key=lambda x: x["subdivision_code"].str.strip().str.casefold()
            ),
        ],
        ignore_index=True,
    )
    return code_map.drop_duplicates("key").set_index("key")["subdivision_code"]


def normalize_continuation_line_location_codes(
    df: pd.DataFrame,
    core_pudl__codes_subdivisions: pd.DataFrame,
    column: str,
) -> pd.DataFrame:
    """Validate and normalize EIA-176 continuation line location codes.

    Values matching ``core_pudl__codes_subdivisions`` by name or code are converted
    to canonical two-letter state, province, or territory codes. Known EIA
    continuation line codes that are not state, province, or territory codes are
    allowed through unchanged so unexpected values still fail loudly. These allowed
    values are raw EIA ``REF_CODE`` values, not standardized ISO country codes.
    """
    df = df.copy()
    code_map = _subdivision_code_map(core_pudl__codes_subdivisions)
    allowed_codes = CONTINUATION_LINES_ALLOWED_NON_SUBDIVISION_CODES.get(column, set())

    norm = df[column].astype("string").str.strip().str.casefold()
    converted = norm.map(code_map).astype("string")
    allowed = norm.str.upper().isin(allowed_codes)
    invalid = df.loc[converted.isna() & ~allowed & df[column].notna(), column]
    if not invalid.empty:
        raise ValueError(
            f"Unknown {column} values: {sorted(invalid.unique().tolist())!r}"
        )

    df[column] = converted.fillna(norm.str.upper())
    return df


def _get_continuation_code_type(
    continuation_code: pd.Series,
    core_pudl__codes_subdivisions: pd.DataFrame,
) -> pd.Series:
    """Classify EIA-176 continuation codes as subnational or national/other codes."""
    subdivision_codes = set(core_pudl__codes_subdivisions["subdivision_code"])
    continuation_code = continuation_code.astype("string").str.strip().str.upper()
    code_type = pd.Series(
        "national_or_other",
        index=continuation_code.index,
        dtype="string",
    )
    code_type = code_type.mask(continuation_code.isin(subdivision_codes), "subnational")
    return code_type.mask(continuation_code.isna(), pd.NA)


def _find_continuation_line_total_mismatches(
    detail_records: pd.DataFrame,
    _core_eia176__yearly_company_data: pd.DataFrame,
    company_total_column: str,
) -> pd.DataFrame:
    """Compare detailed continuation line totals with reported company-level totals."""
    primary_key = ["operator_id_eia", "report_year"]
    company_totals = _core_eia176__yearly_company_data[
        [
            *primary_key,
            company_total_column,
        ]
    ]
    company_totals = company_totals[company_totals[company_total_column].notna()]
    continuation_totals = detail_records.groupby(primary_key, as_index=False).agg(
        {"volume_mcf": "sum"}
    )
    comparison = company_totals.merge(
        continuation_totals,
        how="outer",
        on=primary_key,
        validate="1:1",
    )
    return comparison[
        (comparison[company_total_column] != comparison["volume_mcf"])
        & (comparison[company_total_column].notna() | comparison["volume_mcf"].notna())
    ]


@asset_check(
    asset="_core_eia176__yearly_company_data",
    additional_ins={"_core_eia176__yearly_aggregate_data": AssetIn()},
    blocking=True,
)
def validate_totals(
    _core_eia176__yearly_company_data: pd.DataFrame,
    _core_eia176__yearly_aggregate_data: pd.DataFrame,
) -> AssetCheckResult:
    """Compare reported and calculated totals for different geographical aggregates.

    EIA reports an adjustment company at the area-level, so these values are expected to
    be identical.  Once we validate this, we can preserve the detailed data and discard
    the aggregate data to remove duplicate information.
    """
    # First make it so we can directly compare reported aggregates to groupings of
    # granular data
    comparable_aggregates = _core_eia176__yearly_aggregate_data.sort_values(
        ["report_year", "operating_state"]
    ).fillna(0)

    # Group company data into state-level data and compare to reported totals
    state_data = (
        _core_eia176__yearly_company_data.drop(
            columns=["operator_id_eia", "operator_name"]
        )
        .groupby(["report_year", "operating_state"])
        .sum()
        .round(4)
        .reset_index()
    )
    aggregate_state = comparable_aggregates[
        comparable_aggregates.operating_state != "u.s. total"
    ].reset_index(drop=True)
    # Compare using the same columns
    state_diff = aggregate_state[state_data.columns].compare(state_data)

    # Group calculated state-level data into US-level data and compare to reported totals
    us_data = (
        state_data.drop(columns="operating_state")
        .groupby("report_year")
        .sum()
        .sort_values("report_year")
        .reset_index()
    )
    aggregate_us = (
        comparable_aggregates[comparable_aggregates.operating_state == "u.s. total"]
        .drop(columns="operating_state")
        .sort_values("report_year")
        .reset_index(drop=True)
    )
    # Compare using the same columns
    us_diff = aggregate_us[us_data.columns].compare(us_data)

    # 2024-11-28: "alternative_fuel_fleet_1_yes_0_no" is reported as 1 in aggregate data from 2005
    # through 2015. If we run into cases where the totals don't add up, check
    # to see that they are all this specific case and then ignore them.
    if not state_diff.empty:
        assert (
            state_diff.columns.levels[0] == ["alternative_fuel_fleet_1_yes_0_no"]
        ).all()
        assert (state_diff["alternative_fuel_fleet_1_yes_0_no"]["self"] == 1.0).all()
        assert (
            aggregate_us.loc[us_diff.index].report_year.unique() == range(2005, 2016)
        ).all()

    if not us_diff.empty:
        assert (
            us_diff.columns.levels[0] == ["alternative_fuel_fleet_1_yes_0_no"]
        ).all()
        assert (us_diff["alternative_fuel_fleet_1_yes_0_no"]["self"] == 1.0).all()
        assert (
            aggregate_us.loc[us_diff.index].report_year.unique() == (range(2005, 2016))
        ).all()

    return AssetCheckResult(passed=True)


@asset(io_manager_key="pudl_io_manager")
def core_eia176__yearly_gas_disposition_by_consumer(
    _core_eia176__yearly_company_data: pd.DataFrame,
    core_pudl__codes_subdivisions: pd.DataFrame,
) -> pd.DataFrame:
    """Produce annual company-level gas disposition by consumer class (EIA-176).

    Transforms company-level EIA-176 data into a normalized table with one row per
    ("report_year", "operator_id_eia", "operating_state", "customer_class", "revenue_class")
    and three value columns: "consumers", "revenue", and "volume_mcf".

    Processing:

    * Select sales and transport metrics for residential, commercial, industrial,
      electric_power, vehicle_fuel, and other.
    * Validate that ``sales_volume + transport_volume == total volume`` per customer
      class.
    * Normalize ``operating_state`` to two-letter subdivision codes via
      ``core_pudl__codes_subdivisions``; drop rows with unknown states (these rows must
      contain zeros across value columns).
    * Drop rows where all of ``consumers``/``revenue``/``volume_mcf`` are NULL.

    Args:
        _core_eia176__yearly_company_data: Wide company-level EIA-176 data with
            per-metric columns.
        core_pudl__codes_subdivisions: Mapping from ``subdivision_name`` to
            ``subdivision_code`` used to normalize ``operating_state``.

    Raises:
        AssertionError: If component volumes don’t sum to totals, or if rows with
            unknown ``operating_state`` contain non-zero values.

    Notes:
        - ``volume_mcf`` is thousand cubic feet (MCF).
        - ``consumers`` is a count; ``revenue`` is nominal USD as reported.
        - ``customer_class`` and ``revenue_class`` are returned as categoricals.
    """
    primary_key = ["report_year", "operator_id_eia", "operating_state"]

    keep = [
        # 1 ==========================================
        # 10.1
        "residential_sales_consumers",
        "residential_sales_revenue",
        "residential_sales_volume",
        # 11.1
        "residential_transport_consumers",
        "residential_transport_revenue",
        "residential_transport_volume",
        # 10.1+11.1
        # "residential_volume",
        # 2 ==========================================
        # 10.2
        "commercial_sales_consumers",
        "commercial_sales_revenue",
        "commercial_sales_volume",
        # 11.2
        "commercial_transport_consumers",
        "commercial_transport_revenue",
        "commercial_transport_volume",
        # 10.2 + 11.2
        # "commercial_volume",
        # 3 ==========================================
        # 10.3
        "industrial_sales_consumers",
        "industrial_sales_revenue",
        "industrial_sales_volume",
        # 11.3
        "industrial_transport_consumers",
        "industrial_transport_revenue",
        "industrial_transport_volume",
        # 10.3 + 11.3
        # "industrial_volume",
        # 4 ==========================================
        # 10.4
        "electric_power_sales_consumers",
        "electric_power_sales_revenue",
        "electric_power_sales_volume",
        # 11.4
        "electric_power_transport_consumers",
        "electric_power_transport_revenue",
        "electric_power_transport_volume",
        # 10.4 + 11.4
        # "electric_power_volume",
        # 5 ==========================================
        # 10.5
        "vehicle_fuel_sales_consumers",
        "vehicle_fuel_sales_revenue",
        "vehicle_fuel_sales_volume",
        # 11.5
        "vehicle_fuel_transport_consumers",
        "vehicle_fuel_transport_revenue",
        "vehicle_fuel_transport_volume",
        # 10.5 + 11.5
        # "vehicle_fuel_volume",
        # 6 ==========================================
        # 10.6
        "other_sales_consumers",
        "other_sales_revenue",
        "other_sales_volume",
        # 11.6
        "other_transport_consumers",
        "other_transport_volume",
        # 10.6 + 11.6
        # "other_volume",
    ]

    customer_classes = (
        "residential",
        "commercial",
        "industrial",
        "electric_power",
        "vehicle_fuel",
        "other",
    )

    # Assert that volume sums match, ignoring NA cases
    df = _core_eia176__yearly_company_data
    mismatched = sum(
        sum(
            df[f"{customer_class}_transport_volume"].notna()
            & df[f"{customer_class}_sales_volume"].notna()
            & df[f"{customer_class}_volume"].notna()
            & (
                (
                    df[f"{customer_class}_transport_volume"]
                    + df[f"{customer_class}_sales_volume"]
                )
                != df[f"{customer_class}_volume"]
            )
        )
        for customer_class in customer_classes
    )
    assert not mismatched, f"found {mismatched} mismatched total volumes"

    # Assert that there is an expected number of mismatched volume totals where one of the columns is null
    df = _core_eia176__yearly_company_data.fillna(0)
    mismatched = sum(
        sum(
            (
                df[f"{customer_class}_transport_volume"]
                + df[f"{customer_class}_sales_volume"]
            )
            != df[f"{customer_class}_volume"]
        )
        for customer_class in customer_classes
    )
    assert mismatched <= 28, (
        f"{mismatched} mismatched volume totals found, expected no more than 28."
    )

    df = _core_eia176__yearly_company_data.filter(primary_key + keep)

    df = _normalize_operating_states(core_pudl__codes_subdivisions, df)

    df = pd.melt(
        df, id_vars=primary_key, var_name="metric", value_name="value"
    ).reset_index()
    df[["customer_class", "revenue_class", "metric_type"]] = df["metric"].str.extract(
        rf"({'|'.join(customer_classes)})_(sales|transport)_(.+)$"
    )

    df = df.drop(columns="metric").reset_index()
    primary_key.extend(["customer_class", "revenue_class"])
    df = df.pivot(
        index=primary_key, columns="metric_type", values="value"
    ).reset_index()
    df.columns.name = None

    # Rename columns as needed
    df = df.rename(columns={"volume": "volume_mcf"})

    # Ensure all values in rows with null operating state are zeroes, and drop them
    assert (
        df[df["operating_state"].isna()][["consumers", "revenue", "volume_mcf"]]
        .sum()
        .sum()
        == 0
    )
    df = df.dropna(subset=["consumers", "revenue", "volume_mcf"], how="all")

    return df


@asset(io_manager_key="pudl_io_manager")
def core_eia176__yearly_gas_imports(
    raw_eia176__continuation_text_lines: pd.DataFrame,
    core_pudl__codes_subdivisions: pd.DataFrame,
    _core_eia176__yearly_company_data: pd.DataFrame,
) -> pd.DataFrame:
    """Produce company-level detailed annual gas imports (EIA-176, Line 3.0)."""
    keep = [
        "operator_id_eia",
        "report_year",
        "operating_state",
        "reference_state",
        "reference_company_or_line_description",
        "volume_mcf",
        "mode_of_transportation",
    ]
    df = raw_eia176__continuation_text_lines[
        raw_eia176__continuation_text_lines["line"] == 300
    ].filter(keep)

    # Clean up the existing codes
    df = normalize_continuation_line_location_codes(
        df,
        core_pudl__codes_subdivisions,
        column="operating_state",
    )
    df = normalize_continuation_line_location_codes(
        df,
        core_pudl__codes_subdivisions,
        column="reference_state",
    )

    # Clean up the supplier location
    df["supplier_location"] = (
        df["reference_state"].astype("string").str.strip().str.upper()
    )
    # Create a categorical column noting whether data is a state code
    # or something else.
    df["supplier_location_type"] = _get_continuation_code_type(
        df["supplier_location"],
        core_pudl__codes_subdivisions,
    )

    df = df.rename(
        columns={
            "reference_company_or_line_description": "supplier_name",
        }
    ).drop(columns="reference_state")

    df["mode_of_transportation"] = (
        df["mode_of_transportation"]
        .astype("string")
        .str.strip()
        .replace({".": pd.NA, "0": pd.NA})
        .str.casefold()
    )
    df = df[
        [
            "operator_id_eia",
            "report_year",
            "operating_state",
            "supplier_location",
            "supplier_location_type",
            "supplier_name",
            "mode_of_transportation",
            "volume_mcf",
        ]
    ]

    # check that data isn't too far off from the EIA aggregated data
    mismatches = _find_continuation_line_total_mismatches(
        detail_records=df,
        _core_eia176__yearly_company_data=_core_eia176__yearly_company_data,
        company_total_column="receipts_from_state_or_us_border_volume",
    )
    assert len(mismatches) <= 2, "More than 2 line 300 total mismatches"

    # Clean supplier name and map a few values to N/A
    df["supplier_name"] = name_cleaner.get_clean_data(df["supplier_name"])
    df["supplier_name"] = df["supplier_name"].replace(list(UNKNOWN_TYPES), pd.NA)

    return df.sort_values(
        [
            "report_year",
            "operator_id_eia",
            "operating_state",
            "supplier_location",
            "supplier_location_type",
            "supplier_name",
            "mode_of_transportation",
        ]
    ).reset_index(drop=True)


@asset(io_manager_key="pudl_io_manager")
def core_eia176__yearly_supplemental_gaseous_fuel_supplies(
    raw_eia176__continuation_text_lines: pd.DataFrame,
    core_pudl__codes_subdivisions: pd.DataFrame,
    _core_eia176__yearly_company_data: pd.DataFrame,
) -> pd.DataFrame:
    """Produce detailed annual supplemental gaseous fuel supplies (EIA-176, Line 6.0)."""
    primary_key = ["operator_id_eia", "report_year", "fuel_type"]
    keep = [
        "operator_id_eia",
        "report_year",
        "operating_state",
        "reference_company_or_line_description",
        "volume_mcf",
    ]
    df = raw_eia176__continuation_text_lines[
        raw_eia176__continuation_text_lines["line"] == 600
    ].filter(keep)
    df = normalize_continuation_line_location_codes(
        df,
        core_pudl__codes_subdivisions,
        column="operating_state",
    )
    df["fuel_type"] = cleanstrings_series(
        df["reference_company_or_line_description"], SUPPLEMENTAL_GASEOUS_FUEL_TYPE_MAP
    ).drop(columns=["reference_company_or_line_description"])

    # We combine two pairs of duplicate rows here, one set of which are zero values.
    assert df.duplicated(subset=primary_key).any().sum() <= 2, (
        f"{df.duplicated(subset=primary_key).any()}"
    )
    df = df.groupby(
        [*primary_key, "operating_state"], dropna=False, as_index=False
    ).agg({"volume_mcf": "sum"})
    df = df[
        ["operator_id_eia", "report_year", "operating_state", "fuel_type", "volume_mcf"]
    ]
    assert not df.duplicated(subset=primary_key).any()

    mismatches = _find_continuation_line_total_mismatches(
        detail_records=df,
        _core_eia176__yearly_company_data=_core_eia176__yearly_company_data,
        company_total_column="supplemental_gaseous_fuels_volume",
    )
    assert mismatches.empty, "Found line 600 total mismatches"

    return df.sort_values(
        ["report_year", "operator_id_eia", "operating_state", "fuel_type"]
    ).reset_index(drop=True)


@asset(io_manager_key="pudl_io_manager")
def core_eia176__yearly_gas_exports(
    raw_eia176__continuation_text_lines: pd.DataFrame,
    core_pudl__codes_subdivisions: pd.DataFrame,
    _core_eia176__yearly_company_data: pd.DataFrame,
) -> pd.DataFrame:
    """Produce detailed annual out-of-state gas deliveries (EIA-176, Line 14.0)."""
    keep = [
        "operator_id_eia",
        "report_year",
        "operating_state",
        "reference_state",
        "reference_company_or_line_description",
        "volume_mcf",
        "mode_of_transportation",
    ]
    df = raw_eia176__continuation_text_lines[
        raw_eia176__continuation_text_lines["line"] == 1400
    ].filter(keep)

    # Clean up location codes
    df = normalize_continuation_line_location_codes(
        df,
        core_pudl__codes_subdivisions,
        column="operating_state",
    )
    df = normalize_continuation_line_location_codes(
        df,
        core_pudl__codes_subdivisions,
        column="reference_state",
    )

    # Clean and categorize destination
    df["recipient_location"] = (
        df["reference_state"].astype("string").str.strip().str.upper()
    )
    df["recipient_location_type"] = _get_continuation_code_type(
        df["recipient_location"],
        core_pudl__codes_subdivisions,
    )
    df = df.rename(
        columns={
            "reference_company_or_line_description": "recipient_name",
        }
    ).drop(columns="reference_state")
    df["mode_of_transportation"] = (
        df["mode_of_transportation"]
        .astype("string")
        .str.strip()
        .replace({".": pd.NA, "0": pd.NA})
        .str.casefold()
    )
    df = df[
        [
            "operator_id_eia",
            "report_year",
            "operating_state",
            "recipient_location",
            "recipient_location_type",
            "recipient_name",
            "mode_of_transportation",
            "volume_mcf",
        ]
    ]

    mismatches = _find_continuation_line_total_mismatches(
        detail_records=df,
        _core_eia176__yearly_company_data=_core_eia176__yearly_company_data,
        company_total_column="deliveries_out_of_state_volume",
    )
    assert len(mismatches) <= 4, "More than 4 line 1400 total mismatches"

    df["recipient_name"] = name_cleaner.get_clean_data(df["recipient_name"])
    df["recipient_name"] = df["recipient_name"].replace(list(UNKNOWN_TYPES), pd.NA)

    return df.sort_values(
        [
            "report_year",
            "operator_id_eia",
            "operating_state",
            "recipient_location",
            "recipient_location_type",
            "recipient_name",
            "mode_of_transportation",
        ]
    ).reset_index(drop=True)


@asset(io_manager_key="pudl_io_manager")
def core_eia176__yearly_gas_disposition_other(
    raw_eia176__continuation_text_lines: pd.DataFrame,
    core_pudl__codes_subdivisions: pd.DataFrame,
    _core_eia176__yearly_company_data: pd.DataFrame,
) -> pd.DataFrame:
    """Produce detailed annual gas disposition to other uses (EIA-176, Line 18.4)."""
    primary_key = [
        "operator_id_eia",
        "report_year",
        "operating_state",
        "disposition_type",
    ]
    keep = [
        "operator_id_eia",
        "report_year",
        "operating_state",
        "reference_company_or_line_description",
        "volume_mcf",
    ]
    df = raw_eia176__continuation_text_lines[
        raw_eia176__continuation_text_lines["line"] == 1840
    ].filter(keep)
    df = normalize_continuation_line_location_codes(
        df,
        core_pudl__codes_subdivisions,
        column="operating_state",
    )
    df["disposition_type"] = cleanstrings_series(
        df["reference_company_or_line_description"], OTHER_DISPOSITION_TYPE_MAP
    )
    df = df.drop(columns=["reference_company_or_line_description"])
    df["report_year"] = df["report_year"].astype("int64")

    # Several hundred records have duplicated values by fuel type. We combine them here.
    df = df.groupby(primary_key, dropna=False, as_index=False).agg(
        {"volume_mcf": "sum"}
    )
    assert not df.duplicated(subset=primary_key).any()

    mismatches = _find_continuation_line_total_mismatches(
        detail_records=df,
        _core_eia176__yearly_company_data=_core_eia176__yearly_company_data,
        company_total_column="disposition_to_other_volume",
    )
    assert len(mismatches) <= 6, "More than 6 line 1840 total mismatches"

    return df.sort_values(
        ["report_year", "operator_id_eia", "operating_state", "disposition_type"]
    ).reset_index(drop=True)


@asset(io_manager_key="pudl_io_manager")
def core_eia176__yearly_gas_disposition(
    _core_eia176__yearly_company_data: pd.DataFrame,
    core_pudl__codes_subdivisions: pd.DataFrame,
    raw_eia176__continuation_text_lines: pd.DataFrame,
) -> pd.DataFrame:
    """Produce company-level gas disposition (EIA176, Lines 9.0 and 12.0-20.0)."""
    extras = ["operating_state"]

    keep = [
        # 9.0
        "heat_content_of_delivered_gas_btu_cf",
        # 12
        "facility_space_heat",  # 1
        "new_pipeline_fill_volume",  # 2
        "pipeline_dist_storage_compressor_use",  # 3
        "vaporization_liquefaction_lng_fuel",  # 4
        "vehicle_fuel_used_in_company_fleet",  # 5
        "other",  # 6
        # 13
        "underground_storage_injections_volume",  # 1
        "lng_storage_injections_volume",  # 1
        # 14
        "deliveries_out_of_state_volume",
        # 15
        "lease_use_volume",
        # 16
        "returns_for_repress_reinjection_volume",
        # 17
        "losses_from_leaks_volume",
        # 18
        "disposition_to_distribution_companies_volume",  # 1
        "disposition_to_other_pipelines_volume",  # 2
        "disposition_to_storage_operators_volume",  # 3
        "disposition_to_other_volume",  # 4
        # 19
        "total_disposition_volume",
        # 20
        "unaccounted_for",
    ]

    primary_key = ["operator_id_eia", "report_year"]

    # Keep only the columns we need
    df = _core_eia176__yearly_company_data.filter([*primary_key, *extras, *keep])

    # Add freeform textual data
    tl_text = raw_eia176__continuation_text_lines.filter(
        [*primary_key, "line", "reference_company_or_line_description"]
    )
    tl_text = tl_text[tl_text["line"] == 1260]
    tl_text = tl_text.drop("line", axis=1)
    tl_text = tl_text.rename(
        columns={
            "reference_company_or_line_description": (
                "operational_consumption_other_detail"
            )
        }
    )
    assert not tl_text[primary_key].duplicated().any(), (
        'Found multiple values in "gas consumed in company\'s operations" "other" field (12.6)'
    )
    df = df.merge(tl_text, how="left", validate="1:1")

    # Additionally, add granular data available for "Deliveries out of state" and "Disposition to other"
    # These are getting grouped and summed by operator/state/year
    # TODO (12/3/25): Breakout these unaggregated values into separate tables.
    tl = raw_eia176__continuation_text_lines
    tl = tl.groupby([*primary_key, "line"]).agg("sum").reset_index()
    tl = tl.pivot(index=primary_key, columns="line", values="volume_mcf").reset_index()
    tl = tl.filter([*primary_key, 1400, 1840])
    df = df.merge(tl, how="left", validate="1:1")

    # Ensure that the summed granular data generally matches relevant values in _core_eia176__yearly_company_data
    deliveries_out_of_state_mismatch = (
        (df["deliveries_out_of_state_volume"] != df[1400])
        & (df["deliveries_out_of_state_volume"].notna() | df[1400].notna())
    ).sum()
    assert deliveries_out_of_state_mismatch <= 4, (
        "More than 4 out of state deliveries total mismatches"
    )

    # 2001: 2
    # 2024: 4
    # [km feb 2026] - keep an eye on this. Two of these mismatches (1 2001, 1 2024) happen
    # bc disposition_to_other_volume is NA while line 1840 has valid data, which suggests
    # we should continue to prefer line 1840. But for the other three mismatches (1 2001,
    # 2 2024) disposition_to_other_volume always exceeds the value from 1840. If this pattern
    # continues, we may want to switch to only using 1840 when disposition_to_other_volume
    # is not available.
    max_disposition_mismatch = 6
    disposition_to_other_mismatch = (
        (df["disposition_to_other_volume"] != df[1840])
        & (df["disposition_to_other_volume"].notna() | df[1840].notna())
    ).sum()
    assert disposition_to_other_mismatch <= max_disposition_mismatch, (
        f"More than {max_disposition_mismatch} disposition-to-other mismatches"
    )

    # We assume that the granular data is more accurate, so we'll use that
    df = df.drop(
        columns=["deliveries_out_of_state_volume", "disposition_to_other_volume"]
    )
    df = df.rename(
        columns={
            1400: "deliveries_out_of_state_volume",
            1840: "disposition_to_other_volume",
        }
    )

    # Normalize operating states, drop data from outside of 50 states and NA records
    df = _normalize_operating_states(core_pudl__codes_subdivisions, df)
    df = df.dropna(subset=["operating_state"])
    df = df.dropna(subset=keep, how="all")

    # Replace 9999 and 0 values with nulls
    df.loc[
        (df["heat_content_of_delivered_gas_btu_cf"] == 9999)
        | (df["heat_content_of_delivered_gas_btu_cf"] == 0),
        "heat_content_of_delivered_gas_btu_cf",
    ] = pd.NA

    # Convert heat content of delivered gas to mmtbu/mcf
    df["heat_content_of_delivered_gas_btu_cf"] /= 1000

    # Renaming and reordering columns
    df = df.rename(
        columns={
            "heat_content_of_delivered_gas_btu_cf": (
                "delivered_gas_heat_content_mmbtu_per_mcf"
            ),
            "facility_space_heat": "operational_consumption_facility_space_heat_mcf",
            "new_pipeline_fill_volume": "operational_consumption_new_pipeline_fill_mcf",
            "pipeline_dist_storage_compressor_use": (
                "operational_consumption_compressors_mcf"
            ),
            "vaporization_liquefaction_lng_fuel": (
                "operational_consumption_lng_vaporization_liquefaction_mcf"
            ),
            "vehicle_fuel_used_in_company_fleet": (
                "operational_consumption_vehicle_fuel_mcf"
            ),
            "other": "operational_consumption_other_mcf",
            "underground_storage_injections_volume": (
                "operational_storage_underground_mcf"
            ),
            "lng_storage_injections_volume": "operational_lng_storage_injections_mcf",
            "lease_use_volume": "producer_lease_use_mcf",
            "returns_for_repress_reinjection_volume": (
                "producer_returned_for_repressuring_reinjection_mcf"
            ),
            "losses_from_leaks_volume": "losses_mcf",
            "disposition_to_distribution_companies_volume": (
                "disposition_distribution_companies_mcf"
            ),
            "disposition_to_other_pipelines_volume": "disposition_other_pipelines_mcf",
            "disposition_to_storage_operators_volume": (
                "disposition_storage_operators_mcf"
            ),
            "unaccounted_for": "unaccounted_for_mcf",
            "deliveries_out_of_state_volume": "disposition_out_of_state_mcf",
            "disposition_to_other_volume": "other_disposition_all_other_mcf",
            "total_disposition_volume": "total_disposition_mcf",
        }
    )

    # There is one instance where `losses_mcf` value is inverted
    df.loc[
        (df.operator_id_eia == "17617106KS") & (df.report_year == 2008), "losses_mcf"
    ] = df.loc[
        (df.operator_id_eia == "17617106KS") & (df.report_year == 2008), "losses_mcf"
    ].abs()

    return df


def _normalize_operating_states(
    core_pudl__codes_subdivisions, df, column: str = "operating_state"
):
    """Map full state names to their postal abbreviations.

    This uses the latest year of Census PEP data as the reference. If a full state name is not included in this data, it is set to NA.
    """
    df = df.copy()
    codes = (
        core_pudl__codes_subdivisions.assign(
            key=lambda d: d["subdivision_name"].str.strip().str.casefold()
        )
        .drop_duplicates("key")
        .set_index("key")["subdivision_code"]
    )
    norm = df[column].astype(str).str.strip().str.casefold()
    mask_valid = norm.isin(codes.index)
    mask_safe_drop = norm.isin(DROP_OPERATING_STATES)

    invalid = norm[~(mask_valid | mask_safe_drop)].unique()
    if len(invalid) > 0:
        raise ValueError(f"Unknown operating_state values: {invalid!r}")

    df[column] = norm.map(codes)
    return df
