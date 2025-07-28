"""Standard usage warnings to reference in dynamic table descriptions."""

USAGE_WARNINGS = {
    "multiple_inputs": "Contains information from multiple raw inputs.",
    "derived_values": "Contains columns derived from inputs and not originally present in sources.",
    "imputed_values": "Contains rows where missing values were imputed.",
    "estimated_values": "Contains estimated values.",  # TODO: what do we mean here
    "incomplete_id_coverage": "Not all IDs are present.",  # TODO: do we want to set a coverage threshold and only apply this when we don't meet it?
    "incomplete_value_coverage": "?",  # TODO: do we mean high rates of missingness? do we want to set a threshold?
    "low_coverage": "Table has known low coverage - either geographic or temporal or otherwise.",
    "redacted_values": "Some values have been redacted.",  # eg 88888
    "mixed_aggregations": "Some entries contain aggregates that do not match the table type.",  # eg 99999
    "month_as_date": "Date column arbitrarily uses the first of the month.",
    "no_leap_year": "Date column disregards leap years to comply with Actual/365 (Fixed) standard.",
    "irregular_years": "Some years use a slightly different data definition.",
    "known_discrepancies": "Contains known calculation discrepancies.",
    "free_text": "Contains columns which may appear categorical, but are actually free text.",
    "early_release": "May contain early release data.",
    "aggregation_hazard": "Some columns contain subtotals; use caution when choosing columns to aggregate.",
    "scale_hazard": "Extremely large table; do not attempt to open with Excel.",  # TODO: set a threshold
    "outliers": "Outliers present.",
    "missing_years": "Some years are missing from the data record.",
    "ferc_is_hard": (
        "FERC data is notoriously difficult to extract cleanly, and often contains free-form strings, "
        "non-labeled total rows and lack of IDs. See "
        "`Notable Irregularities <https://catalystcoop-pudl.readthedocs.io/en/latest/data_sources/ferc1.html#notable-irregularities>`_ "
        "for details."
    ),
    "discontinued_data": "The data is no longer being collected or reported in this way.",
    "experimental_wip": "This table is experimental and/or a work in progress and may change in the future.",
}
