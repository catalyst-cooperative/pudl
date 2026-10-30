"""Standard usage warnings to reference in dynamic table descriptions.

This is the single source of truth for the warnings. The short ``description`` is
embedded in the descriptions of the tables a warning applies to, and
:func:`pudl.docs.data_dictionary.usage_warnings_to_rst` renders every warning that
has a ``title`` into the "PUDL Usage Warnings" documentation page.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class UsageWarning:
    """A standard usage warning that can be attached to a table."""

    description: str
    """One or two sentences, in RST, embedded in the description of every table the
    warning applies to."""

    title: str | None = None
    """Heading for the warning on the "PUDL Usage Warnings" page. Warnings without a
    title are not listed on that page."""

    details: str = ""
    """Additional RST, shown on the "PUDL Usage Warnings" page below the description.
    It isn't included in table descriptions. Write it as a self-contained block
    starting at column zero. It is indented when rendered."""


USAGE_WARNINGS: dict[str, UsageWarning] = {
    "multiple_inputs": UsageWarning(
        title="Multiple inputs",
        description="Contains information from multiple raw inputs.",
    ),
    "derived_values": UsageWarning(
        title="Derived values",
        description="Contains columns derived from inputs and not originally present in sources.",
    ),
    "imputed_values": UsageWarning(
        title="Imputed values",
        description="Contains rows where missing values were imputed.",
    ),
    "estimated_values": UsageWarning(
        title="Estimated values",
        description="Contains estimated values.",  # TODO: what do we mean here
    ),
    "incomplete_id_coverage": UsageWarning(
        title="Incomplete ID coverage",
        description="Not all IDs are present.",  # TODO: do we want to set a coverage threshold and only apply this when we don't meet it?
    ),
    "incomplete_value_coverage": UsageWarning(
        description="?",  # TODO: do we mean high rates of missingness? do we want to set a threshold?
    ),
    "low_coverage": UsageWarning(
        title="Low coverage",
        description="Table has known low coverage - either geographic or temporal or otherwise.",
    ),
    "redacted_values": UsageWarning(
        title="Redacted values",
        description="Some values have been redacted.",  # eg 88888
    ),
    "mixed_aggregations": UsageWarning(
        title="Mixed aggregations",
        description="Some entries contain aggregates that do not match the table type.",  # eg 99999
    ),
    "month_as_date": UsageWarning(
        title="Month as date",
        description="Date column arbitrarily uses the first of the month.",
    ),
    "no_leap_year": UsageWarning(
        title="No leap year",
        description="Date column disregards leap years to comply with Actual/365 (Fixed) standard.",
    ),
    "irregular_years": UsageWarning(
        title="Irregular years",
        description="Some years use a slightly different data definition.",
    ),
    "known_discrepancies": UsageWarning(
        title="Known discrepancies",
        description="Contains known calculation discrepancies.",
    ),
    "free_text": UsageWarning(
        title="Free text",
        description="Contains columns which may appear categorical, but are actually free text.",
    ),
    "early_release": UsageWarning(
        title="Early release",
        description="May contain early release data.",
        details=(
            "EIA releases some of its annual data early for immediate access to "
            "individual plant and generator level data. The data has not been fully "
            "edited and is inappropriate for use in aggregation. Data for certain "
            "plants may be excluding pending validation."
        ),
    ),
    "aggregation_hazard": UsageWarning(
        title="Aggregation hazard",
        description="Some columns contain subtotals; use caution when choosing columns to aggregate.",
    ),
    "scale_hazard": UsageWarning(
        title="Scale hazard",
        description="Large table; do not attempt to open with Excel.",  # TODO: set a threshold
    ),
    "outliers": UsageWarning(
        title="Outliers",
        description="Outliers present.",
    ),
    "missing_years": UsageWarning(
        title="Missing years",
        description="Some years are missing from the data record.",
    ),
    "ferc_is_hard": UsageWarning(
        title="FERC",
        description=(
            "FERC data is notoriously difficult to extract cleanly, and often contains free-form strings, "
            "non-labeled total rows and lack of IDs. See "
            "`Notable Irregularities <https://docs.catalyst.coop/pudl/en/latest/data_sources/ferc1.html#notable-irregularities>`_ "
            "for details."
        ),
    ),
    "discontinued_data": UsageWarning(
        title="Discontinued by the source",
        description="The original data is no longer being collected or reported in this way.",
    ),
    "discontinued_pudl": UsageWarning(
        title="Discontinued by us",
        description="PUDL does not currently update its copy of this data.",
        details="If you would be interested in funding additional updates to this data, get in touch!",
    ),
    "experimental_wip": UsageWarning(
        title="Experimental Work-In-Progress",
        description="This table is experimental and/or a work in progress and may change in the future.",
    ),
    "harvested": UsageWarning(
        title="Harvested",
        description=(
            "Data has been drawn from several EIA sources which are not always consistent with each other, and PUDL chooses "
            "the most consistent or relevant value to facilitate cross-referencing even if that means some values"
            " will differ from the raw sources. See "
            "`Harvesting <https://docs.catalyst.coop/pudl/en/latest/data_dictionaries/usage_warnings.html#harvested>`_ "
            "for details, and see "
            "`Entity Resolution Methodology <https://docs.catalyst.coop/pudl/en/latest/methodology/entity_resolution.html>`_ "
            "for a fuller conceptual overview."
        ),
        details="""\
When there are multiple values reported for the same entity, in most cases, PUDL
chooses the most consistent value reported which is found in at least 70% of
available entries, and if no value occurs more than 70% of the time, PUDL fills
in a null value. Internally, we refer to this process as **harvesting**.

The 70% threshold is the default, and we use different rules for columns with
additional requirements:

* Latitude and longitude are particularly noisy, and 70% consistency is not
  attainable very often. We use the 70% threshold when possible, but for records
  that don't meet the threshold, we do a second pass after rounding latitude and
  longitude to the nearest tenth of a degree.
* Generator operating date has an unusual pattern of missingness that permits the
  most recently reported operating date to be reliable when 70% consistency
  cannot otherwise be reached.
* We set the consistency threshold to 0% for a few columns so that we always get
  a value: Plant name, utility name and prime mover code.""",
    ),
    "harvested_rus": UsageWarning(  # TODO: If more attributes harvested, update to refer to static attributes.
        title="Harvested borrower names",
        description=(
            "Borrower name data has been drawn from reported values over multiple years and tables of data which are not always consistent with each other. PUDL chooses "
            "the most consistent borrower name to facilitate cross-referencing even if that means some values"
            " will differ from the raw sources."
        ),
    ),
    "harvesting_ingredients": UsageWarning(
        title="Harvesting ingredients",
        description=(
            "This table is meant for forensic purposes only. It contains all values which were used to "
            "choose canonical or golden-record. "
            "See `Entity Resolution Methodology <https://docs.catalyst.coop/pudl/en/latest/methodology/entity_resolution.html>`_ "
            "for a fuller conceptual overview."
        ),
    ),
}
