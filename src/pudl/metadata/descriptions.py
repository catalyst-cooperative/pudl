"""Mechanisms and constants for setting standard resource descriptions."""

import re
from collections import namedtuple
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from pudl.metadata.sources import SOURCES
from pudl.metadata.warnings import USAGE_WARNINGS

layer_descriptions: dict = {
    "raw": (
        "Data has been extracted from original format, columns have been renamed for "
        "consistency, and multiple reporting periods have been concatenated, but no "
        "transformations or cleaning have been applied."
    ),
    "_core": (
        "Data has been cleaned but not tidied/normalized. Published only "
        "temporarily and may be removed without notice."
    ),
    "core": (
        "Data has been cleaned and organized into well-modeled tables that serve as "
        "building blocks for downstream wide tables and analyses."
    ),
    "_out": "Intermediate output table.",
    "out": (
        "Data has been expanded into a wide/denormalized format, with IDs and codes "
        "accompanied by human-readable names and descriptions."
    ),
    "test": (
        "Only used in unit and integration testing; not intended for public "
        "consumption."
    ),
}
"""Standard descriptive text to appear in the Processing section of resource descriptions."""

# TODO: add link to https://catalystcoop-pudl.readthedocs.io/en/latest/data_sources/{datasource_name}.html
# if we have a data_sources page for it.
source_descriptions: dict = {
    source_name: SOURCES[source_name]["title"] for source_name in SOURCES
} | {
    "eia": "EIA -- Mix of multiple EIA Forms",
    "epa": "EPA -- Mix of multiple EPA sources",
    "ferc": "FERC -- Mix of multiple FERC Forms",
    "ferc1_xbrl": "FERC 1 XBRL -- Post-2021 years of Annual Report of Major Electric Utilities",
    "ferc1_dbf": "FERC 1 DBF -- Pre-2021 years of Annual Report of Major Electric Utilities",
}
"""Standard descriptive text to appear in the Source section of resource descriptions."""


TableTypeFragments = namedtuple("TableTypeFragments", "subject conjunction")
table_type_fragments: dict[str, TableTypeFragments] = {
    "assn": TableTypeFragments("Association table", "providing connections between"),
    "codes": TableTypeFragments(
        "Code table containing descriptions of categorical codes", "for"
    ),
    "entity": TableTypeFragments("Entity table containing static information", "about"),
    "scd": TableTypeFragments(
        "Slowly changing dimension (SCD) table", "describing attributes of"
    ),
    "timeseries": TableTypeFragments("time series", "of"),
}
"""Standard descriptive text to appear in the Summary (first line) of resource descriptions.

These are split into fragments to permit some resources to do without any additional illustration beyond the basic table type.
Such resources should not provide description.description_summary, and the system will apply only the first fragment here.
If a resource provides description.description_summary, the system will apply both fragments from the corresponding table_type_fragments entry, so that the summary starts with standardized text but finishes with more specific information."""
NONE_TABLETYPE_FRAGMENTS = TableTypeFragments(None, None)

timeseries_resolution_fragments: dict = {
    "quarterly": "Quarterly",
    "yearly": "Annual",
    "monthly": "Monthly",
    "hourly": "Hourly",
}
"""Additional standard descriptive text to appear in the Summary (first line) of resource descriptions, for timeseries resources."""


# [jul 2025 kmm] This is a mirror of pudl.metadata.PudlMeta, which can't be included here because it creates a circular import.
# Collecting all the description mechanisms in one file seemed more important than a few lines of duplication, but if
# PudlMeta changes, we may wish to apply the same change here.
class DescriptionMeta(BaseModel):
    """A base model that configures some options for PUDL description classes."""

    model_config = ConfigDict(
        extra="forbid",
        validate_default=True,
        validate_assignment=True,
    )


class ResourceTrait(DescriptionMeta):
    """Keeps the categorical information for one facet of a resource together with its prose description.

    type - the category code; one of the keys from the dictionaries above
    description - the text which will be included in the resource description block. This could be
        one of the values from the dictionaries above, or a more complex string composed of multiple
        pieces of standardized text and manually-provided annotations.
    """

    type: str
    description: str


# [jul 2025 kmm] it feels like this should already exist somewhere but all I found were terrible Python Golf one-liners
def first_non_none(*args):
    """Return the first argument which is not None.

    Returns None only if no such argument is found.
    """
    for x in args:
        if x is not None:
            return x
    return None


class ResourceDescriptionBuilder:
    """Generate the static text of a resource description from its decomposed parts.

    Information for deciding what descriptive text to display in a resource description comes from several places:

        * The resource metadata dictionary, containing manually-specified description components in the "description" section
        * ResourceNameComponents, which automatically extracts appropriate description components from the resource id
        * The *_descriptions and *_fragments dictionaries in this file, which set standardized text for basic resource types
        * Some limited logic in this class which detects automatic usage warnings and primary key information

    In order to keep manually-specified and -maintained components to a minimum, most keys in the
    resource metadata dictionary's description section are optional. Keys which are not manually specified are filled
    in using ResourceNameComponents or left blank. See pudl.metadata.PudlResourceDescriptor.PudlDescriptionComponents for
    complete documentation on manually-specifiable keys.

    This class computes and stores "final" description components from all available inputs.
    There are six final description components:

        * summary
        * layer
        * source
        * primary_key
        * details
        * usage_warnings

    Each takes the form of a ResourceTrait (or list of ResourceTraits, in the case of usage_warnings),
    which include the description text along with any types/categories extracted along the way.

    This class then serves as the input to the resource_description template, which assembles the components into a static text block
    appropriate for including in a data dictionary, datapackage export, or sqlachemy operation.
    """

    def __init__(self, resource_id: str, settings: dict):
        """Compute and store all description components from manually-specified settings and automatic sources."""
        self.resource_id = resource_id
        defaults = ResourceNameComponents(name=resource_id)

        # make a copy and move all the description keys up a level
        settings = dict(settings)
        if isinstance(settings.get("description"), dict):
            settings = settings | settings["description"]

        # this is a little gross but calling 5 methods with the same signature individually makes me sad
        for trait in ["summary", "layer", "source", "primary_key", "details"]:
            getattr(self, f"_create_{trait}_description")(settings, defaults)
        self._assemble_usage_warnings(settings, defaults)

    def _create_summary_description(self, settings, defaults):
        """Compute the Summary (first line) of the resource description.

        The Summary is standardized based on table type, and if the table type is timeseries, the timeseries resolution.
        Any illustration, if present, is included after the timeseries resolution and table type text.
        If the timeseries resolution and table type aren't set manually and can't be auto detected, fall back to
        the description_summary key and use it as a complete sentence instead of just a predicate fragment.
        """
        timeseries_resolution = first_non_none(
            settings.get("timeseries_resolution"), defaults.timeseries_resolution
        )
        table_type = first_non_none(settings.get("table_type"), defaults.table_type)
        summary_annotation = settings.get("description_summary")
        # default NONE_TABLETYPE_FRAGMENTS: temporarily accept none tabletypes during migration
        type_fragments = table_type_fragments.get(table_type, NONE_TABLETYPE_FRAGMENTS)

        components = [
            timeseries_resolution_fragments.get(timeseries_resolution),
            type_fragments.subject,
        ]

        if summary_annotation is not None:
            components.extend(
                [
                    type_fragments.conjunction,
                    summary_annotation,
                ]
            )

        self.summary = ResourceTrait(
            type=f"{table_type}[{timeseries_resolution}]",
            description=" ".join(c for c in components if c is not None),
        )

    def _create_x_description(self, attr, lookup, settings, defaults):
        """Compute a generic component of the resource description.

        This function applies to any component whose computation is a straightforward cascade:

            1. Use the manually-specified text
            2. If that's not available, use the auto-extracted text
            3. Follow up with any additional manually-specified illustrative text
        """
        attr_value = first_non_none(settings.get(attr), getattr(defaults, attr))
        components = [lookup.get(attr_value), settings.get(f"description_{attr}")]
        setattr(
            self,
            attr,
            ResourceTrait(
                type=attr_value,
                description=" ".join(c for c in components if c is not None),
            ),
        )

    def _create_layer_description(self, settings, defaults):
        """Compute the processing layer component of the resource description."""
        self._create_x_description("layer", layer_descriptions, settings, defaults)

    def _create_source_description(self, settings, defaults):
        """Compute the data source component of the resource description."""
        self._create_x_description("source", source_descriptions, settings, defaults)

    def _create_primary_key_description(self, settings, defaults):
        """Compute the primary key component of the resource description.

        If a primary key is available in the resource schema, include a list of the primary key columns.

        If a primary key is not available, include standardized text and any manually-specified illustrative text for
        what each row contains and perhaps why a primary key is not appropriate for the resource.
        """
        has_primary_key = (
            "primary_key" in settings.get("schema", {})
            and len(settings["schema"]["primary_key"]) > 0
        )
        components = [
            ", ".join(settings["schema"]["primary_key"])
            if has_primary_key
            else "This table has no primary key.",
            settings.get("description_primary_key"),
        ]
        self.primary_key = ResourceTrait(
            type=str(has_primary_key),
            description=" ".join(c for c in components if c is not None),
        )

    def _create_details_description(self, settings, defaults):
        """Compute the details component of the resource description.

        There is no standardized text for this component, and manually specifying it is optional.
        """
        # TODO: remove this handling after migration is complete
        # temporary description->description_details passthrough for legacy description text
        if isinstance(settings.get("description"), str):
            settings["description_details"] = settings["description"]
        # end TODO: remove

        self.details = ResourceTrait(
            type=str("description_details" in settings),
            description=first_non_none(settings.get("description_details"), ""),
        )

    def _assemble_usage_warnings(self, settings, defaults):
        """Combine manually-provided warnings and automatically-detected warnings for the requested resource.

        We automatically detect and include the following usage warnings:

            * multiple_inputs - for all resources that specify more than one source_id as part of their toplevel metadata (not the description source, that's meant to be more summative)
            * ferc_is_hard - for all resources that include the string "ferc" in the resource id
        """
        usage_warnings = list(first_non_none(settings.get("usage_warnings"), []))
        if len(settings["sources"]) > 1:
            usage_warnings.append("multiple_inputs")
        if "ferc" in self.resource_id:
            usage_warnings.append("ferc_is_hard")
        self.usage_warnings = [
            ResourceTrait(type=uw, description=USAGE_WARNINGS[uw])
            if isinstance(uw, str)
            else ResourceTrait(**uw)
            for uw in usage_warnings
        ]

    def summarize(self) -> str:
        """Show all computed description components, including type/category information.

        This is suitable for low-overhead previews and debugging.
        """
        return f"""
{self.resource_id}
   Summary [{self.summary.type}]: {self.summary.description}
     Layer [{self.layer.type}]: {self.layer.description}
    Source [{self.source.type}]: {self.source.description}
        PK [{self.primary_key.type}]: {self.primary_key.description}
  Warnings [{len(self.usage_warnings)}]:{"\n\t".join([""] + [f"{uw.type} - {uw.description}" for uw in self.usage_warnings])}
   Details [{self.details.type}]:
{self.details.description}
"""

    def build(self, jinja_environment):
        """Render all description components into the full static description text block."""
        return (
            jinja_environment.get_template("resource_description.rst.jinja").render(
                descriptions=self
            )
        ).strip()


class ResourceNameComponents(DescriptionMeta):
    """Extract basic information from the name of a resource."""

    name: str
    """Resource name (aka table name)."""

    layer_options: str = "|".join(layer_descriptions.keys())
    source_options: str = "|".join(source_descriptions.keys())
    timeseries_resolution_options: str = "|".join(
        timeseries_resolution_fragments.keys()
    )
    table_type_options: str = "|".join(table_type_fragments.keys())

    table_name_pattern: str = rf"^(?P<layer>{layer_options})_(?P<source>{source_options})__(?P<timeseries_resolution>{timeseries_resolution_options}|)(?:_|)(?P<table_type>{table_type_options}|)(?:_|)(?:_|)(?P<slug>.*)$"

    _match = None

    @property
    def match(self):
        """Return the regex match for the table name."""
        if self._match is None:
            self._match = re.match(self.table_name_pattern, self.name)
        return self._match

    @property
    def layer(self):
        """Layer extracted from table name."""
        return self.match.group("layer")

    @property
    def source(self):
        """Source extracted from table name."""
        return self.match.group("source")

    @property
    def table_type(self):
        """Table type extracted from table name."""
        if tt := self.match.group("table_type"):
            return tt
        if self.match.group("timeseries_resolution"):
            return "timeseries"
        return None

    @property
    def timeseries_resolution(self):
        """Timeseries resolution extracted from table name."""
        return self.match.group("timeseries_resolution")

    @property
    def slug(self):
        """Slug extracted from table name.

        Possible use case: extract what is being associated from an assn table.
        """
        return self.match.group("slug")

    @model_validator(mode="after")
    def table_name_check(self: Self):
        """Check the expected pattern of the table name."""
        if not self.match:
            raise ValueError(
                f"Table name not formatted as expected. Table name found: {self.name}.\nExpected table name pattern: {self.table_name_pattern}"
            )
        return self
