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
    "changelog": TableTypeFragments("Changelog table", "tracking changes in"),
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

These are split into fragments to permit some resources to do without any additional descriptive text beyond the basic table type.
Such resources should not provide :attr:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text`,
and the system will apply only the first fragment here.
If a resource provides :attr:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text`,
the system will apply both fragments from the corresponding :data:`table_type_fragments` entry, so that the summary starts with standardized text but finishes with more specific information.

Examples:
    * Rendered summary: "Association table"

      * resource name: [layer]_[source]__assn_[slug]
      * :attr:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text`: not specified

    * Rendered summary: "Association table providing connections between cats and Catalysters."

      * resource name: [layer]_[source]__assn_[slug]
      * :attr:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text`: "cats and Catalysters."
"""
NONE_TABLETYPE_FRAGMENTS = TableTypeFragments(None, None)

timeseries_resolution_fragments: dict = {
    "quarterly": "Quarterly",
    "yearly": "Annual",
    "monthly": "Monthly",
    "hourly": "Hourly",
}
"""More standard descriptive text to appear in the Summary (first line) of resource descriptions, for timeseries resources."""


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

    * type - the category code; one of the keys from the dictionaries above
    * description - the text which will be included in the resource description block. This could be
      one of the values from the dictionaries above, or a more complex string composed of multiple
      pieces of standardized and manually-provided text.
    """

    type: str
    """Category code for this aspect of the resource.

    Primarily used for debugging descriptions; not shown to users."""
    description: str
    """Text to be included in the rendered resource description."""


# [jul 2025 kmm] it feels like this should already exist somewhere but all I found were terrible Python Golf one-liners
def first_non_none(*args):
    """Return the first argument which is not None.

    Useful when you have multiple candidates for a value, some of which are preferred but rarely available.
    Call with candidates in order of descending preference, and you'll get the most-preferred value available.

    Returns None only if no such argument is found.
    """
    for x in args:
        if x is not None:
            return x
    return None


class ResourceDescriptionBuilder:
    r"""Generate the static text of a resource description from its decomposed parts.

    Information for deciding what descriptive text to display in a resource description comes from several places:

    * The resource metadata dictionary, containing manually-specified description codes and text in the "description" section
    * :class:`ResourceNameComponents`, which automatically extracts appropriate description codes from the resource id
    * The \*_descriptions and \*_fragments dictionaries in this file, which set standardized text for each code
    * Some limited logic in this class which detects automatic usage warnings and primary key information

    In order to keep manually-specified and -maintained components to a minimum, most keys in the
    resource metadata dictionary's description section are optional. Keys which have not been manually specified are filled
    in using :class:`ResourceNameComponents` or left blank. See :class:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents` for
    complete documentation on manually-specifiable keys.

    This class computes and stores "final" description components from all available inputs.
    There are six final description components:

    * summary
    * layer
    * source
    * primary_key
    * details
    * usage_warnings

    Each takes the form of a :class:`ResourceTrait` (or list of :class:`ResourceTrait`, in the case of usage_warnings),
    which include the description text along with any types/categories extracted along the way.

    This class then serves as the input to the resource_description template, which assembles the components into a static text block
    appropriate for including in a data dictionary, datapackage export, or sqlachemy operation.
    """

    def __init__(self, resource_id: str, settings: dict):
        """Compute and store all description components from manually-specified settings and automatic sources.

        Args:
            resource_id: a snake-case string uniquely identifying the resource; aka the table name.
            settings: a dictionary of resource metadata, usually obtained as a :class:`~pudl.metadata.classes.PudlResourceDescriptor` model dump.
        """
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

    def _create_summary_description(self, settings, defaults: "ResourceNameComponents"):
        """Compute the summary component (first line) of the resource description.

        The summary is standardized based on table type, and if the table type is timeseries, the timeseries resolution.
        Any :attr:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text`, if present, is included after the standard text for the timeseries resolution and table type.
        If the timeseries resolution and table type aren't set manually and can't be auto detected, fall back to
        the :attr:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text` key and use it as a complete sentence instead of just a predicate fragment.
        """
        timeseries_resolution_code = first_non_none(
            settings.get("timeseries_resolution_code"),
            defaults.timeseries_resolution_code,
        )
        table_type_code = first_non_none(
            settings.get("table_type_code"), defaults.table_type_code
        )
        additional_summary_text = settings.get("additional_summary_text")
        # permit a none tabletype for weirdos like core_ferc714__respondent_id
        type_fragments = table_type_fragments.get(
            table_type_code, NONE_TABLETYPE_FRAGMENTS
        )

        # assemble the parts of the summary description in display order.
        # any or all of these may be none, depending on the resource name and manually specified information.
        components = [
            timeseries_resolution_fragments.get(timeseries_resolution_code),
            type_fragments.subject,
        ]

        # only include the conjunction fragment for the table type if there's manually-specified text to go after it.
        # conjunction may be none, depending on the resource name.
        if additional_summary_text is not None:
            components.extend(
                [
                    type_fragments.conjunction,
                    additional_summary_text,
                ]
            )

        self.summary = ResourceTrait(
            type=f"{table_type_code}[{timeseries_resolution_code}]",
            # glue all the component parts into a single string, skipping any missing bits.
            description=" ".join(c for c in components if c is not None),
        )

    def _create_x_description(
        self,
        attr: str,
        lookup: dict[str, str],
        settings: dict,
        defaults: "ResourceNameComponents",
    ):
        """Compute a generic component of the resource description.

        This function applies to any component whose computation is a straightforward cascade:

        1. Use the manually-specified category code
        2. If that's not available, use the auto-extracted category code
        3. Fetch the standard text for the computed category code
        4. Follow up with any additional manually-specified description text

        Args:
            attr: name of the key (exclude the _code suffix) for the category code for this component (shared with :class:`ResourceNameComponents`)
            lookup: dictionary containing the mapping from the category code to the corresponding display text
            settings: a dictionary of resource metadata, lightly-processed by the constructor to make description keys easier to access
            defaults: a :class:`ResourceNameComponents` instance for this resource, containing the default category codes as extracted from the resource id
        """
        attr_value = first_non_none(
            settings.get(f"{attr}_code"), getattr(defaults, f"{attr}_code")
        )

        # assemble the parts of the component description in display order.
        # any or all of these may be none, depending on the resource name and manually specified information.
        components = [lookup.get(attr_value), settings.get(f"additional_{attr}_text")]
        setattr(
            self,
            attr,
            ResourceTrait(
                type=attr_value,
                # glue all the component parts into a single string, skipping any missing bits.
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

        If a primary key is not available, include standardized text and any manually-specified description
        text from :attr:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_primary_key_text`
        for what each row contains and perhaps why a primary key is not appropriate for the resource.

        If the primary key is available, *and* :attr:`~pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_primary_key_text` is specified,
        the manually-specified text will be placed after the primary key listing.
        """
        has_primary_key = (
            "primary_key" in settings.get("schema", {})
            and len(settings["schema"]["primary_key"]) > 0
        )

        # assemble the parts of the pk description in display order.
        # the last part may be none.
        components = [
            ", ".join(settings["schema"]["primary_key"])
            if has_primary_key
            else "This table has no primary key.",
            settings.get("additional_primary_key_text"),
        ]
        self.primary_key = ResourceTrait(
            type=str(has_primary_key),
            # glue all the component parts into a single string, skipping any missing bits.
            description=" ".join(c for c in components if c is not None),
        )

    def _create_details_description(self, settings, defaults):
        """Compute the details component of the resource description.

        There is no standardized text for this component, and manually specifying it is optional.
        """
        # TODO: remove this handling after migration is complete
        # temporary description->description_details passthrough for legacy description text
        if isinstance(settings.get("description"), str):
            settings["additional_details_text"] = settings["description"]
        # end TODO: remove

        self.details = ResourceTrait(
            type=str(settings.get("additional_details_text") is not None),
            description=first_non_none(settings.get("additional_details_text"), ""),
        )

    def _assemble_usage_warnings(self, settings, defaults):
        """Combine manually-provided warnings and automatically-detected warnings for the requested resource.

        We automatically detect and include the following usage warnings:

        * multiple_inputs - for all resources that specify more than one source_id as part of their toplevel metadata (not description.source, that's meant to be more summative)
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
        """Render all description components into the full static description text block using the resource_description template."""
        return (
            jinja_environment.get_template("resource_description.rst.jinja").render(
                descriptions=self
            )
        ).strip()


class ResourceNameComponents(DescriptionMeta):
    """Extract basic information from the name of a resource."""

    name: str
    """Resource name (aka table name)."""

    # define match groups for the different parts of a resource name
    layer_options: str = "|".join(layer_descriptions.keys())
    source_options: str = "|".join(source_descriptions.keys())
    timeseries_resolution_options: str = "|".join(
        timeseries_resolution_fragments.keys()
    )
    table_type_options: str = "|".join(table_type_fragments.keys())

    resource_name_pattern: str = rf"^(?P<layer>{layer_options})_(?P<source>{source_options})__(?P<timeseries_resolution>{timeseries_resolution_options}|)(?:_|)(?P<table_type>{table_type_options}|)(?:_|)(?:_|)(?P<slug>.*)$"

    _match = None

    @property
    def match(self):
        """Return the regex match for the resource name."""
        if self._match is None:
            self._match = re.match(self.resource_name_pattern, self.name)
        return self._match

    @property
    def layer_code(self):
        """Layer extracted from resource name."""
        return self.match.group("layer")

    @property
    def source_code(self):
        """Source extracted from resource name."""
        return self.match.group("source")

    @property
    def table_type_code(self):
        """Table type extracted from resource name."""
        if tt := self.match.group("table_type"):
            return tt
        if self.match.group("timeseries_resolution"):
            return "timeseries"
        return None

    @property
    def timeseries_resolution_code(self):
        """Timeseries resolution extracted from resource name."""
        return self.match.group("timeseries_resolution")

    @property
    def slug_text(self):
        """Slug extracted from resource name.

        Possible use case: extract what is being associated from an assn table.
        """
        return self.match.group("slug")

    @model_validator(mode="after")
    def table_name_check(self: Self):
        """Check the expected pattern of the resource name."""
        if not self.match:  # pragma: no cover
            raise ValueError(
                f"Resource name not formatted as expected. Resource name found: {self.name}.\nExpected resource name pattern: {self.resource_name_pattern}"
            )
        return self
