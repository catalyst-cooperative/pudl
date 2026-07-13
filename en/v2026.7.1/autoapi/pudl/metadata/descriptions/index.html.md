# pudl.metadata.descriptions

Mechanisms and constants for setting standard resource descriptions.

## Attributes

| [`LAYER_DESCRIPTIONS`](#pudl.metadata.descriptions.LAYER_DESCRIPTIONS)                           | Standard descriptive text to appear in the Processing section of resource descriptions.                                  |
|--------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| [`SOURCE_DESCRIPTIONS`](#pudl.metadata.descriptions.SOURCE_DESCRIPTIONS)                         | Standard descriptive text to appear in the Source section of resource descriptions.                                      |
| [`TABLE_TYPE_FRAGMENTS`](#pudl.metadata.descriptions.TABLE_TYPE_FRAGMENTS)                       | Standard descriptive text to appear in the Summary (first line) of resource descriptions.                                |
| [`NONE_TABLETYPE_FRAGMENTS`](#pudl.metadata.descriptions.NONE_TABLETYPE_FRAGMENTS)               |                                                                                                                          |
| [`TIMESERIES_RESOLUTION_FRAGMENTS`](#pudl.metadata.descriptions.TIMESERIES_RESOLUTION_FRAGMENTS) | More standard descriptive text to appear in the Summary (first line) of resource descriptions, for timeseries resources. |
| [`PARTITION_OFFSETS`](#pudl.metadata.descriptions.PARTITION_OFFSETS)                             | Lookup table for computing offsets of temporal partitions.                                                               |

## Classes

| [`TableTypeFragments`](#pudl.metadata.descriptions.TableTypeFragments)                   |                                                                                                    |
|------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| [`DescriptionMeta`](#pudl.metadata.descriptions.DescriptionMeta)                         | A base model that configures some options for PUDL description classes.                            |
| [`ResourceTrait`](#pudl.metadata.descriptions.ResourceTrait)                             | Keeps the categorical information for one facet of a resource together with its prose description. |
| [`ResolvedResourceDescription`](#pudl.metadata.descriptions.ResolvedResourceDescription) | The fully-resolved components of a resource description.                                           |
| [`ResourceDescriptionBuilder`](#pudl.metadata.descriptions.ResourceDescriptionBuilder)   | Generate the static text of a resource description from its decomposed parts.                      |
| [`ResourceNameComponents`](#pudl.metadata.descriptions.ResourceNameComponents)           | Extract basic information from the name of a resource.                                             |

## Functions

| [`half_year_offset`](#pudl.metadata.descriptions.half_year_offset)(→ str)                      | Offset a half_year partition by the specified number of half_years.   |
|------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`first_non_none`](#pudl.metadata.descriptions.first_non_none)(\*args)                         | Return the first argument which is not None.                          |
| [`component`](#pudl.metadata.descriptions.component)(→ collections.abc.Callable[Ellipsis, ...) | Decorator for functions which resolve a description component.        |

## Module Contents

### pudl.metadata.descriptions.LAYER_DESCRIPTIONS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

Standard descriptive text to appear in the Processing section of resource descriptions.

### pudl.metadata.descriptions.SOURCE_DESCRIPTIONS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

Standard descriptive text to appear in the Source section of resource descriptions.

### *class* pudl.metadata.descriptions.TableTypeFragments

Bases: [`tuple`](https://docs.python.org/3/library/stdtypes.html#tuple)

#### subject

#### conjunction

### pudl.metadata.descriptions.TABLE_TYPE_FRAGMENTS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [TableTypeFragments](#pudl.metadata.descriptions.TableTypeFragments)]*

Standard descriptive text to appear in the Summary (first line) of resource descriptions.

These are split into fragments to permit some resources to do without any additional descriptive text beyond the basic table type.
Such resources should not provide [`additional_summary_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text),
and the system will apply only the first fragment here.
If a resource provides [`additional_summary_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text),
the system will apply both fragments from the corresponding `table_type_fragments` entry, so that the summary starts with standardized text but finishes with more specific information.

### Examples

* Rendered summary: “Association table”
  * resource name: [layer]_[source]_\_assn_[slug]
  * [`additional_summary_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text): not specified
* Rendered summary: “Association table providing connections between cats and Catalysters.”
  * resource name: [layer]_[source]_\_assn_[slug]
  * [`additional_summary_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text): “cats and Catalysters.”

### pudl.metadata.descriptions.NONE_TABLETYPE_FRAGMENTS

### pudl.metadata.descriptions.TIMESERIES_RESOLUTION_FRAGMENTS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

More standard descriptive text to appear in the Summary (first line) of resource descriptions, for timeseries resources.

### pudl.metadata.descriptions.half_year_offset(partition: [str](https://docs.python.org/3/library/stdtypes.html#str), offset: [int](https://docs.python.org/3/library/functions.html#int)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Offset a half_year partition by the specified number of half_years.

### pudl.metadata.descriptions.PARTITION_OFFSETS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)]*

Lookup table for computing offsets of temporal partitions.

Maps each partition key to a function that, given a partition (2026, 2026q3,
2026half1, 2026-07) and an integer offset, returns the partition `offset`
partition-units away from the input partition.

### *class* pudl.metadata.descriptions.DescriptionMeta(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

A base model that configures some options for PUDL description classes.

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

### *class* pudl.metadata.descriptions.ResourceTrait(/, \*\*data: Any)

Bases: [`DescriptionMeta`](#pudl.metadata.descriptions.DescriptionMeta)

Keeps the categorical information for one facet of a resource together with its prose description.

* type - the category code; one of the keys from the dictionaries above
* description - the text which will be included in the resource description block. This could be
  one of the values from the dictionaries above, or a more complex string composed of multiple
  pieces of standardized and manually-provided text.

#### type *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Category code for this aspect of the resource.

Primarily used for debugging descriptions; not shown to users.

#### description *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Text to be included in the rendered resource description.

### pudl.metadata.descriptions.first_non_none(\*args)

Return the first argument which is not None.

Useful when you have multiple candidates for a value, some of which are preferred but rarely available.
Call with candidates in order of descending preference, and you’ll get the most-preferred value available.

Returns None only if no such argument is found.

### *class* pudl.metadata.descriptions.ResolvedResourceDescription

The fully-resolved components of a resource description.

This class stores the different components of a resource description, as computed by [`ResourceDescriptionBuilder`](#pudl.metadata.descriptions.ResourceDescriptionBuilder).

There are seven description components:

* summary
* availability
* layer
* source
* primary_key
* details
* usage_warnings

Each takes the form of a [`ResourceTrait`](#pudl.metadata.descriptions.ResourceTrait) (or list of [`ResourceTrait`](#pudl.metadata.descriptions.ResourceTrait), in the case of usage_warnings),
which include the description text along with any types/categories extracted along the way.

This object serves as the input to the resource_description template, which assembles the components into a static text block
appropriate for including in a data dictionary, datapackage export, or sqlachemy operation.

#### resource_id *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### summary *: [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)*

#### availability *: [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)*

#### layer *: [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)*

#### source *: [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)*

#### primary_key *: [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)*

#### details *: [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)*

#### usage_warnings *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)]*

#### summarize() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Show all computed description components, including type/category information.

This is suitable for low-overhead previews and debugging.

#### jinja_render(jinja_environment)

Render all description components into the full static description text block using the resource_description template.

### pudl.metadata.descriptions.component(fn: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[Ellipsis, [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)]]) → [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[Ellipsis, [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)]]

Decorator for functions which resolve a description component.

### *class* pudl.metadata.descriptions.ResourceDescriptionBuilder(resource_id: [str](https://docs.python.org/3/library/stdtypes.html#str), settings: [dict](https://docs.python.org/3/library/stdtypes.html#dict))

Generate the static text of a resource description from its decomposed parts.

Information for deciding what descriptive text to display in a resource description comes from several places:

* The resource metadata dictionary, containing manually-specified description codes and text in the “description” section
* [`ResourceNameComponents`](#pudl.metadata.descriptions.ResourceNameComponents), which automatically extracts appropriate description codes from the resource id
* The \*_descriptions and \*_fragments dictionaries in this file, which set standardized text for each code
* Some limited logic in this class which detects automatic usage warnings and primary key information

In order to keep manually-specified and -maintained components to a minimum, most keys in the
resource metadata dictionary’s description section are optional. Keys which have not been manually specified are filled
in using [`ResourceNameComponents`](#pudl.metadata.descriptions.ResourceNameComponents) or left blank. See [`PudlDescriptionComponents`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents) for
complete documentation on manually-specifiable keys.

This class computes “final” description components from all available inputs, and outputs a [`ResolvedResourceDescription`](#pudl.metadata.descriptions.ResolvedResourceDescription) which may be rendered in one or more ways.

#### resource_id

#### defaults

#### settings

#### components

#### build()

Compute and store all description components from manually-specified settings and automatic sources.

#### *static* offset_source_availability(source, offset: [int](https://docs.python.org/3/library/functions.html#int)) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

Compute an availability date from the most recent available partition for a DataSource, offset by some number of partition-length units.

#### *static* compute_rowcounts_availability(resource_id) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

Compute an availability date from the most recent available partition for a resource, according to the row counts file.

#### summary(settings, defaults: [ResourceNameComponents](#pudl.metadata.descriptions.ResourceNameComponents)) → [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)

Compute the summary component (first line) of the resource description.

The summary is standardized based on table type, and if the table type is timeseries, the timeseries resolution.
Any [`additional_summary_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text), if present, is included after the standard text for the timeseries resolution and table type.
If the timeseries resolution and table type aren’t set manually and can’t be auto detected, fall back to
the [`additional_summary_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_summary_text) key and use it as a complete sentence instead of just a predicate fragment.

#### availability(settings, defaults) → [ResourceTrait](#pudl.metadata.descriptions.ResourceTrait)

Compute the availability component of the resource description.

If
[`availability_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.availability_text)
was set manually, use that.

If the table has temporal partitions in the dbt row counts file, use the
most recent row counts partition.

If the table has exactly one source, and that source has temporal
partitions, use the most recent source partition, optionally offset by
[`availability_offset`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.availability_offset).

Otherwise, use “Unknown” and set `ResourceTrait.type`=``False`` to
permit display logics to hide this component.

#### \_generic_component(attr: [str](https://docs.python.org/3/library/stdtypes.html#str), lookup: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)], settings: [dict](https://docs.python.org/3/library/stdtypes.html#dict), defaults: [ResourceNameComponents](#pudl.metadata.descriptions.ResourceNameComponents))

Compute a generic component of the resource description.

This function applies to any component whose computation is a straightforward cascade:

1. Use the manually-specified category code
2. If that’s not available, use the auto-extracted category code
3. Fetch the standard text for the computed category code
4. Follow up with any additional manually-specified description text

* **Parameters:**
  * **attr** – name of the key (exclude the \_code suffix) for the category code for this component (shared with [`ResourceNameComponents`](#pudl.metadata.descriptions.ResourceNameComponents))
  * **lookup** – dictionary containing the mapping from the category code to the corresponding display text
  * **settings** – a dictionary of resource metadata, lightly-processed by the constructor to make description keys easier to access
  * **defaults** – a [`ResourceNameComponents`](#pudl.metadata.descriptions.ResourceNameComponents) instance for this resource, containing the default category codes as extracted from the resource id

#### layer(settings, defaults)

Compute the processing layer component of the resource description.

#### source(settings, defaults)

Compute the data source component of the resource description.

#### primary_key(settings, defaults)

Compute the primary key component of the resource description.

If a primary key is available in the resource schema, include a list of the primary key columns.

If a primary key is not available, include standardized text and any manually-specified description
text from [`additional_primary_key_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_primary_key_text)
for what each row contains and perhaps why a primary key is not appropriate for the resource.

If the primary key is available, *and* [`additional_primary_key_text`](../classes/index.md#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_primary_key_text) is specified,
the manually-specified text will be placed after the primary key listing.

#### details(settings, defaults)

Compute the details component of the resource description.

There is no standardized text for this component, and manually specifying it is optional.

#### usage_warnings(settings, defaults)

Combine manually-provided warnings and automatically-detected warnings for the requested resource.

We automatically detect and include the following usage warnings:

* multiple_inputs - for all resources that specify more than one source_id as part of their toplevel metadata (not description.source, that’s meant to be more summative)
* ferc_is_hard - for all resources that include the string “ferc” in the resource id

### *class* pudl.metadata.descriptions.ResourceNameComponents(/, \*\*data: Any)

Bases: [`DescriptionMeta`](#pudl.metadata.descriptions.DescriptionMeta)

Extract basic information from the name of a resource.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Resource name (aka table name).

#### layer_options *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

#### source_options *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

#### timeseries_resolution_options *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

#### table_type_options *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

#### resource_name_pattern *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= '^(?P<layer>)_(?P<source>)_\_(?P<timeseries_resolution>|)(?:_|)(?P<table_type>|)(?:_|)(?:_|)(?P<slug>.\*)$'*

#### \_match *= None*

#### *property* match

Return the regex match for the resource name.

#### *property* layer_code

Layer extracted from resource name.

#### *property* source_code

Source extracted from resource name.

#### *property* table_type_code

Table type extracted from resource name.

#### *property* timeseries_resolution_code

Timeseries resolution extracted from resource name.

#### *property* slug_text

Slug extracted from resource name.

Possible use case: extract what is being associated from an assn table.

#### table_name_check()

Check the expected pattern of the resource name.
