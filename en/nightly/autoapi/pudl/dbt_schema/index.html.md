# pudl.dbt_schema

Define dbt schema types and merging logic.

We generate dbt schema.yml files by translating our metadata into schema.yml
format, then applying human-sourced patches to the auto-generated schemas.

## Attributes

| [`_DESCRIPTION_WRAP_WIDTH`](#pudl.dbt_schema._DESCRIPTION_WRAP_WIDTH)   |    |
|----------------------------------------------------------------------------|----|
| [`_NormalizedDescription`](#pudl.dbt_schema._NormalizedDescription)    |    |
| [`_NormalizedDataTests`](#pudl.dbt_schema._NormalizedDataTests)      |    |

## Classes

| [`_LiteralStr`](#pudl.dbt_schema._LiteralStr)   | Marker subclass telling the dumper to use YAML literal block style (`|`).   |
|----------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`DbtColumn`](#pudl.dbt_schema.DbtColumn)     | Define yaml structure of a dbt column.                                      |
| [`DbtTable`](#pudl.dbt_schema.DbtTable)      | Define yaml structure of a dbt table.                                       |
| [`DbtSource`](#pudl.dbt_schema.DbtSource)     | Define basic dbt yml structure to add a pudl table as a dbt source.         |
| [`DbtSchema`](#pudl.dbt_schema.DbtSchema)     | Define basic structure of a dbt models yaml file.                           |

## Functions

| [`_normalize_whitespace`](#pudl.dbt_schema._normalize_whitespace)(→ str)                 | Collapse all whitespace (including blank lines) to single spaces.                  |
|-----------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| [`_normalize_descriptions`](#pudl.dbt_schema._normalize_descriptions)(→ Any)               | Recursively collapse whitespace in every `description` field.                      |
| [`_normalize_description_field`](#pudl.dbt_schema._normalize_description_field)(→ str | None)   |                                                                                    |
| [`_normalize_data_tests_field`](#pudl.dbt_schema._normalize_data_tests_field)(→ list | None)   |                                                                                    |
| [`_wrap_description`](#pudl.dbt_schema._wrap_description)(→ str | \_LiteralStr)      | Re-wrap a description string to short lines, for readability on disk.              |
| [`_wrap_descriptions`](#pudl.dbt_schema._wrap_descriptions)(→ Any)                    | Recursively re-wrap every `description` field in a dumped schema dict.             |
| [`_prettier_yaml_dumps`](#pudl.dbt_schema._prettier_yaml_dumps)(→ str)                  | Dump YAML to string that Prettier likes.                                           |
| [`_foreign_key_data_tests`](#pudl.dbt_schema._foreign_key_data_tests)(→ list[dict] | None) | Build `foreign_key` data test entries for a resource's outgoing FKs.               |
| [`merge_schema`](#pudl.dbt_schema.merge_schema)(→ DbtSchema)                    | Merge two DbtSchemas by applying human-schema as a patch on top of machine-schema. |
| [`merge_by_name`](#pudl.dbt_schema.merge_by_name)(→ list)                        | Perform a generic merge of two lists of dbt elements, matching by name.            |
| [`merge_sources_by_name`](#pudl.dbt_schema.merge_sources_by_name)(→ list[DbtSource])     | Match machine/human sources by name, then merge them.                              |
| [`merge_source`](#pudl.dbt_schema.merge_source)(→ DbtSource)                    | Merge two DbtSources by applying human-source as a patch on top of machine-source. |
| [`merge_tables_by_name`](#pudl.dbt_schema.merge_tables_by_name)(→ list[DbtTable])       | Match machine/human tables by name, then merge them.                               |
| [`merge_table`](#pudl.dbt_schema.merge_table)(→ DbtTable)                      | Merge two DbtTables by applying human-table as a patch on top of machine-table.    |
| [`merge_columns_by_name`](#pudl.dbt_schema.merge_columns_by_name)(→ list[DbtColumn])     | Match machine/human columns by name, then merge them.                              |
| [`merge_column`](#pudl.dbt_schema.merge_column)(→ DbtColumn)                    | Merge two DbtColumns by applying human-column as a patch on top of machine-column. |

## Module Contents

### pudl.dbt_schema.\_DESCRIPTION_WRAP_WIDTH *= 88*

### pudl.dbt_schema.\_normalize_whitespace(text: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Collapse all whitespace (including blank lines) to single spaces.

### pudl.dbt_schema.\_normalize_descriptions(obj: Any) → Any

Recursively collapse whitespace in every `description` field.

Normalizing whitespace at parse time reduces spurious diffs and round trip errors,
treating all whitespace as semantically identical.

The `description` fields show up in two places: the typed `description` field on
`DbtColumn`/`DbtTable`/`DbtSource`, and nested inside arbitrary `data_tests`
entries which are untyped `list` content that pydantic doesn’t otherwise inspect.
This function is used as a validator for both.

### pudl.dbt_schema.\_normalize_description_field(value: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

### pudl.dbt_schema.\_normalize_data_tests_field(value: [list](https://docs.python.org/3/library/stdtypes.html#list) | [None](https://docs.python.org/3/library/constants.html#None)) → [list](https://docs.python.org/3/library/stdtypes.html#list) | [None](https://docs.python.org/3/library/constants.html#None)

### pudl.dbt_schema.\_NormalizedDescription

### pudl.dbt_schema.\_NormalizedDataTests

### *class* pudl.dbt_schema.\_LiteralStr

Bases: [`str`](https://docs.python.org/3/library/stdtypes.html#str)

Marker subclass telling the dumper to use YAML literal block style (`|`).

Only used for `description` fields that we’ve re-wrapped ourselves, so
they read as human-friendly paragraphs on disk instead of one giant line.
Everything else (regexes, SQL snippets, argument lists) is left completely
alone, since forcing a global line width in the dumper risks reflowing
content where whitespace is significant.

### pudl.dbt_schema.\_wrap_description(text: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [\_LiteralStr](#pudl.dbt_schema._LiteralStr)

Re-wrap a description string to short lines, for readability on disk.

A description may already contain embedded newlines (e.g. from a blank
line in a folded YAML block scalar, or from a previous pass of this same
function). We preserve that line structure and only wrap the text
*within* each line, so we don’t invent new paragraph breaks the human
didn’t write. We use block style whenever the result spans multiple
lines – including when wrapping made no change to an already-wrapped
multi-line value – since a bare multi-line `str` would otherwise fall
back to an ugly quoted flow scalar. Single-line results are left as a
plain string so short descriptions keep their current compact
`description: ...` formatting.

### pudl.dbt_schema.\_wrap_descriptions(obj: Any) → Any

Recursively re-wrap every `description` field in a dumped schema dict.

### pudl.dbt_schema.\_prettier_yaml_dumps(yaml_contents: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Dump YAML to string that Prettier likes.

### pudl.dbt_schema.\_foreign_key_data_tests(resource: [pudl.metadata.classes.Resource](../metadata/classes/index.html.md#pudl.metadata.classes.Resource)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)] | [None](https://docs.python.org/3/library/constants.html#None)

Build `foreign_key` data test entries for a resource’s outgoing FKs.

One entry per foreign key relationship declared on the resource, in declaration
order, so regenerating a table’s schema.yml produces a stable diff.

### *class* pudl.dbt_schema.DbtColumn(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Define yaml structure of a dbt column.

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### description *: [\_NormalizedDescription](#pudl.dbt_schema._NormalizedDescription)* *= None*

#### data_tests *: [\_NormalizedDataTests](#pudl.dbt_schema._NormalizedDataTests)* *= None*

#### meta *: [dict](https://docs.python.org/3/library/stdtypes.html#dict) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### tags *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

### *class* pudl.dbt_schema.DbtTable(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Define yaml structure of a dbt table.

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### description *: [\_NormalizedDescription](#pudl.dbt_schema._NormalizedDescription)* *= None*

#### data_tests *: [\_NormalizedDataTests](#pudl.dbt_schema._NormalizedDataTests)* *= None*

#### columns *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtColumn](#pudl.dbt_schema.DbtColumn)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### meta *: [dict](https://docs.python.org/3/library/stdtypes.html#dict) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### tags *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### config *: [dict](https://docs.python.org/3/library/stdtypes.html#dict) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* from_table_name(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [DbtTable](#pudl.dbt_schema.DbtTable)

Construct configuration defining table from PUDL metadata.

### *class* pudl.dbt_schema.DbtSource(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Define basic dbt yml structure to add a pudl table as a dbt source.

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'pudl'*

#### tables *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtTable](#pudl.dbt_schema.DbtTable)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### description *: [\_NormalizedDescription](#pudl.dbt_schema._NormalizedDescription)* *= None*

#### meta *: [dict](https://docs.python.org/3/library/stdtypes.html#dict) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

### *class* pudl.dbt_schema.DbtSchema(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Define basic structure of a dbt models yaml file.

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### version *: [int](https://docs.python.org/3/library/functions.html#int)* *= 2*

#### sources *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtSource](#pudl.dbt_schema.DbtSource)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### models *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtTable](#pudl.dbt_schema.DbtTable)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* from_table_name(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [DbtSchema](#pudl.dbt_schema.DbtSchema)

Construct configuration defining table from PUDL metadata.

#### *classmethod* from_yaml(schema_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [DbtSchema](#pudl.dbt_schema.DbtSchema)

Load a DbtSchema object from a YAML file.

#### to_yaml(schema_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path))

Write DbtSchema object to YAML file.

#### validate_humanity()

Make sure the human schema matches expectations.

We expect that all human overrides on source tables are data tests or column-level data tests.
We allow the ‘name’ field so we can match human tables/columns with machine ones.

We do not have any expectations about model definitions since those are human-only.

### pudl.dbt_schema.merge_schema(machine_schema: [DbtSchema](#pudl.dbt_schema.DbtSchema), human_schema: [DbtSchema](#pudl.dbt_schema.DbtSchema)) → [DbtSchema](#pudl.dbt_schema.DbtSchema)

Merge two DbtSchemas by applying human-schema as a patch on top of machine-schema.

Empty merged sources will be stored in the DbtSchema model as None to avoid serializing them.

### pudl.dbt_schema.merge_by_name(machine_elements: [list](https://docs.python.org/3/library/stdtypes.html#list), human_elements: [list](https://docs.python.org/3/library/stdtypes.html#list), merger: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable), element_factory: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)) → [list](https://docs.python.org/3/library/stdtypes.html#list)

Perform a generic merge of two lists of dbt elements, matching by name.

* **Parameters:**
  * **machine_elements** – can be empty list.
  * **human_elements** – can be empty list.
  * **merger** – callable that takes two elements of the same dbt type (source, table,
    column) and returns a new element that is the merged version.
  * **element_factory** – callable that takes the element name and returns an empty instance - used if e.g. the human element doesn’t exist.

### pudl.dbt_schema.merge_sources_by_name(machine_sources: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtSource](#pudl.dbt_schema.DbtSource)], human_sources: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtSource](#pudl.dbt_schema.DbtSource)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtSource](#pudl.dbt_schema.DbtSource)]

Match machine/human sources by name, then merge them.

### pudl.dbt_schema.merge_source(machine_source: [DbtSource](#pudl.dbt_schema.DbtSource), human_source: [DbtSource](#pudl.dbt_schema.DbtSource)) → [DbtSource](#pudl.dbt_schema.DbtSource)

Merge two DbtSources by applying human-source as a patch on top of machine-source.

Returns a deep copy of the machine source to avoid aliasing,
updating with tables as the merge of the tables of the machine and human sources.

### pudl.dbt_schema.merge_tables_by_name(machine_tables: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtTable](#pudl.dbt_schema.DbtTable)], human_tables: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtTable](#pudl.dbt_schema.DbtTable)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtTable](#pudl.dbt_schema.DbtTable)]

Match machine/human tables by name, then merge them.

### pudl.dbt_schema.merge_table(machine_table: [DbtTable](#pudl.dbt_schema.DbtTable), human_table: [DbtTable](#pudl.dbt_schema.DbtTable)) → [DbtTable](#pudl.dbt_schema.DbtTable)

Merge two DbtTables by applying human-table as a patch on top of machine-table.

Returns a deep copy of the machine table to avoid aliasing,
updating with columns and table-level data tests as the merge of the respective machine and human data.

### pudl.dbt_schema.merge_columns_by_name(machine_columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtColumn](#pudl.dbt_schema.DbtColumn)], human_columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtColumn](#pudl.dbt_schema.DbtColumn)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[DbtColumn](#pudl.dbt_schema.DbtColumn)]

Match machine/human columns by name, then merge them.

### pudl.dbt_schema.merge_column(machine_column: [DbtColumn](#pudl.dbt_schema.DbtColumn), human_column: [DbtColumn](#pudl.dbt_schema.DbtColumn)) → [DbtColumn](#pudl.dbt_schema.DbtColumn)

Merge two DbtColumns by applying human-column as a patch on top of machine-column.

Returns a deep copy of the machine column to avoid aliasing,
updating with data tests as the merge of the data tests of the machine and human columns.

Does **not** update any other attributes (descriptions, etc.).
