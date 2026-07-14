# pudl.scripts.resource_description

Tiny CLI for showing table descriptions without building the full docs.

## Functions

| [`main`](#pudl.scripts.resource_description.main)(→ None)   | Compute and display the description components for a resource.   |
|-------------------------------------------------------------|------------------------------------------------------------------|

## Module Contents

### pudl.scripts.resource_description.main(ctx: click.Context, name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Compute and display the description components for a resource.

These components are used to build the full resource description which goes into the
data dictionary, datapackage, and other downstream applications.

Useful when adding a new table, if you have the top-level structure installed in
[`pudl.metadata.resources`](../../metadata/resources/index.md#module-pudl.metadata.resources) but don’t yet have public documentation written.
