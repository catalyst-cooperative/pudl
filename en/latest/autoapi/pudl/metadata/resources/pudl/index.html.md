# pudl.metadata.resources.pudl

Definitions for the connection between PUDL-specific IDs and other datasets.

Most of this is compiled from handmapping records.

## Attributes

| [`RESOURCE_METADATA`](#pudl.metadata.resources.pudl.RESOURCE_METADATA)   | PUDL-specific resource attributes by PUDL identifier (`resource.name`).   |
|--------------------------------------------------------------------------|---------------------------------------------------------------------------|

## Module Contents

### pudl.metadata.resources.pudl.RESOURCE_METADATA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

PUDL-specific resource attributes by PUDL identifier (`resource.name`).

Keys are in alphabetical order.

See [`pudl.metadata.helpers.build_foreign_keys()`](../../helpers/index.md#pudl.metadata.helpers.build_foreign_keys) for the expected format of
`foreign_key_rules`.
