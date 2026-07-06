# pudl.metadata.resources.eia

Definitions of data tables primarily coming from EIA 860/861/923.

## Attributes

| [`AGG_FREQS`](#pudl.metadata.resources.eia.AGG_FREQS)                 |                                                                                 |
|-----------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [`RESOURCE_METADATA`](#pudl.metadata.resources.eia.RESOURCE_METADATA) | Generic EIA resource attributes organized by PUDL identifier (`resource.name`). |

## Module Contents

### pudl.metadata.resources.eia.AGG_FREQS *= ['yearly', 'monthly']*

### pudl.metadata.resources.eia.RESOURCE_METADATA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

Generic EIA resource attributes organized by PUDL identifier (`resource.name`).

See [`pudl.metadata.helpers.build_foreign_keys()`](../../helpers/index.md#pudl.metadata.helpers.build_foreign_keys) for the expected format of
`foreign_key_rules`.
