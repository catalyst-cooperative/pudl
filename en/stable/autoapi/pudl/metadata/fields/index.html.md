# pudl.metadata.fields

Field metadata.

## Attributes

| [`FIELD_METADATA`](#pudl.metadata.fields.FIELD_METADATA)                           | Field attributes by PUDL identifier (field.name).                        |
|------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`FIELD_METADATA_BY_NAMESPACE`](#pudl.metadata.fields.FIELD_METADATA_BY_NAMESPACE) | Field attributes by resource group (resource.group) and PUDL identifier. |
| [`FIELD_METADATA_BY_RESOURCE`](#pudl.metadata.fields.FIELD_METADATA_BY_RESOURCE)   |                                                                          |

## Module Contents

### pudl.metadata.fields.FIELD_METADATA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

Field attributes by PUDL identifier (field.name).

### pudl.metadata.fields.FIELD_METADATA_BY_NAMESPACE *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

Field attributes by resource group (resource.group) and PUDL identifier.

If a field exists in more than one data group (e.g. both `eia` and `ferc1`) and has
distinct metadata in those groups, this is the place to specify the override. Only those
elements which should be overridden need to be specified.

### pudl.metadata.fields.FIELD_METADATA_BY_RESOURCE *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*
