# pudl.metadata.resources.ferc1

Table definitions for the FERC Form 1 data group.

## Attributes

| [`PLANT_AGGREGATION_HAZARD`](#pudl.metadata.resources.ferc1.PLANT_AGGREGATION_HAZARD)                     |                                                                       |
|-----------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`PLANT_PRIMARY_KEY_TEXT`](#pudl.metadata.resources.ferc1.PLANT_PRIMARY_KEY_TEXT)                         |                                                                       |
| [`DETAILED_ACCOUNTING_TABLES_WARNING`](#pudl.metadata.resources.ferc1.DETAILED_ACCOUNTING_TABLES_WARNING) |                                                                       |
| [`TABLE_DESCRIPTIONS`](#pudl.metadata.resources.ferc1.TABLE_DESCRIPTIONS)                                 |                                                                       |
| [`RESOURCE_METADATA`](#pudl.metadata.resources.ferc1.RESOURCE_METADATA)                                   | FERC Form 1 resource attributes by PUDL identifier (`resource.name`). |

## Module Contents

### pudl.metadata.resources.ferc1.PLANT_AGGREGATION_HAZARD

### pudl.metadata.resources.ferc1.PLANT_PRIMARY_KEY_TEXT *= 'The best approximation for primary keys for this table would be: \`\`report_year\`\`,...*

### pudl.metadata.resources.ferc1.DETAILED_ACCOUNTING_TABLES_WARNING

### pudl.metadata.resources.ferc1.TABLE_DESCRIPTIONS

### pudl.metadata.resources.ferc1.RESOURCE_METADATA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

FERC Form 1 resource attributes by PUDL identifier (`resource.name`).

Keys are in alphabetical order.

See [`pudl.metadata.helpers.build_foreign_keys()`](../../helpers/index.md#pudl.metadata.helpers.build_foreign_keys) for the expected format of
`foreign_key_rules`.
