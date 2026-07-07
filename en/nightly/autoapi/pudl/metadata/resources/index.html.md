# pudl.metadata.resources

A subpackage to define and organize PUDL database tables by data group.

## Submodules

* [pudl.metadata.resources.allocate_gen_fuel](allocate_gen_fuel/index.md)
* [pudl.metadata.resources.censusdp1tract](censusdp1tract/index.md)
* [pudl.metadata.resources.eia](eia/index.md)
* [pudl.metadata.resources.eia176](eia176/index.md)
* [pudl.metadata.resources.eia191](eia191/index.md)
* [pudl.metadata.resources.eia860](eia860/index.md)
* [pudl.metadata.resources.eia860m](eia860m/index.md)
* [pudl.metadata.resources.eia861](eia861/index.md)
* [pudl.metadata.resources.eia923](eia923/index.md)
* [pudl.metadata.resources.eia930](eia930/index.md)
* [pudl.metadata.resources.eiaaeo](eiaaeo/index.md)
* [pudl.metadata.resources.eiaapi](eiaapi/index.md)
* [pudl.metadata.resources.epacems](epacems/index.md)
* [pudl.metadata.resources.ferc1](ferc1/index.md)
* [pudl.metadata.resources.ferc1_eia_record_linkage](ferc1_eia_record_linkage/index.md)
* [pudl.metadata.resources.ferc714](ferc714/index.md)
* [pudl.metadata.resources.ferccid](ferccid/index.md)
* [pudl.metadata.resources.ferceqr](ferceqr/index.md)
* [pudl.metadata.resources.glue](glue/index.md)
* [pudl.metadata.resources.gridpathratoolkit](gridpathratoolkit/index.md)
* [pudl.metadata.resources.nrelatb](nrelatb/index.md)
* [pudl.metadata.resources.phmsagas](phmsagas/index.md)
* [pudl.metadata.resources.pudl](pudl/index.md)
* [pudl.metadata.resources.rus](rus/index.md)
* [pudl.metadata.resources.rus12](rus12/index.md)
* [pudl.metadata.resources.rus7](rus7/index.md)
* [pudl.metadata.resources.sec10k](sec10k/index.md)
* [pudl.metadata.resources.vcerare](vcerare/index.md)

## Attributes

| [`RESOURCE_METADATA`](#pudl.metadata.resources.RESOURCE_METADATA)   |                                                                                |
|---------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`module`](#pudl.metadata.resources.module)                         |                                                                                |
| [`FOREIGN_KEYS`](#pudl.metadata.resources.FOREIGN_KEYS)             | Generated foreign key constraints by resource name.                            |
| [`ENTITIES`](#pudl.metadata.resources.ENTITIES)                     | Columns kept for either entity or annual EIA tables in the harvesting process. |

## Functions

| [`build_foreign_keys`](#pudl.metadata.resources.build_foreign_keys)(→ dict[str, list[dict]])   | Build foreign keys for each resource.   |
|------------------------------------------------------------------------------------------------|-----------------------------------------|

## Package Contents

### pudl.metadata.resources.build_foreign_keys(resources: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)], prune: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]

Build foreign keys for each resource.

A resource’s foreign_key_rules (if present) determines which other resources will
be assigned a foreign key (foreign_keys) to the reference’s primary key:

* fields (list[list[str]]): Sets of field names for which to create a foreign key.
  These are assumed to match the order of the reference’s primary key fields.
* exclude (Optional[list[str]]): Names of resources to exclude.

* **Parameters:**
  * **resources** – Resource descriptors by name.
  * **prune** – Whether to prune redundant foreign keys.
* **Returns:**
  Foreign keys for each resource (if any), by resource name.
  * fields (list[str]): Field names.
  * reference[‘resource’] (str): Reference resource name.
  * reference[‘fields’] (list[str]): Reference resource field names.

### Examples

```pycon
>>> resources = {
...     'x': {
...         'schema': {
...             'fields': ['z'],
...             'primary_key': ['z'],
...             'foreign_key_rules': {'fields': [['z']]}
...         }
...     },
...     'y': {
...         'schema': {
...             'fields': ['z', 'yy'],
...             'primary_key': ['z', 'yy'],
...             'foreign_key_rules': {'fields': [['z', 'zz']]}
...         }
...     },
...     'z': {'schema': {'fields': ['z', 'zz']}}
... }
>>> keys = build_foreign_keys(resources)
>>> keys['z']
[{'fields': ['z', 'zz'], 'reference': {'resource': 'y', 'fields': ['z', 'yy']}}]
>>> keys['y']
[{'fields': ['z'], 'reference': {'resource': 'x', 'fields': ['z']}}]
>>> keys = build_foreign_keys(resources, prune=False)
>>> keys['z'][0]
{'fields': ['z'], 'reference': {'resource': 'x', 'fields': ['z']}}
```

### pudl.metadata.resources.RESOURCE_METADATA

### pudl.metadata.resources.module

### pudl.metadata.resources.FOREIGN_KEYS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]*

Generated foreign key constraints by resource name.

See [`pudl.metadata.helpers.build_foreign_keys()`](../helpers/index.md#pudl.metadata.helpers.build_foreign_keys).

### pudl.metadata.resources.ENTITIES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]]*

Columns kept for either entity or annual EIA tables in the harvesting process.

For each entity type (key), the ID columns, static columns, annual columns, and mapped
columns.

The order of the entities matters. Plants must be harvested before utilities, since
plant location must be removed before the utility locations are harvested.

`mapped_schemas` allows for harvesting an entity ID / value relationship
from multiple columns in the same input dataframe. Each item in `mapped_schemas` is a
dictionary mapping column names in one of the cleaned tables to the standard column names
for that entity.  This is useful if a table has entities that should be harvested,
but whose column names don’t have the same name as those in the `id_cols`, `static_cols`,
or `annual_cols` list. For example, in the ownership table the owner and operator utility
columns map to different column names in the other tables, i.e. “owner_utility_id_eia”: “utility_id_eia”.
In the harvesting process, a copy of the clean dataframe is made, and these columns are
renamed so the relationship can be harvested and added to the normalized entity tables.
