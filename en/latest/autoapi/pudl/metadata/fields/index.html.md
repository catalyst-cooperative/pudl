# pudl.metadata.fields

Field metadata.

## Attributes

| [`FIELD_METADATA`](#pudl.metadata.fields.FIELD_METADATA)                         | Field attributes by PUDL identifier (field.name).                        |
|----------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`FIELD_METADATA_BY_GROUP`](#pudl.metadata.fields.FIELD_METADATA_BY_GROUP)       | Field attributes by resource group (resource.group) and PUDL identifier. |
| [`FIELD_METADATA_BY_RESOURCE`](#pudl.metadata.fields.FIELD_METADATA_BY_RESOURCE) |                                                                          |

## Functions

| [`get_pudl_dtypes`](#pudl.metadata.fields.get_pudl_dtypes)(→ dict[str, Any])                     | Compile a dictionary of field dtypes, applying group overrides.            |
|--------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| [`apply_pudl_dtypes`](#pudl.metadata.fields.apply_pudl_dtypes)(...)                              | Apply dtypes to those columns in a dataframe that have PUDL types defined. |
| [`apply_pudl_dtypes_polars`](#pudl.metadata.fields.apply_pudl_dtypes_polars)(→ polars.LazyFrame) | Apply dtypes to those columns in a dataframe that have PUDL types defined. |

## Module Contents

### pudl.metadata.fields.FIELD_METADATA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

Field attributes by PUDL identifier (field.name).

### pudl.metadata.fields.FIELD_METADATA_BY_GROUP *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

Field attributes by resource group (resource.group) and PUDL identifier.

If a field exists in more than one data group (e.g. both `eia` and `ferc1`) and has
distinct metadata in those groups, this is the place to specify the override. Only those
elements which should be overridden need to be specified.

### pudl.metadata.fields.FIELD_METADATA_BY_RESOURCE *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

### pudl.metadata.fields.get_pudl_dtypes(group: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, field_meta: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = FIELD_METADATA, field_meta_by_group: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = FIELD_METADATA_BY_GROUP, dtype_map: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = FIELD_DTYPES_PANDAS) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Compile a dictionary of field dtypes, applying group overrides.

* **Parameters:**
  * **group** – The data group (e.g. ferc1, eia) to use for overriding the default
    field types. If None, no overrides are applied and the default types
    are used.
  * **field_meta** – Field metadata dictionary which at least describes a “type”.
  * **field_meta_by_group** – Field metadata type overrides to apply based on the data
    group that the field is part of, if any.
  * **dtype_map** – Mapping from canonical PUDL data types to some other set of data
    types. Uses pandas data types by default.
* **Returns:**
  A mapping of PUDL field names to their associated data types.

### pudl.metadata.fields.apply_pudl_dtypes(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), group: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, field_meta: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = FIELD_METADATA, field_meta_by_group: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = FIELD_METADATA_BY_GROUP, strict: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Apply dtypes to those columns in a dataframe that have PUDL types defined.

Note that ad-hoc column dtypes can be defined and merged with default PUDL field
metadata before it’s passed in as `field_meta` if you have module specific column
types you need to apply alongside the standard PUDL field types.

* **Parameters:**
  * **df** – The dataframe to apply types to. Not all columns need to have types
    defined in the PUDL metadata unless you pass `strict=True`.
  * **group** – The data group to use for overrides, if any. E.g. “eia”, “ferc1”.
  * **field_meta** – A dictionary of field metadata, where each key is a field name
    and the values are dictionaries which must have a “type” element. By
    default this is pudl.metadata.fields.FIELD_METADATA.
  * **field_meta_by_group** – A dictionary of field metadata to use as overrides,
    based on the value of group, if any. By default it uses the overrides
    defined in pudl.metadata.fields.FIELD_METADATA_BY_GROUP.
  * **strict** – whether or not all columns need a corresponding field.
* **Returns:**
  The input dataframe, but with standard PUDL types applied.

### pudl.metadata.fields.apply_pudl_dtypes_polars(lf: polars.LazyFrame, group: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, field_meta: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = FIELD_METADATA, field_meta_by_group: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = FIELD_METADATA_BY_GROUP, strict: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → polars.LazyFrame

Apply dtypes to those columns in a dataframe that have PUDL types defined.

Note that ad-hoc column dtypes can be defined and merged with default PUDL field
metadata before it’s passed in as `field_meta` if you have module specific column
types you need to apply alongside the standard PUDL field types.

* **Parameters:**
  * **df** – The dataframe to apply types to. Not all columns need to have types
    defined in the PUDL metadata unless you pass `strict=True`.
  * **group** – The data group to use for overrides, if any. E.g. “eia”, “ferc1”.
  * **field_meta** – A dictionary of field metadata, where each key is a field name
    and the values are dictionaries which must have a “type” element. By
    default this is pudl.metadata.fields.FIELD_METADATA.
  * **field_meta_by_group** – A dictionary of field metadata to use as overrides,
    based on the value of group, if any. By default it uses the overrides
    defined in pudl.metadata.fields.FIELD_METADATA_BY_GROUP.
  * **strict** – whether or not all columns need a corresponding field.
* **Returns:**
  The input dataframe, but with standard PUDL types applied.
