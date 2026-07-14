# pudl.metadata.codes

Metadata for cleaning, re-encoding, and documenting coded data columns.

These dictionaries are used to create Encoder instances. Each key is a table name with
a sub dictionary that includes additional detail. The table names must end with the
data_source as a suffix (for EIA 860, 861 or 923 tables include `_eia`).

The table-specific dictionaries contain the following keys:

* ‘df’: A dataframe associating short codes with long descriptions and other information.
  Each dataframe needs at least three standard columns: “code”, “label”, “description”.
  The codes and lables must be unique. By convention, the “label“‘s are snake case.
* ‘code_fixes’: A dictionary mapping non-standard codes to canonical, standardized
  codes.
* ‘ignored_codes’: A list of non-standard codes which appear in the data, and will
  be set to NA.

## Attributes

| [`CODE_METADATA`](#pudl.metadata.codes.CODE_METADATA)                   |    |
|-------------------------------------------------------------------------|----|
| [`DISABLED_CODE_METADATA`](#pudl.metadata.codes.DISABLED_CODE_METADATA) |    |

## Module Contents

### pudl.metadata.codes.CODE_METADATA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

### pudl.metadata.codes.DISABLED_CODE_METADATA
