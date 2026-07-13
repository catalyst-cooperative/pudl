# pudl.validate.integrity

Database integrity validation checks for PUDL data.

This module implements checks for structural database constraints such as foreign key
relationships. These checks are applied after all data has been loaded into the database,
since the parallel nature of the ETL pipeline means that foreign key constraints cannot
be enforced during loading. As these checks are migrated into dbt, this module should
shrink accordingly.

## Attributes

| [`logger`](#pudl.validate.integrity.logger)   |    |
|-----------------------------------------------|----|

## Exceptions

| [`ForeignKeyError`](#pudl.validate.integrity.ForeignKeyError)   | Raised when data in a database violates a foreign key constraint.        |
|-----------------------------------------------------------------|--------------------------------------------------------------------------|
| [`ForeignKeyErrors`](#pudl.validate.integrity.ForeignKeyErrors) | Raised when data in a database violate multiple foreign key constraints. |

## Functions

| [`_get_fk_list`](#pudl.validate.integrity._get_fk_list)(→ pandas.DataFrame)   | Retrieve a dataframe of foreign keys for a table.   |
|-------------------------------------------------------------------------------|-----------------------------------------------------|
| [`check_foreign_keys`](#pudl.validate.integrity.check_foreign_keys)(engine)   | Check foreign key relationships in the database.    |

## Module Contents

### pudl.validate.integrity.logger

### *exception* pudl.validate.integrity.ForeignKeyError(child_table: [str](https://docs.python.org/3/library/stdtypes.html#str), parent_table: [str](https://docs.python.org/3/library/stdtypes.html#str), foreign_key: [str](https://docs.python.org/3/library/stdtypes.html#str), rowids: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)])

Bases: [`sqlalchemy.exc.SQLAlchemyError`](https://docs.sqlalchemy.org/en/21/core/exceptions.html#sqlalchemy.exc.SQLAlchemyError)

Raised when data in a database violates a foreign key constraint.

#### child_table

#### parent_table

#### foreign_key

#### rowids

#### \_\_str_\_()

Create string representation of ForeignKeyError object.

#### \_\_eq_\_(other)

Compare a ForeignKeyError with another object.

### *exception* pudl.validate.integrity.ForeignKeyErrors(fk_errors: [list](https://docs.python.org/3/library/stdtypes.html#list)[[ForeignKeyError](#pudl.validate.integrity.ForeignKeyError)])

Bases: [`sqlalchemy.exc.SQLAlchemyError`](https://docs.sqlalchemy.org/en/21/core/exceptions.html#sqlalchemy.exc.SQLAlchemyError)

Raised when data in a database violate multiple foreign key constraints.

#### fk_errors

#### \_\_str_\_()

Create string representation of ForeignKeyErrors object.

#### \_\_iter_\_()

Iterate over the fk errors.

#### \_\_getitem_\_(idx)

Index the fk errors.

### pudl.validate.integrity.\_get_fk_list(engine: sqlalchemy.Engine, table: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Retrieve a dataframe of foreign keys for a table.

Description from the SQLite Docs: ‘This pragma returns one row for each foreign
key constraint created by a REFERENCES clause in the CREATE TABLE statement of
table “table-name”.’

The PRAGMA returns one row for each field in a foreign key constraint. This
method collapses foreign keys with multiple fields into one record for
readability.

### pudl.validate.integrity.check_foreign_keys(engine: sqlalchemy.Engine)

Check foreign key relationships in the database.

The order assets are loaded into the database will not satisfy foreign key
constraints so we can’t enable foreign key constraints. However, we can
check for foreign key failures once all of the data has been loaded into
the database using the foreign_key_check and foreign_key_list PRAGMAs.

You can learn more about the PRAGMAs in the [SQLite docs](https://www.sqlite.org/pragma.html#pragma_foreign_key_check).

* **Raises:**
  [**ForeignKeyErrors**](#pudl.validate.integrity.ForeignKeyErrors) – if data in the database violate foreign key constraints.
