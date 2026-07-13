# pudl.metadata.helpers

Functions for manipulating metadata constants.

## Exceptions

| [`AggregationError`](#pudl.metadata.helpers.AggregationError)   | Error raised by aggregation functions.   |
|-----------------------------------------------------------------|------------------------------------------|

## Functions

| [`format_errors`](#pudl.metadata.helpers.format_errors)(→ str)                                               | Format multiple errors into a single error.                           |
|--------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`_parse_field_names`](#pudl.metadata.helpers._parse_field_names)(→ list[str])                               | Parse field names.                                                    |
| [`_parse_foreign_key_rule`](#pudl.metadata.helpers._parse_foreign_key_rule)(→ list[dict])                    | Parse foreign key rule from resource descriptor.                      |
| [`_build_foreign_key_tree`](#pudl.metadata.helpers._build_foreign_key_tree)(→ dict[str, dict[tuple[str, ...) | Build foreign key tree.                                               |
| [`_traverse_foreign_key_tree`](#pudl.metadata.helpers._traverse_foreign_key_tree)(→ list[dict[str, Any]])    | Traverse foreign key tree.                                            |
| [`build_foreign_keys`](#pudl.metadata.helpers.build_foreign_keys)(→ dict[str, list[dict]])                   | Build foreign keys for each resource.                                 |
| [`split_period`](#pudl.metadata.helpers.split_period)(→ tuple[str, str | None])                              | Split the time period from a column name.                             |
| [`expand_periodic_column_names`](#pudl.metadata.helpers.expand_periodic_column_names)(→ list[str])           | Add smaller periods to a list of column names.                        |
| [`most_frequent`](#pudl.metadata.helpers.most_frequent)(→ Any)                                               | Return most frequent value (or error if none exists).                 |
| [`most_and_more_frequent`](#pudl.metadata.helpers.most_and_more_frequent)(→ Any)                             | Return the most frequent value if more frequent than `min_frequency`. |
| [`unique`](#pudl.metadata.helpers.unique)(→ Any)                                                             | Return single unique value (or error if none exists).                 |
| [`as_dict`](#pudl.metadata.helpers.as_dict)(→ dict[Any, list])                                               | Return dictionary of values, listed by index.                         |
| [`try_aggfunc`](#pudl.metadata.helpers.try_aggfunc)(→ collections.abc.Callable)                              | Wrap aggregate function in a try-except for error handling.           |
| [`groupby_apply`](#pudl.metadata.helpers.groupby_apply)(→ tuple[pandas.DataFrame, dict[Any, ...)             | Aggregate dataframe and capture errors (using apply).                 |
| [`groupby_aggregate`](#pudl.metadata.helpers.groupby_aggregate)(→ tuple[pandas.DataFrame, dict[Any, ...)     | Aggregate dataframe and capture errors (using aggregate).             |

## Module Contents

### pudl.metadata.helpers.format_errors(\*errors: [str](https://docs.python.org/3/library/stdtypes.html#str), title: [str](https://docs.python.org/3/library/stdtypes.html#str) = None, pydantic: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Format multiple errors into a single error.

* **Parameters:**
  * **errors** – Error messages.
  * **title** – Title for error messages.

### Examples

```pycon
>>> e = format_errors('worse', title='bad')
>>> print(e)
bad
* worse
>>> e = format_errors('worse', title='bad', pydantic=True)
>>> print(e)
bad
  * worse
>>> e = format_errors('bad', 'worse')
>>> print(e)
* bad
* worse
>>> e = format_errors('bad', 'worse', pydantic=True)
>>> print(e)
* bad
  * worse
```

### pudl.metadata.helpers.\_parse_field_names(fields: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str) | [dict](https://docs.python.org/3/library/stdtypes.html#dict)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Parse field names.

* **Parameters:**
  **fields** – Either field names or field descriptors with a name key.
* **Returns:**
  Field names.

### pudl.metadata.helpers.\_parse_foreign_key_rule(rule: [dict](https://docs.python.org/3/library/stdtypes.html#dict), name: [str](https://docs.python.org/3/library/stdtypes.html#str), key: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]

Parse foreign key rule from resource descriptor.

* **Parameters:**
  * **meta** – Resource descriptor.
  * **name** – Resource name.
  * **key** – Resource primary key.
* **Returns:**
  * fields (List[str]): Local fields.
  * reference[‘resource’] (str): Reference resource name.
  * reference[‘fields’] (List[str]): Reference primary key fields.
  * exclude (List[str]): Names of resources to exclude, including name.
* **Return type:**
  Parsed foreign key rules

### pudl.metadata.helpers.\_build_foreign_key_tree(resources: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)]]

Build foreign key tree.

* **Parameters:**
  **resources** – Resource descriptors by name.
* **Returns:**
  Foreign key tree where the first key is a resource name (str),
  the second key is resource field names (Tuple[str, …]),
  and the value describes the reference resource (dict):
  * reference[‘resource’] (str): Reference name.
  * reference[‘fields’] (List[str]): Reference field names.

### pudl.metadata.helpers.\_traverse_foreign_key_tree(tree: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)]], name: [str](https://docs.python.org/3/library/stdtypes.html#str), fields: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]

Traverse foreign key tree.

* **Parameters:**
  * **tree** – Foreign key tree (see [`_build_foreign_key_tree()`](#pudl.metadata.helpers._build_foreign_key_tree)).
  * **name** – Local resource name.
  * **fields** – Local resource fields.
* **Returns:**
  * fields (List[str]): Local fields.
  * reference[‘resource’] (str): Reference resource name.
  * reference[‘fields’] (List[str]): Reference primary key fields.
* **Return type:**
  Sequence of foreign keys starting from name and fields

### pudl.metadata.helpers.build_foreign_keys(resources: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)], prune: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]

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

### pudl.metadata.helpers.split_period(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)]

Split the time period from a column name.

* **Parameters:**
  **name** – Column name.
* **Returns:**
  Base name and time period, if any.

### Examples

```pycon
>>> split_period('report_date')
('report', 'date')
>>> split_period('report_day')
('report_day', None)
>>> split_period('date')
('date', None)
```

### pudl.metadata.helpers.expand_periodic_column_names(names: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Add smaller periods to a list of column names.

* **Parameters:**
  **names** – Column names.
* **Returns:**
  Column names with additional names for smaller periods.

### Examples

```pycon
>>> expand_periodic_column_names(['id', 'report_year'])
['id', 'report_year', 'report_quarter', 'report_month', 'report_date']
```

### *exception* pudl.metadata.helpers.AggregationError

Bases: [`ValueError`](https://docs.python.org/3/library/exceptions.html#ValueError)

Error raised by aggregation functions.

### pudl.metadata.helpers.most_frequent(x: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → Any

Return most frequent value (or error if none exists).

### pudl.metadata.helpers.most_and_more_frequent(x: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), min_frequency: [float](https://docs.python.org/3/library/functions.html#float) = None) → Any

Return the most frequent value if more frequent than `min_frequency`.

The minimum frequency ignores null values, so for example, 1 in [1, 1, 1, nan]
has a frequency of 1.

* **Raises:**
  [**AggregationError**](#pudl.metadata.helpers.AggregationError) – if no value is more frequent than `min_frequency`.

### pudl.metadata.helpers.unique(x: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → Any

Return single unique value (or error if none exists).

### pudl.metadata.helpers.as_dict(x: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[Any, [list](https://docs.python.org/3/library/stdtypes.html#list)]

Return dictionary of values, listed by index.

### pudl.metadata.helpers.try_aggfunc(func: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable), raised: [bool](https://docs.python.org/3/library/functions.html#bool) = True, error: [str](https://docs.python.org/3/library/stdtypes.html#str) | [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable) = None) → [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)

Wrap aggregate function in a try-except for error handling.

* **Parameters:**
  * **func** – Aggregate function.
  * **raised** – Whether [`AggregationError`](#pudl.metadata.helpers.AggregationError) errors are raised or returned.
  * **error** – 

    Error value, whose type and format depends on raise.
    Below, x is the original input and e is the original error.
    * raised=True: A string with substitutions
      (e.g. ‘Error at {x.name}: {e}’)
      that replaces the arguments of the original error.
      By default, the original error is raised unchanged.
    * raised=False: A function with signature f(x, e)
      returning a value that replaces the arguments of the original error.
      By default, the original error is returned unchanged.
* **Returns:**
  Aggregate function with custom error handling.

### Examples

```pycon
>>> x = pd.Series([0, 0, 1, 1], index=['a', 'a', 'a', 'b'])
>>> most_frequent(x)
Traceback (most recent call last):
AggregationError: No value is most frequent.
>>> try_aggfunc(most_frequent, raised=False)(x)
AggregationError('No value is most frequent.')
>>> try_aggfunc(most_frequent, error='Bad dtype {x.dtype}')(x)
Traceback (most recent call last):
AggregationError: Bad dtype int64
>>> error = lambda x, e: as_dict(x)
>>> try_aggfunc(most_frequent, raised=False, error=error)(x)
AggregationError({'a': [0, 0, 1], 'b': [1]})
```

### pudl.metadata.helpers.groupby_apply(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), by: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable), aggfuncs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[Any, [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)], raised: [bool](https://docs.python.org/3/library/functions.html#bool) = True, error: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable) = None) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[Any, [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)]]

Aggregate dataframe and capture errors (using apply).

* **Parameters:**
  * **df** – Dataframe to aggregate.
  * **by** – Columns names to use to group rows (see [`pandas.DataFrame.groupby()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html#pandas.DataFrame.groupby)).
  * **aggfuncs** – Aggregation functions for columns not in by.
  * **raised** – Whether [`AggregationError`](#pudl.metadata.helpers.AggregationError) errors are raised or
    replaced with `np.nan` and returned in an error report.
  * **error** – A function with signature f(x, e) -> Tuple[Any, Any],
    where x is the original input and e is the original error,
    used when raised=False.
    The first and second value of the returned tuple are used as the
    index and values, respectively,
    of the [`pandas.Series`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series) returned for each column.
    By default, the first value is x.name
    (the values of columns by for that row group),
    and the second is the original error.
* **Returns:**
  Aggregated dataframe with by columns set as the index and
  an error report with (if raised=False)
  a [`pandas.Series`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series) for each column where errors occurred.

### Examples

```pycon
>>> df = pd.DataFrame({'x': [0, 0, 1, 1], 'y': pd.Series([2, 2, 2, 3], dtype='Int64')})
>>> df.index = [0, 0, 0, 1]
>>> base = dict(df=df, by='x', aggfuncs={'y': unique})
>>> groupby_apply(**base)
Traceback (most recent call last):
AggregationError: Could not aggregate y at x = 1: Not unique.
>>> _, report = groupby_apply(**base, raised=False)
>>> report['y']
x
1    Not unique.
dtype: object
>>> error = lambda x, e: (x.name, as_dict(x))
>>> _, report = groupby_apply(**base, raised=False, error=error)
>>> report['y']
x
1    {0: [2], 1: [3]}
dtype: object
```

### pudl.metadata.helpers.groupby_aggregate(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), by: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable), aggfuncs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[Any, [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)], raised: [bool](https://docs.python.org/3/library/functions.html#bool) = True, error: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable) = None) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[Any, [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)]]

Aggregate dataframe and capture errors (using aggregate).

Although faster than [`groupby_apply()`](#pudl.metadata.helpers.groupby_apply), it has some limitations:

* Raised errors cannot access the group index.
* Aggregation functions must return a scalar (must ‘reduce’).
  This is not a limitation with [`pandas.Series.apply()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.apply.html#pandas.Series.apply).

* **Parameters:**
  * **df** – Dataframe to aggregate.
  * **by** – Columns names to use to group rows (see [`pandas.DataFrame.groupby()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html#pandas.DataFrame.groupby)).
  * **aggfuncs** – Aggregation functions for columns not in by.
  * **raised** – Whether [`AggregationError`](#pudl.metadata.helpers.AggregationError) errors are raised or
    replaced with `np.nan` and returned in an error report.
  * **error** – A function with signature f(x, e) -> Any,
    where x is the original input and e is the original error,
    used when raised=False.
    By default, the original error is returned.
* **Returns:**
  Aggregated dataframe with by columns set as the index and
  an error report with (if raised=False)
  a [`pandas.Series`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series) of errors (or the value returned by error)
  for each column where errors occurred.

### Examples

```pycon
>>> df = pd.DataFrame({
...     'x': [0, 0, 1, 1],
...     'y': pd.Series([2, 2, 2, 3], dtype='Int64')
... })
>>> df.index = [0, 0, 0, 1]
>>> base = dict(df=df, by='x', aggfuncs={'y': unique})
>>> groupby_aggregate(**base)
Traceback (most recent call last):
AggregationError: Could not aggregate y: Not unique.
>>> result, report = groupby_aggregate(**base, raised=False)
>>> result
      y
x
0     2
1  <NA>
>>> report['y']
x
1    Not unique.
Name: y, dtype: object
>>> error = lambda x, e: as_dict(x)
>>> result, report = groupby_aggregate(**base, raised=False, error=error)
>>> report['y']
x
1    {0: [2], 1: [3]}
Name: y, dtype: object
```
