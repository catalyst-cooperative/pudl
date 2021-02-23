"""Functions for manipulating metadata constants."""
from collections import defaultdict
from typing import (Any, Callable, Dict, Iterable, List, Literal, Optional,
                    Tuple, Union)

import numpy as np
import pandas as pd

# --- Foreign keys --- #


def _parse_field_names(fields: List[Union[str, dict]]) -> List[str]:
    """
    Parse field names.

    Args:
        fields: Either field names or field descriptors with a `name` key.

    Returns:
        Field names.
    """
    return [field if isinstance(field, str) else field["name"] for field in fields]


def _parse_foreign_key_rules(meta: dict, name: str) -> List[dict]:
    """
    Parse foreign key rules from resource descriptor.

    Args:
        meta: Resource descriptor.
        name: Resource name.

    Returns:
        Foreign key rules:
        * `fields` (List[str]): Local fields.
        * `reference['resource']` (str): Reference resource name.
        * `reference['fields']` (List[str]): Reference primary key fields.
        * `exclude` (List[str]): Names of resources to exclude, including `name`.
    """
    rules = []
    if "foreignKeyRules" in meta["schema"]:
        for fields in meta["schema"]["foreignKeyRules"]["fields"]:
            exclude = meta["schema"]["foreignKeyRules"].get("exclude", [])
            rules.append({
                "fields": fields,
                "reference": {"resource": name, "fields": meta["schema"]["primaryKey"]},
                "exclude": [name] + exclude
            })
    return rules


def _build_foreign_key_tree(
    resources: Dict[str, dict]
) -> Dict[str, Dict[Tuple[str], dict]]:
    """
    Build foreign key tree.

    Args:
        resources: Resource descriptors by name.

    Returns:
        Foreign key tree where the first key is a resource name (str),
        the second key is resource field names (Tuple[str]),
        and the value describes the reference resource (dict):
        * `reference['resource']` (str): Reference name.
        * `reference['fields']` (List[str]): Reference field names.
    """
    # Parse foreign key rules
    # [{fields: [], reference: {name: '', fields: []}, exclude: []}, ...]
    rules = []
    for name, meta in resources.items():
        rules.extend(_parse_foreign_key_rules(meta, name=name))
    # Build foreign key tree
    # [local_name][local_fields] => (reference_name, reference_fields)
    tree = defaultdict(dict)
    for name, meta in resources.items():
        fields = _parse_field_names(meta["schema"]["fields"])
        for rule in rules:
            local_fields = rule["fields"]
            if name not in rule["exclude"] and set(local_fields) <= set(fields):
                tree[name][tuple(local_fields)] = rule["reference"]
    return dict(tree)


def _traverse_foreign_key_tree(
    tree: Dict[str, Dict[Tuple[str], dict]],
    name: str,
    fields: Tuple[str]
) -> List[Tuple[tuple, str, tuple]]:
    """
    Traverse foreign key tree.

    Args:
        tree: Foreign key tree (see :func:`_build_foreign_key_tree`).
        name: Local resource name.
        fields: Local resource fields.

    Returns:
        Sequence of foreign keys starting from `name` and `fields`:
        * `fields` (List[str]): Local fields.
        * `reference['resource']` (str): Reference resource name.
        * `reference['fields']` (List[str]): Reference primary key fields.
    """
    keys = []
    if name not in tree or fields not in tree[name]:
        return keys
    ref = tree[name][fields]
    keys.append({"fields": list(fields), "reference": ref})
    if ref["resource"] not in tree:
        return keys
    for next_fields in tree[ref["resource"]]:
        if set(next_fields) <= set(ref["fields"]):
            for key in _traverse_foreign_key_tree(tree, ref["resource"], next_fields):
                mapped_fields = [
                    fields[ref["fields"].index(field)] for field in key["fields"]
                ]
                keys.append({"fields": mapped_fields, "reference": key["reference"]})
    return keys


def build_foreign_keys(
    resources: Dict[str, dict], prune: bool = True
) -> Dict[str, List[dict]]:
    """
    Build foreign keys for each resource.

    A resource's `foreignKeyRules` (if present)
    determines which other resources will be assigned a foreign key (`foreignKeys`)
    to the reference's primary key.
    * `fields` (List[List[str]]): Sets of field names for which to create a foreign key.
      These are assumed to match the order of the reference's primary key fields.
    * `exclude` (Optional[List[str]]): Names of resources to exclude.

    Args:
        resources: Resource descriptors by name.
        prune: Whether to prune redundant foreign keys.

    Returns:
        Foreign keys for each resource (if any), by resource name.
        * `fields` (List[str]): Field names.
        * `reference['resource']` (str): Reference resource name.
        * `reference['fields']` (List[str]): Reference resource field names.

    Examples:
        >>> resources = {
        ...     'x': {
        ...         'schema': {
        ...             'fields': ['z'],
        ...             'primaryKey': ['z'],
        ...             'foreignKeyRules': {'fields': [['z']]}
        ...         }
        ...     },
        ...     'y': {
        ...         'schema': {
        ...             'fields': ['z', 'yy'],
        ...             'primaryKey': ['z', 'yy'],
        ...             'foreignKeyRules': {'fields': [['z', 'zz']]}
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
    """
    tree = _build_foreign_key_tree(resources)
    keys = {}
    for name in tree:
        firsts = []
        followed = []
        for fields in tree[name]:
            path = _traverse_foreign_key_tree(tree, name, fields)
            firsts.append(path[0])
            followed.extend(path[1:])
        keys[name] = firsts
        if prune:
            # Keep key if not on path of other key
            keys[name] = [key for key in keys[name] if key not in followed]
    return keys


# --- Harvest --- #


PERIODS: Dict[str, Callable[[pd.Series], pd.Series]] = {
    "year": lambda x: x.astype("datetime64[Y]"),
    "quarter": lambda x: x.apply(
        pd.tseries.offsets.QuarterBegin(startingMonth=1).rollback
    ),
    "month": lambda x: x.astype("datetime64[M]"),
    "day": lambda x: x.astype("datetime64[D]"),
}
"""Functions converting timestamps to the start of the desired period."""


def split_period(name: str) -> Tuple[str, Optional[str]]:
    """
    Split the time period from a column name.

    Arguments:
        name: Column name.

    Returns:
        Base name and time period, if any.

    Examples:
        >>> split_period('report_day')
        ('report', 'day')
        >>> split_period('report_date')
        ('report_date', None)
        >>> split_period('day')
        ('day', None)
    """
    parts = name.rsplit("_", 1)
    if len(parts) == 1 or parts[1] not in PERIODS:
        return name, None
    return parts[0], parts[1]


def has_duplicate_basenames(names: Iterable[str]) -> bool:
    """
    Test whether column names contain duplicate base names.

    Arguments:
        names: Column names.

    Returns:
        Whether duplicate base names were found.

    Examples:
        >>> has_duplicate_basenames(['id', 'report_day'])
        False
        >>> has_duplicate_basenames(['id', 'report_day', 'report_month'])
        True
    """
    basenames = [split_period(name)[0] for name in names]
    return len(set(basenames)) != len(basenames)


def expand_periodic_column_names(names: Iterable[str]) -> List[str]:
    """
    Add smaller periods to a list of column names.

    Arguments:
        names: Column names.

    Returns:
        Column names with additional names for smaller periods.

    Examples:
        >>> expand_periodic_column_names(['id', 'report_year'])
        ['id', 'report_year', 'report_quarter', 'report_month', 'report_day']
    """
    periods = list(PERIODS)
    results = list(names)
    for name in names:
        base, period = split_period(name)
        if period in periods:
            results += [f"{base}_{p}" for p in periods[periods.index(period) + 1:]]
    return results


# ---- Aggregation: Column ---- #

"""
Aggregation functions.

All take a :class:`pd.Series` as input (and any optional keyword arguments).
They may either return a single value (ideally of the same data type as the input),
null (`np.nan`),
or raise a :class:`AggregationError` if the input does not meet requirements.
"""


class AggregationError(ValueError):
    """Error raised by aggregation functions."""

    pass


def most_frequent(x: pd.Series) -> Any:
    """Return most frequent value (or error if none exists)."""
    mode = x.mode(dropna=True)
    if mode.size == 1:
        return mode[0]
    if mode.empty:
        return np.nan
    raise AggregationError("No value is most frequent.")


def most_and_more_frequent(x: pd.Series, min_frequency: float = None) -> Any:
    """
    Return most frequent value if more frequent than minimum (or error if none exists).

    The minimum frequency ignores null values, so for example,
    `1` in `[1, 1, 1, nan]` has a frequency of 1.
    """
    x = x.dropna()
    mode = x.mode()
    if mode.size == 1:
        if min_frequency and min_frequency > (x == mode[0]).sum() / len(x):
            raise AggregationError(
                f"The most frequent value is less frequent than {min_frequency}."
            )
        return mode[0]
    if mode.empty:
        return np.nan
    raise AggregationError("No value is most frequent.")


def unique(x: pd.Series) -> Any:
    """Return single unique value (or error if none exists)."""
    x = x.dropna()
    if x.empty:
        return np.nan
    uniques = x.unique()
    if uniques.size == 1:
        return uniques[0]
    raise AggregationError("Not unique.")


def as_dict(x: pd.Series) -> Dict[Any, list]:
    """Return dictionary of values, listed by index."""
    result = {}
    x = x.dropna()
    for key, xi in x.groupby(x.index):
        result[key] = list(xi)
    return result


def try_aggfunc(  # noqa: C901
    func: Callable,
    method: Literal["coerce", "raise", "report"] = "coerce",
    error: Union[str, Callable] = None,
) -> Callable:
    """
    Wrap aggregate function in a try-except for error handling.

    Arguments:
        func: Aggregate function.
        method: Error handling method for :class:`AggregationError`.

            - 'coerce': Return `np.nan`.
            - 'raise': Raise the error.
            - 'report': Return the error.

        error: Error value, whose type and format depends on `method`.
            Below, `x` is the original input and `e` is the original error.

            - 'raise': A string with substitions (e.g. 'Error at {x.name}: {e}')
              that replaces the arguments of the original error.
              By default, the original error is raised unchanged.
            - 'report': A function with signature `f(x, e)` returning a value that
              replaces the arguments of the original error.
              By default, the original error is returned unchanged.

    Returns:
        Aggregate function with custom error handling.

    Examples:
        >>> import pandas as pd
        >>> x = pd.Series([0, 0, 1, 1], index=['a', 'a', 'a', 'b'])
        >>> most_frequent(x)
        Traceback (most recent call last):
          ...
        AggregationError: No value is most frequent.
        >>> try_aggfunc(most_frequent, 'coerce')(x)
        nan
        >>> try_aggfunc(most_frequent, 'report')(x)
        AggregationError('No value is most frequent.')
        >>> try_aggfunc(most_frequent, 'raise', 'Bad dtype {x.dtype}')(x)
        Traceback (most recent call last):
          ...
        AggregationError: Bad dtype int64
        >>> error = lambda x, e: as_dict(x)
        >>> try_aggfunc(most_frequent, 'report', error)(x)
        AggregationError({'a': [0, 0, 1], 'b': [1]})
    """
    # Conditional statements outside function for execution speed.
    wrapped = func
    if method == "coerce":

        def wrapped(x):
            try:
                return func(x)
            except AggregationError:
                return np.nan

    elif method == "raise" and error is not None:

        def wrapped(x):
            try:
                return func(x)
            except AggregationError as e:
                e.args = error.format(x=x, e=e),  # noqa: FS002
                raise e

    elif method == "report":
        if error is None:

            def wrapped(x):
                try:
                    return func(x)
                except AggregationError as e:
                    return e

        else:

            def wrapped(x):
                try:
                    return func(x)
                except AggregationError as e:
                    e.args = error(x, e),
                    return e

    return wrapped


# ---- Aggregation: Table ---- #


def groupby_apply(  # noqa: C901
    df: pd.DataFrame,
    by: Iterable,
    aggfuncs: Dict[Any, Callable],
    errors: Literal["raise", "coerce", "report"] = "raise",
    errorfunc: Callable = None,
) -> Tuple[pd.DataFrame, Dict[Any, pd.Series]]:
    """
    Aggregate dataframe and capture errors (using apply).

    Arguments:
        df: Dataframe to aggregate.
        by: Columns names to use to group rows (see :meth:`pd.DataFrame.groupby`).
        aggfuncs: Aggregation functions for columns not in `by`.
        errors: Handling method for errors raised by `aggfuncs`.

            - 'raise': Stop at the first error.
            - 'coerce': Silently replace errors with `np.nan`.
            - 'report': Replace errors with `np.nan` and return an error report.

        errorfunc: A function with signature `f(x, e) -> Tuple[Any, Any]`,
            where `x` is the original input and `e` is the original error,
            used when `errors`='report'.
            The first and second value of the returned tuple are used as the
            index and values, respectively,
            of the :class:`pd.Series` returned for each column.
            By default, the first value is `x.name`
            (the values of columns `by` for that row group),
            and the second is the original error.

    Returns:
        Aggregated dataframe with `by` columns set as the index and
        an error report with (if `errors`='report')
        a :class:`pd.Series` for each column where errors occured.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'x': [0, 0, 1, 1], 'y': pd.Series([2, 2, 2, 3], dtype='Int64')})
        >>> df.index = [0, 0, 0, 1]
        >>> base = dict(df=df, by='x', aggfuncs={'y': unique})
        >>> groupby_apply(**base, errors='raise')
        Traceback (most recent call last):
          ...
        AggregationError: Could not aggregate y at x = 1: Not unique.
        >>> groupby_apply(**base, errors='coerce')[0]
              y
        x
        0     2
        1  <NA>
        >>> _, report = groupby_apply(**base, errors='report')
        >>> report['y']
        x
        1    Not unique.
        dtype: object
        >>> errorfunc = lambda x, e: (x.name, as_dict(x))
        >>> _, report = groupby_apply(**base, errors='report', errorfunc=errorfunc)
        >>> report['y']
        x
        1    {0: [2], 1: [3]}
        dtype: object
    """
    groupby = df.groupby(by)
    data_columns = [col for col in df.columns if col not in by]
    series = {}
    reports = {}
    for col in data_columns:
        report = []
        aggfunc = aggfuncs[col]
        if errors == "raise":
            msg = f"Could not aggregate {col} at {by} = {{x.name}}: {{e}}"
            wrapper = try_aggfunc(aggfunc, method="raise", error=msg)
        elif errors == "coerce":
            wrapper = try_aggfunc(aggfunc, method="coerce")
        elif errors == "report":
            if errorfunc is None:

                def errorfunc(x, e):
                    return x.name, str(e)

            def wrapper(x):
                try:
                    return aggfunc(x)
                except AggregationError as e:
                    report.append(errorfunc(x, e))
                    return np.nan

        ds = groupby[col].apply(wrapper)
        if str(ds.dtype) != str(df[col].dtype):
            # Undo type changes triggered by nulls
            ds = ds.astype(df[col].dtype)
        if report:
            report = pd.Series(dict(report))
            keys = ds.index.names
            if report.index.nlevels == len(keys):
                report.index.names = keys
            reports[col] = report
        series[col] = ds
    return pd.DataFrame(series), reports


def groupby_aggregate(  # noqa: C901
    df: pd.DataFrame,
    by: Iterable,
    aggfuncs: Dict[Any, Callable],
    errors: Literal["raise", "coerce", "report"] = "raise",
    errorfunc: Callable = None,
) -> Tuple[pd.DataFrame, Dict[Any, pd.Series]]:
    """
    Aggregate dataframe and capture errors (using aggregate).

    Although faster than :func:`groupby_apply`, it has some limitations:

    - Raised errors cannot access the group index.
    - Aggregation functions must return a scalar (must 'reduce').
      This is not a limitation with :meth:`pd.Series.apply`.

    Args:
        df: Dataframe to aggregate.
        by: Columns names to use to group rows (see :meth:`pd.DataFrame.groupby`).
        aggfuncs: Aggregation functions for columns not in `by`.
        errors: Error handling method.

            - 'raise': Stop at the first error.
            - 'coerce': Silently replace errors with `np.nan`.
            - 'report': Replace errors with `np.nan` and return an error report.

        errorfunc: A function with signature `f(x, e) -> Any`,
            where `x` is the original input and `e` is the original error,
            used when `errors='report'`.
            By default, the original error is returned.

    Returns:
        Aggregated dataframe with `by` columns set as the index and
        an error report with (if `errors`='report') a :class:`pd.Series` of errors
        (or the value returned by `errorfunc`) for each column where errors occured.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     'x': [0, 0, 1, 1],
        ...     'y': pd.Series([2, 2, 2, 3], dtype='Int64')
        ... })
        >>> df.index = [0, 0, 0, 1]
        >>> base = dict(df=df, by='x', aggfuncs={'y': unique})
        >>> groupby_aggregate(**base, errors='raise')
        Traceback (most recent call last):
          ...
        AggregationError: Could not aggregate y: Not unique.
        >>> groupby_aggregate(**base, errors='coerce')[0]
              y
        x
        0     2
        1  <NA>
        >>> result, report = groupby_aggregate(**base, errors='report')
        >>> result
              y
        x
        0     2
        1  <NA>
        >>> report['y']
        x
        1    Not unique.
        Name: y, dtype: object
        >>> errorfunc = lambda x, e: as_dict(x)
        >>> result, report = groupby_aggregate(**base, errors='report', errorfunc=errorfunc)
        >>> report['y']
        x
        1    {0: [2], 1: [3]}
        Name: y, dtype: object
    """
    data_columns = [col for col in df.columns if col not in by]
    dtypes = {col: df[col].dtype for col in data_columns}
    reports = {}
    if errors == "report":
        # Prepare data columns for error objects returned by their aggregation function
        df = df.astype({col: object for col in data_columns})
    aggfuncs = {
        col: try_aggfunc(
            func,
            method=errors,
            error=f"Could not aggregate {col}: {{e}}" if errors == "raise" else errorfunc
        )
        for col, func in aggfuncs.items()
    }
    if data_columns:
        result = df.groupby(by).aggregate(aggfuncs)
    else:
        # groupby.aggregate drops index when aggfuncs is empty
        result = df[by].drop_duplicates().set_index(by)
    if errors == "report":
        # Move errors to report and replace errors with nulls
        is_error = result.applymap(lambda x: isinstance(x, AggregationError))
        for col in data_columns:
            report = result[col][is_error[col]]
            if not report.empty:
                reports[col] = report
        result = result.where(~is_error)
    # Enforce original data types, which nulls and errors may have changed
    result = result.astype(dtypes, copy=False)
    return result, reports
