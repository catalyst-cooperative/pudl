"""Harvest and normalize source data following a predefined schema."""
from typing import (Any, Callable, Dict, Iterable, List, Literal, Optional,
                    Set, Tuple, Union)

import numpy as np
import pandas as pd

# ---- Sampling ----


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


def find_sample(
    df: pd.DataFrame, key: Iterable[str], data: Iterable[str] = None
) -> Tuple[List[str], List[str]]:
    """
    Find dataframe key and data columns matching a query.

    Arguments:
        df: Dataframe to sample.
        key: Names of primary-key columns.
            Takes precedence over `data` if a name appears in both.
        data: Names of data columns.

    Returns:
        Names of the matching key and data columns in `df`.
        An exact name match is required,
        except periodic `key` columns which match a base name with a smaller period.

    Examples:
        >>> df = pd.DataFrame({'id': [0], 'report_day': ['2020-12-31']})
        >>> find_sample(df, key=['id'], data=['report_day'])
        (['id'], ['report_day'])
        >>> find_sample(df, key=['id', 'report_month'])
        (['id', 'report_day'], [])
        >>> find_sample(df, key=['id', 'report_month'], data=['report_day'])
        (['id', 'report_day'], [])
    """
    key = list(key)
    if data is None:
        data = []
    data = [col for col in data if col not in key]
    df_key = []
    for k in key:
        for name in expand_periodic_column_names([k]):
            if name in df.columns:
                df_key.append(name)
    df_data = [col for col in data if col in df.columns and col not in df_key]
    return df_key, df_data


def sample(
    df: pd.DataFrame, key: Iterable[str], data: Iterable[str] = None
) -> Optional[pd.DataFrame]:
    """
    Sample a dataframe.

    Arguments:
        df: Dataframe to sample.
        key: Names of primary-key columns.
            Takes precedence over `columns` if a name appears in both.
        data: Names of data columns.

    Returns:
        A dataframe with all `key` columns and any matching `data` columns from `df`.
        Values of periodic `key` columns are snapped to the start of the desired period,
        and cast to :class:`np.datetime64`.
        If not all `key` matched columns in `df`, nothing is returned.

    Examples:
        >>> df = pd.DataFrame({'id': [0], 'report_day': ['2020-12-31']})
        >>> sample(df, key=['id'], data=['report_day'])
           id  report_day
        0   0  2020-12-31
        >>> sample(df, key=['id', 'report_month'])
           id report_month
        0   0   2020-12-01
        >>> sample(df, key=['id', 'report_quarter'])
           id report_quarter
        0   0     2020-10-01
        >>> sample(df, key=['id', 'report_date']) is None
        True

        Periodic key columns are cast to :class:`np.datetime64`,
        whether or not the period needs to be changed.

        >>> sample(df, key=['report_day'])['report_day'].dtype.name
        'datetime64[ns]'
        >>> sample(df, key=['report_month'])['report_month'].dtype.name
        'datetime64[ns]'
    """
    df_key, df_data = find_sample(df=df, key=key, data=data)
    if len(df_key) < len(key):
        #
        return None
    mapper = {old: new for old, new in zip(df_key, key) if new != old}
    sdf = df[df_key + df_data].rename(columns=mapper)
    for k, df_k in zip(key, df_key):
        _, period = split_period(k)
        if period:
            sdf[k] = PERIODS[period](sdf[k])
    return sdf


# ---- Column aggregation ---- #

"""
Aggregation functions.

All take a :class:`pd.Series` as input (and any optional keyword arguments).
They may either return a single value (ideally of the same data type as the input),
null (`np.nan`),
or raise a :class:`ValueError` if the input does not meet requirements.
"""


def most_frequent(x: pd.Series) -> Any:
    """Return most frequent value (or error if none exists)."""
    mode = x.mode(dropna=True)
    if mode.size == 1:
        return mode[0]
    if mode.empty:
        return np.nan
    raise ValueError("No value is most frequent.")


def unique(x: pd.Series) -> Any:
    """Return single unique value (or error if none exists)."""
    x = x.dropna()
    if x.empty:
        return np.nan
    uniques = x.unique()
    if uniques.size == 1:
        return uniques[0]
    raise ValueError("Not unique.")


def as_dict(x: pd.Series) -> Dict[Any, list]:
    """Return dictionary of values, listed by index."""
    result = {}
    for key, xi in x.dropna().groupby(x.index):
        result[key] = list(xi)
    return result


def try_aggfunc(  # noqa: C901
    func: Callable,
    method: Literal["coerce", "raise", "return", "insert", "append"] = "coerce",
    error: Union[str, Callable] = None,
) -> Callable:
    """
    Wrap aggregate function in a try-except for error handling.

    Arguments:
        func: Aggregate function.
        method: Error handling method (for :class:`ValueError` only).

            - 'coerce': Return `np.nan` instead of the error.
            - 'raise': Re-raise the error.
            - 'return' and 'insert': Return rather than raise the error.
            - 'append': Append the error to a list passed as a second function argument.

        error: Error value, whose type and format depends on `method`.
            Below, `x` is the original input and `e` is the original error.

            - 'raise': A string with substitions (e.g. 'Error at {x.name}: {e}')
              By default, the original error is raised.
            - 'return': A function with signature `f(x, e)` returning a value to return.
              By default, the original error is returned.
            - 'insert': Same as for 'return', except the returned value replaces the
              arguments of the original error and the original error is returned.
            - 'append': Same as for 'return', except the returned value is appended.

    Returns:
        Aggregate function with custom error handling.

    Examples:
        >>> import pandas as pd
        >>> x = pd.Series([0, 0, 1, 1], index=['a', 'a', 'a', 'b'])
        >>> most_frequent(x)
        Traceback (most recent call last):
          ...
        ValueError: No value is most frequent.
        >>> try_aggfunc(most_frequent, 'coerce')(x)
        nan
        >>> try_aggfunc(most_frequent, 'return')(x)
        ValueError('No value is most frequent.')
        >>> try_aggfunc(most_frequent, 'raise', 'Bad dtype {x.dtype}')(x)
        Traceback (most recent call last):
          ...
        ValueError: Bad dtype int64
        >>> error = lambda x, e: as_dict(x)
        >>> try_aggfunc(most_frequent, 'return', error)(x)
        {'a': [0, 0, 1], 'b': [1]}
        >>> try_aggfunc(most_frequent, 'insert', error)(x)
        ValueError({'a': [0, 0, 1], 'b': [1]})
        >>> errors = []
        >>> try_aggfunc(most_frequent, 'append', error)(x, errors)
        nan
        >>> errors
        [{'a': [0, 0, 1], 'b': [1]}]
    """
    # Conditional statements outside function for execution speed.
    wrapped = func
    if method == "coerce":

        def wrapped(x):
            try:
                return func(x)
            except ValueError:
                return np.nan

    elif method == "raise":
        if error is None:

            def wrapped(x):
                try:
                    return func(x)
                except ValueError as e:
                    raise e

        elif method == "raise":

            def wrapped(x):
                try:
                    return func(x)
                except ValueError as e:
                    raise ValueError(error.format(x=x, e=e))  # noqa: FS002

    elif method == "insert":
        if error is None:

            def wrapped(x):
                try:
                    return func(x)
                except ValueError as e:
                    return e

        else:

            def wrapped(x):
                try:
                    return func(x)
                except ValueError as e:
                    e.args = (error(x, e),)
                    return e

    elif method == "return":
        if error is None:

            def wrapped(x):
                try:
                    return func(x)
                except ValueError as e:
                    return e

        else:

            def wrapped(x):
                try:
                    return func(x)
                except ValueError as e:
                    return error(x, e)

    elif method == "append":
        if error is None:

            def wrapped(x, errors):
                try:
                    return func(x)
                except ValueError as e:
                    errors.append(e)
                    return np.nan

        else:

            def wrapped(x, errors):
                try:
                    return func(x)
                except ValueError as e:
                    errors.append(error(x, e))
                    return np.nan

    return wrapped


# ---- Dataframe aggregation ---- #


def groupby_apply(  # noqa: C901
    df: pd.DataFrame,
    by: Iterable,
    aggfuncs: Dict[Any, Callable],
    errors: Literal["raise", "coerce", "report"] = "raise",
    errorfunc: Callable = None,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[Any, pd.Series]]]:
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
        (if `errors`='report') an error report with a :class:`pd.Series`
        for each column where errors occured.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'x': [0, 0, 1, 1], 'y': pd.Series([2, 2, 2, 3], dtype='Int64')})
        >>> df.index = [0, 0, 0, 1]
        >>> base = dict(df=df, by='x', aggfuncs={'y': unique})
        >>> groupby_apply(**base, errors='raise')
        Traceback (most recent call last):
          ...
        ValueError: Could not aggregate y at x = 1: Not unique.
        >>> groupby_apply(**base, errors='coerce')
              y
        x ...
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

            # NOTE: Appending to list in scope ~10% faster than passing list as argument
            # aggfunc = try_aggfunc(aggfunc, method='append', error=errorfunc)
            def wrapper(x):
                try:
                    return aggfunc(x)
                except ValueError as e:
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
    if errors == "report":
        return pd.DataFrame(series), reports
    return pd.DataFrame(series)


def groupby_aggregate(  # noqa: C901
    df: pd.DataFrame,
    by: Iterable,
    aggfuncs: Dict[Any, Callable],
    errors: Literal["raise", "coerce", "report"] = "raise",
    errorfunc: Callable = None,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[Any, pd.Series]]]:
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
        (if `errors`='report') an error report with a :class:`pd.Series` of errors
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
        ValueError: Could not aggregate y: Not unique.
        >>> groupby_aggregate(**base, errors='coerce')
              y
        x ...
        0     2
        1  <NA>
        >>> result, report = groupby_aggregate(**base, errors='report')
        >>> result
              y
        x ...
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
    if errors == "report":
        reports = {}
        # Prepare columns for error objects
        df = df.astype({col: object for col in data_columns})
    if errors == "raise":
        aggfuncs = {
            col: try_aggfunc(
                func, method="raise", error=f"Could not aggregate {col}: {{e}}"
            )
            for col, func in aggfuncs.items()
        }
    elif errors == "coerce":
        aggfuncs = {
            col: try_aggfunc(func, method="coerce") for col, func in aggfuncs.items()
        }
    elif errors == "report":
        aggfuncs = {
            col: try_aggfunc(func, method="insert", error=errorfunc)
            for col, func in aggfuncs.items()
        }
    if data_columns:
        result = df.groupby(by).aggregate(aggfuncs)
    else:
        # groupby.aggregate drops index when aggfuncs is empty
        result = df[by].drop_duplicates().set_index(by)
    for col in data_columns:
        ds = result[col]
        if errors == "report":
            is_error = ds.apply(isinstance, args=(ValueError,))
            report = ds[is_error]
            if not report.empty:
                reports[col] = report
                ds = ds.where(~is_error)
        # Enforce original data types, which nulls and errors may have changed
        if str(dtypes[col]) != str(ds.dtype):
            ds = ds.astype(dtypes[col])
        result[col] = ds
    if errors == "report":
        return result, reports
    return result


# ---- Class definition ---- #


class ResourceBuilder:
    """
    Build resources from dataframes.

    Each resource is described by a dictionary modeled after Frictionless Data's
    [Tabular Data Resource](https://specs.frictionlessdata.io/tabular-data-resource).
    The following attributes are used:

    - `name` (`str`): Name. Used to label output.
    - `harvest` (`bool`, optional, default: `False`): Whether to harvest from dataframes
      based on field names. If `False`, the dataframe with the same name is used and
      the process is limited to dropping unwanted fields.
    - `tolerance` (`float`, optional, default: `0.0`): Fraction of invalid fields below
      which the harvest result is considered valid.
    - `schema`:
        - `fields` (`List[dict]`): Field descriptors (see below).
        - `primaryKey` (`List[str]`): Primary key field names.
          Determines which dataframes to harvest from and how to aggregate rows.

    Each resource field is described by a dictionary modeled after Frictionless Data's
    [field descriptors](https://specs.frictionlessdata.io/table-schema/#field-descriptors).
    The following attributes are used:

    - `name` (`str`): Name. Matched to dataframe column names and used to label output.
    - `aggregate` (`Callable[[pd.Series], Any]`): Function that computes a single value
      from multiple field values. Required for fields not only used as primary keys.
    - `tolerance` (`float`, optional, default: `0`): Fraction of invalid groups below
      which harvest result is considered valid.

    Attributes:
        dfs (Dict[Any, pd.DataFrame]): Input dataframes.
            Dataframes do not contain duplicate column basenames
            (e.g. 'report_year', 'report_month').
        errors (Dict[str, Dict[str, pd.Series]]): Error log from the last :meth:`build`
            with `errors='log'`.
            The error log includes, for each resource with aggregation errors,
            a :class:`pd.Series` of error values indexed by primary key fields,
            for each field where errors occured.

    Examples:
        >>> import pandas as pd
        >>> fields = [
        ...     {'name': 'id'},
        ...     {'name': 'x', 'aggregate': unique, 'tolerance': 0.25}
        ... ]
        >>> resources = [{
        ...     'name': 'A',
        ...     'harvest': True,
        ...     'schema': {'fields': fields, 'primaryKey': ['id']}
        ... }]
        >>> dfs = {
        ...     'A': pd.DataFrame({
        ...         'id': [1, 1, 2, 2],
        ...         'x': pd.Series([1, 1, 2, 2], dtype='Int64')
        ...     }),
        ...     'B': pd.DataFrame({
        ...         'id': [2, 3, 3],
        ...         'x': pd.Series([3, 4, 4], dtype='Int64')
        ...     })
        ... }
        >>> builder = ResourceBuilder(dfs)

        Skip aggregation to access all the rows concatenated from the input dataframes.
        The names of the input dataframes are used as the index.

        >>> build = builder.build(resources, errors='coerce', aggregate=False)
        >>> build['A']['x']
        df
        A    1
        A    1
        A    2
        A    2
        B    3
        B    4
        B    4
        Name: x, dtype: Int64

        Aggregate and coerce any aggregation errors to null
        (by default, aggregation stops at and raises the first error).
        The primary key fields are used as the index.

        >>> build = builder.build(resources, errors='coerce')
        >>> build['A']['x']
        id
        1       1
        2    <NA>
        3       4
        Name: x, dtype: Int64
        >>> builder.errors is None
        True

        Aggregate, log errors, and generate an error report.

        >>> build = builder.build(resources, errors='log')
        >>> build['A']['x']
        id
        1       1
        2    <NA>
        3       4
        Name: x, dtype: Int64
        >>> builder.errors['A']['x']
        id
        2    Not unique.
        Name: x, dtype: object
        >>> report = builder.report(build, resources)
        >>> report['valid']
        False
        >>> report['resources']['A']['fields']['x']['stats']
        {'all': 3, 'invalid': 1, 'tolerance': 0.25, 'actual': 0.333...}
        >>> report['resources']['A']['fields']['x']['errors']
        id
        2    Not unique.
        Name: x, dtype: object

        Customize the error values in the error report.

        >>> errorfunc = lambda x, e: as_dict(x)
        >>> build = builder.build(resources, errors='log', errorfunc=errorfunc)
        >>> report = builder.report(build, resources)
        >>> report['resources']['A']['fields']['x']['errors']
        id
        2    {'A': [2, 2], 'B': [3]}
        Name: x, dtype: object

        Skip harvesting by turning off the behavior in the resource metadata.

        >>> resources[0]['harvest'] = False
        >>> build = builder.build(resources)
        >>> build['A']['x']
        id
        1    1
        1    1
        2    2
        2    2
        Name: x, dtype: Int64
    """

    def __init__(
        self, dfs: Union[Dict[Any, pd.DataFrame], Iterable[pd.DataFrame]]
    ) -> None:
        """
        Initialize a new ResourceBuilder.

        Args:
            dfs: Either a dictionary or iterable of dataframes.
                If an iterable, the integer index is used as dictionary keys.
                Dataframes may not contain duplicate column basenames
                (e.g. 'report_year', 'report_month').

        Raises:
            ValueError: Dataframe has duplicate column basenames.
        """
        if not isinstance(dfs, dict):
            # Use index as integer keys
            dfs = {i: df for i, df in enumerate(dfs)}
        self.dfs = dfs
        for key in self.dfs:
            if has_duplicate_basenames(self.dfs[key].columns):
                raise ValueError(f"Dataframe {key} has duplicate column basenames")
        self.errors = None

    @property
    def columns(self) -> Set[str]:
        """Column names of all tables."""
        return set([name for df in self.dfs.values() for name in df.columns])

    def prune(self, resources: Iterable[dict]) -> List[dict]:
        """
        Return only the buildable resources.

        Standard resources (`harvest: False`, default) are pruned if there is no input
        dataframe with the same name.
        Harvested resources (`harvest: True`) are pruned if none of the primary-key
        fields, or data fields (if any), match columns in the input dataframes. Periodic
        key fields (e.g. 'report_month') are matched to any column of the same name with
        an equal or smaller period (e.g. 'report_day').

        Args:
            resources: Resources with required attributes
                `name`, `schema`.`fields`, and `schema`.`primaryKey`.

        Returns:
            The subset of `resources` which can theoretically be built.
        """
        pruned = []
        for resource in resources:
            if resource.get("harvest"):
                columns = set([field["name"] for field in resource["schema"]["fields"]])
                key_columns = set(resource["schema"]["primaryKey"])
                data_columns = columns - key_columns
                # Add alternative keys with smaller periods
                key_columns = set(expand_periodic_column_names(key_columns))
                # Keep if 1+ key columns in input, and 1+ data columns (if any) in input
                if (self.columns & key_columns) and (
                    not data_columns or (self.columns & data_columns)
                ):
                    pruned.append(resource)
            elif resource["name"] in self.dfs:
                # Keep standard tables with input tables of the same name
                pruned.append(resource)
        return pruned

    def test(self, resources: Iterable[dict]) -> None:
        """
        Test whether the resources can be built.

        For standard resources (`harvest: False`, default), tests that each field is in
        the input dataframe of the same name.
        For harvested resources (`harvest: True`), tests that all key fields are
        together in one more input dataframes, and that each data field is in one of
        those input dataframes. Periodic key fields (e.g. 'report_month') are matched to
        any column of the same name with an equal or smaller period (e.g. 'report_day').

        Args:
            resources: Resources with required attributes
                `name`, `schema`.`fields` with `name`, and `schema`.`primaryKey`.

        Raises:
            ValueError: Resource has duplicate field basenames.
            KeyError: Missing columns needed by resource.
        """
        for resource in resources:
            columns = set([field["name"] for field in resource["schema"]["fields"]])
            if has_duplicate_basenames(columns):
                raise ValueError(
                    f"Resource {resource['name']} has duplicate field basenames"
                )
            if resource.get("harvest"):
                # All resource fields must be in dataframes with matching primary key
                key = resource["schema"]["primaryKey"]
                for df in self.dfs.values():
                    df_key, df_data = find_sample(df, key=key, data=columns)
                    if len(df_key) == len(key):
                        columns -= set(key) | set(df_data)
            else:
                # All resource fields must be in dataframe of same name
                columns -= set(self.dfs[resource["name"]].columns)
            if columns:
                raise KeyError(
                    f'Missing columns needed by resource {resource["name"]}: {columns}'
                )

    def build(
        self,
        resources: Iterable[dict],
        aggregate: bool = True,
        errors: Literal["raise", "coerce", "log"] = "raise",
        errorfunc: Callable = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Build resources.

        Prunes and test resources before building (see :meth:`prune` and :meth:`test`).

        For standard resources (`harvest: False`, default), the columns matching all key
        fields and any data fields are extracted from the input dataframe of the same
        name.

        For harvested resources (`harvest: True`), the columns matching all key fields
        and any data fields are extracted from each compatible input dataframe,
        concatenated into a single dataframe, grouped by key fields, and aggregated
        using the aggregate function for each data field (`schema.fields[*].aggregate`).
        Periodic key fields (e.g. 'report_month') are matched to any column of the same
        name with an equal or smaller period (e.g. 'report_day') and snapped to the
        start of the desired period before aggregation.

        Args:
            resources: Resources with required attributes `name`,
                `schema`.`fields` with `name` and `aggregate` (if `aggregate`),
                and `schema`.`primaryKey`.
            aggregate: Whether to aggregate harvested resources
                (attribute `harvest: True`) by their primary key (default).
            errors: Error handling method for aggregation errors.

                - 'raise': Stop at the first error.
                - 'coerce': Silently replace errors with `null` values.
                - 'log': Silently replace errors with `null` and
                    log the errors in :attr:`errors`.

            errorfunc: A function with signature `f(x, e) -> Any`,
                where `x` are the original column values as a :class:`pd.Series` indexed
                by the source input dataframe names, and `e` is the original error.
                When `errors='log'`, the returned value is logged instead of the
                original error.

        Returns:
            A dictionary of dataframes, harvested and aggregated (if `aggregate`)
            from the input dataframes, matching the name and schema of the resources.
            The index are resource key fields if `aggregate`=`True`,
            and the name of the input dataframe otherwise.

        Raises:
            ValueError: Resource has duplicate field basenames.
            KeyError: Missing columns needed by resource.
        """
        resources = self.prune(resources)
        self.test(resources)
        odfs = {}
        reports = {}
        for resource in resources:
            rname = resource["name"]
            harvest = resource.get("harvest", False)
            columns = [field["name"] for field in resource["schema"]["fields"]]
            key = resource["schema"]["primaryKey"]
            samples = {}
            if harvest:
                for name, df in self.dfs.items():
                    sdf = sample(df, key=key, data=columns)
                    if sdf is not None:
                        samples[name] = sdf
            else:
                samples[rname] = self.dfs[rname][columns]
            # Pass source dataframe names to aggregate via the index
            for name, sdf in samples.items():
                samples[name] = sdf.set_index(pd.Index([name] * len(sdf), name="df"))
            odf = pd.concat(samples.values())[columns]
            if aggregate:
                if harvest:
                    aggfuncs = {
                        field["name"]: field["aggregate"]
                        for field in resource["schema"]["fields"]
                        if field["name"] not in key
                    }
                    if errors == "log":
                        odf, report = groupby_aggregate(
                            odf,
                            by=key,
                            aggfuncs=aggfuncs,
                            errors="report",
                            errorfunc=errorfunc,
                        )
                        if report:
                            reports[rname] = report
                    else:
                        odf = groupby_aggregate(
                            odf,
                            by=key,
                            aggfuncs=aggfuncs,
                            errors=errors,
                            errorfunc=errorfunc,
                        )
                else:
                    # Set index to primary key to match harvest output
                    odf = odf.set_index(key)
            odfs[rname] = odf
        self.errors = reports if errors == "log" else None
        return odfs

    def report(
        self, build: Dict[str, pd.DataFrame], resources: Iterable[dict] = None
    ) -> dict:
        """
        Report errors.

        The errors logged for the last :meth:`build` (:attr:`errors`) are formatted into
        the following report:

        - valid (bool): Whether resources are valid.
        - stats (dict): Error statistics for resources.
        - resources (dict):
            - <resource_name>
                - valid (bool): Whether resouce is valid.
                - stats (dict): Error statistics for resource fields.
                - fields (dict):
                    - <field_name>
                        - valid (bool): Whether field is valid.
                        - stats (dict): Error statistics for field groups.
                        - errors (pd.Series): Error values indexed by primary key.

        where each 'stats' contains the following:

        - stats (dict):
            - all (int): Number of entities (resource, field, or field group).
            - invalid (int): Invalid number of entities.
            - tolerance (float): Fraction of invalid entities below which parent entity
              is considered valid.
            - actual (float): Actual fraction of invalid entities.

        Args:
            build: Result of :meth:`build`.
            resources: Resources with attributes `name` and `schema.fields[*].name`.
                Used for non-default tolerances (default is 0.0) for resources
                (`tolerance`) and resource fields (`schema.fields[*].tolerance`).

        Returns:
            Error report as described above.

        Raises:
            ValueError: No errors were logged by the last build.
        """
        if self.errors is None:
            raise ValueError("No errors were logged by the last build.")
        rreports = {}
        if resources:
            rnames = [resource["name"] for resource in resources]
        for rname in self.errors:
            nrows, ncols = build[rname].shape
            freports = {}
            if resources:
                rindex = rnames.index(rname)
                fnames = [
                    field["name"] for field in resources[rindex]["schema"]["fields"]
                ]
            for fname, errors in self.errors[rname].items():
                ftolerance = 0.0
                if resources:
                    findex = fnames.index(fname)
                    ftolerance = resources[rindex]["schema"]["fields"][findex].get(
                        "tolerance", 0.0
                    )
                nerrors = len(errors)
                stats = {
                    "all": nrows,
                    "invalid": nerrors,
                    "tolerance": ftolerance,
                    "actual": nerrors / nrows,
                }
                freports[fname] = {
                    "valid": stats["actual"] < stats["tolerance"],
                    "stats": stats,
                    "errors": errors,
                }
            rtolerance = 0.0
            if resources:
                rtolerance = resources[rindex].get("tolerance", 0.0)
            nerrors = sum([not f["valid"] for f in freports.values()])
            stats = {
                "all": ncols,
                "invalid": nerrors,
                "tolerance": rtolerance,
                "actual": nerrors / ncols,
            }
            rreports[rname] = {
                "valid": stats["actual"] < stats["tolerance"],
                "stats": stats,
                "fields": freports,
            }
        nresources = len(build)
        nerrors = sum([not r["valid"] for r in rreports.values()])
        return {
            "valid": nerrors == 0,
            "stats": {
                "all": nresources,
                "invalid": nerrors,
                "tolerance": 0.0,
                "actual": nerrors / nresources,
            },
            "resources": rreports,
        }


# ---- Metadata ---- #


def expand_resource_fields(
    resources: List[dict], fields: List[dict] = None, default: dict = None
) -> None:
    """
    Replace resource field names with field descriptors.

    Args:
        resources: Resources with a list of field names in attribute `schema.fields`.
        fields: Field descriptors with attribute `name`.
        default: Default field attribute values.

    Examples:
        >>> resources = [{'name': 'r', 'schema': {'fields': ['x', 'y']}}]
        >>> fields = [{'name': 'x', 'aggregate': unique}]
        >>> default = {'aggregate': most_frequent}
        >>> expand_resource_fields(resources, fields=fields, default=default)
        >>> resources[0]['schema']['fields'][0]['aggregate'] is unique
        True
        >>> resources[0]['schema']['fields'][1]['aggregate'] is most_frequent
        True
    """
    if fields is None:
        fields = []
    if default is None:
        default = {}
    names = [field["name"] for field in fields]
    for resource in resources:
        for i, value in enumerate(resource["schema"]["fields"]):
            if isinstance(value, dict):
                continue
            if value in names:
                resource["schema"]["fields"][i] = {
                    **default,
                    **fields[names.index(value)],
                }
            else:
                resource["schema"]["fields"][i] = {**default, "name": value}
