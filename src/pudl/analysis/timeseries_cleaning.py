"""
Screen timeseries for anomalies and impute missing and anomalous values.

The screening methods were originally designed to identify unrealistic data in the
electricity demand timeseries reported to EIA on Form 930, and have also been
applied to the FERC Form 714, and various historical demand timeseries
published by regional grid operators like MISO, PJM, ERCOT, and SPP.

They are adapted from code published and modified by:

* Tyler Ruggles <truggles@carnegiescience.edu>
* Greg Schivley <greg@carbonimpact.co>

And described at:

* https://doi.org/10.1038/s41597-020-0483-x
* https://zenodo.org/record/3737085
* https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code

The imputation methods were designed for multivariate time series forecasting.

They are adapted from code published by:

* Xinyu Chen <chenxy346@gmail.com>

And described at:

* https://arxiv.org/abs/2006.10436
* https://arxiv.org/abs/2008.03194
* https://github.com/xinychen/tensor-learning

"""

import functools
import warnings
from typing import Any, Iterable, List, Sequence, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats

# ---- Helpers ---- #


def slice_axis(
    x: np.ndarray, start: int = None, end: int = None, step: int = None, axis: int = 0
) -> Tuple[slice, ...]:
    """
    Return an index that slices an array along an axis.

    Args:
        x: Array to slice.
        start: Start index of slice.
        end: End index of slice.
        step: Step size of slice.
        axis: Axis along which to slice.

    Returns:
        Tuple of :class:`slice` that slices array `x` along axis `axis`
        (`x[..., start:stop:step]`).

    Examples:
        >>> x = np.random.random((3, 4, 5))
        >>> np.all(x[1:] == x[slice_axis(x, start=1, axis=0)])
        True
        >>> np.all(x[:, 1:] == x[slice_axis(x, start=1, axis=1)])
        True
        >>> np.all(x[:, :, 1:] == x[slice_axis(x, start=1, axis=2)])
        True
    """
    index = [slice(None)] * np.mod(axis, x.ndim) + [slice(start, end, step)]
    return tuple(index)


def array_diff(
    x: np.ndarray,
    periods: int = 1,
    axis: int = 0,
    fill: Any = np.nan
) -> np.ndarray:
    """
    First discrete difference of array elements.

    This is a fast numpy implementation of :meth:`pd.DataFrame.diff`.

    Args:
        periods: Periods to shift for calculating difference, accepts negative values.
        axis: Array axis along which to calculate the difference.
        fill: Value to use at the margins where a difference cannot be calculated.

    Returns:
        Array of same shape and type as `x` with discrete element differences.

    Examples:
        >>> x = np.random.random((4, 2))
        >>> np.all(array_diff(x, 1)[1:] == pd.DataFrame(x).diff(1).values[1:])
        True
        >>> np.all(array_diff(x, 2)[2:] == pd.DataFrame(x).diff(2).values[2:])
        True
        >>> np.all(array_diff(x, -1)[:-1] == pd.DataFrame(x).diff(-1).values[:-1])
        True
    """
    if not periods:
        return x - x
    dx = np.empty_like(x)
    prepend = slice_axis(x, end=periods, axis=axis)
    append = slice_axis(x, start=periods, axis=axis)
    if periods > 0:
        dx[prepend] = fill
        dx[append] = x[append] - x[slice_axis(x, end=-periods, axis=axis)]
    else:
        dx[prepend] = x[prepend] - x[slice_axis(x, start=-periods, axis=axis)]
        dx[append] = fill
    return dx


def encode_run_length(
    x: Union[Sequence, np.ndarray]
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Encode vector with run-length encoding.

    Args:
        x: Vector to encode.

    Returns:
        Values and their run lengths.

    Examples:
        >>> x = np.array([0, 1, 1, 0, 1])
        >>> encode_run_length(x)
        (array([0, 1, 0, 1]), array([1, 2, 1, 1]))
        >>> encode_run_length(x.astype('bool'))
        (array([False,  True, False,  True]), array([1, 2, 1, 1]))
        >>> encode_run_length(x.astype('<U1'))
        (array(['0', '1', '0', '1'], dtype='<U1'), array([1, 2, 1, 1]))
        >>> encode_run_length(np.where(x == 0, np.nan, x))
        (array([nan,  1., nan,  1.]), array([1, 2, 1, 1]))
    """
    # Inspired by https://stackoverflow.com/a/32681075
    x = np.asarray(x)
    n = len(x)
    if not n:
        return x, np.array([], dtype=int)
    # Pairwise unequal (string safe)
    y = np.array(x[1:] != x[:-1])
    # Must include last element position
    i = np.append(np.where(y), n - 1)
    lengths = np.diff(np.append(-1, i))
    # starts = np.cumsum(np.append(0, lengths))[:-1]
    return x[i], lengths


def insert_run_length(  # noqa: C901
    x: Union[Sequence, np.ndarray],
    values: Union[Sequence, np.ndarray],
    lengths: Sequence[int],
    mask: Sequence[bool] = None,
    padding: int = 0,
    intersect: bool = False,
) -> np.ndarray:
    """
    Insert run-length encoded values into a vector.

    Args:
        x: Vector to insert values into.
        values: Values to insert.
        lengths: Length of run to insert for each value in `values`.
        mask: Boolean mask, of the same length as `x`, where values can be inserted.
            By default, values can be inserted anywhere in `x`.
        padding: Minimum space between inserted runs and,
            if `mask` is provided, the edges of masked-out areas.
        intersect: Whether to allow inserted runs to intersect each other.

    Raises:
        ValueError: Padding must zero or greater.
        ValueError: Run length must be greater than zero.
        ValueError: Cound not find space for run of length {length}.

    Returns:
        Copy of array `x` with values inserted.

    Example:
        >>> x = [0, 0, 0, 0]
        >>> mask = [True, False, True, True]
        >>> insert_run_length(x, values=[1, 2], lengths=[1, 2], mask=mask)
        array([1, 0, 2, 2])

        If we use unique values for the background and each inserted run,
        the run length encoding of the result (ignoring the background)
        is the same as the inserted run, albeit in a different order.

        >>> x = np.zeros(10, dtype=int)
        >>> values = [1, 2, 3]
        >>> lengths = [1, 2, 3]
        >>> x = insert_run_length(x, values=values, lengths=lengths)
        >>> rvalues, rlengths = encode_run_length(x[x != 0])
        >>> order = np.argsort(rvalues)
        >>> all(rvalues[order] == values) and all(rlengths[order] == lengths)
        True

        Null values can be inserted into a vector such that the new null runs
        match the run length encoding of the existing null runs.

        >>> x = [1, 2, np.nan, np.nan, 5, 6, 7, 8, np.nan]
        >>> is_nan = np.isnan(x)
        >>> rvalues, rlengths = encode_run_length(is_nan)
        >>> xi = insert_run_length(
        ...     x,
        ...     values=[np.nan] * rvalues.sum(),
        ...     lengths=rlengths[rvalues],
        ...     mask=~is_nan
        ... )
        >>> np.isnan(xi).sum() == 2 * is_nan.sum()
        True

        The same as above, with non-zero `padding`, yields a unique solution:

        >>> insert_run_length(
        ...     x,
        ...     values=[np.nan] * rvalues.sum(),
        ...     lengths=rlengths[rvalues],
        ...     mask=~is_nan,
        ...     padding=1
        ... )
        array([nan,  2., nan, nan,  5., nan, nan,  8., nan])
    """
    if padding < 0:
        raise ValueError("Padding must zero or greater")
    # Make a new array to modify in place
    x = np.array(x)
    # Compute runs available for insertions
    if mask is None:
        run_starts = np.array([0])
        run_lengths = np.array([len(x)])
    else:
        mask_values, mask_lengths = encode_run_length(mask)
        run_starts = np.cumsum(np.append(0, mask_lengths))[:-1][mask_values]
        run_lengths = mask_lengths[mask_values]
    if padding:
        # Constrict runs
        run_ends = run_starts + run_lengths
        # Move run starts forward, unless endpoint
        moved = slice(int(run_starts[0] == 0), None)
        run_starts[moved] += padding
        # Move run ends backward, unless endpoint
        moved = slice(None, -1 if run_ends[-1] == len(x) else None)
        run_ends[moved] -= padding
        # Recalculate run lengths and keep runs with positive length
        run_lengths = run_ends - run_starts
        keep = run_lengths > 0
        run_starts = run_starts[keep]
        run_lengths = run_lengths[keep]
    # Grow runs by maximum number of insertions (for speed)
    n_runs = len(run_starts)
    if not intersect:
        buffer = np.zeros(len(values), dtype=int)
        run_starts = np.concatenate((run_starts, buffer))
        run_lengths = np.concatenate((run_lengths, buffer))
    # Initialize random number generator
    rng = np.random.default_rng()
    # Sort insertions from longest to shortest
    order = np.argsort(lengths)[::-1]
    values = np.asarray(values)[order]
    lengths = np.asarray(lengths)[order]
    for value, length in zip(values, lengths):
        if length < 1:
            raise ValueError("Run length must be greater than zero")
        # Choose runs of adequate length
        choices = np.nonzero(run_lengths[:n_runs] >= length)[0]
        if not choices.size:
            raise ValueError(f"Could not find space for run of length {length}")
        idx = rng.choice(choices)
        # Choose adequate start position in run
        offset = rng.integers(0, run_lengths[idx] - length, endpoint=True)
        start = run_starts[idx] + offset
        # Insert value
        x[start:start + length] = value
        if intersect:
            continue
        # Update runs
        padded_length = length + padding
        if offset:
            tail = run_lengths[idx] - offset - padded_length
            if tail > 0:
                # Insert run
                run_starts[n_runs] = start + padded_length
                run_lengths[n_runs] = tail
                n_runs += 1
            # Shorten run
            run_lengths[idx] = offset - padding
        else:
            # Shift and shorten run
            run_starts[idx] += padded_length
            run_lengths[idx] -= padded_length
    return x


def _mat2ten(matrix: np.ndarray, shape: np.ndarray, mode: int) -> np.ndarray:
    """Fold matrix into a tensor."""
    index = [mode] + [i for i in range(len(shape)) if i != mode]
    return np.moveaxis(
        np.reshape(matrix, newshape=shape[index], order='F'),
        source=0,
        destination=mode
    )


def _ten2mat(tensor: np.ndarray, mode: int) -> np.ndarray:
    """Unfold tensor into a matrix."""
    return np.reshape(
        np.moveaxis(tensor, source=mode, destination=0),
        newshape=(tensor.shape[mode], -1),
        order='F'
    )


def _svt_tnn(matrix: np.ndarray, tau: float, theta: int) -> np.ndarray:
    """Singular value thresholding (SVT) truncated nuclear norm (TNN) minimization."""
    [m, n] = matrix.shape
    if 2 * m < n:
        u, s, v = np.linalg.svd(matrix @ matrix.T, full_matrices=0)
        s = np.sqrt(s)
        idx = np.sum(s > tau)
        mid = np.zeros(idx)
        mid[: theta] = 1
        mid[theta:idx] = (s[theta:idx] - tau) / s[theta:idx]
        return (u[:, :idx] @ np.diag(mid)) @ (u[:, :idx].T @ matrix)
    if m > 2 * n:
        return _svt_tnn(matrix.T, tau, theta).T
    u, s, v = np.linalg.svd(matrix, full_matrices=0)
    idx = np.sum(s > tau)
    vec = s[:idx].copy()
    vec[theta:idx] = s[theta:idx] - tau
    return u[:, :idx] @ np.diag(vec) @ v[:idx, :]


def impute_latc_tnn(
    tensor: np.ndarray,
    lags: Sequence[int] = [1],
    alpha: Sequence[float] = [1 / 3, 1 / 3, 1 / 3],
    rho0: float = 1e-7,
    lambda0: float = 2e-7,
    theta: int = 20,
    epsilon: float = 1e-7,
    maxiter: int = 300
) -> np.ndarray:
    """
    Impute tensor values with LATC-TNN method by Chen and Sun (2020).

    Uses low-rank autoregressive tensor completion (LATC) with
    truncated nuclear norm (TNN) minimization.

    * description: https://arxiv.org/abs/2006.10436
    * code: https://github.com/xinychen/tensor-learning/blob/master/mats

    Args:
        tensor: Observational series in the form (series, groups, periods).
            Null values are replaced with zeros, so any zeros will be treated as null.
        lags:
        alpha:
        rho0:
        lambda0:
        theta:
        epsilon: Convergence criterion. A smaller number will result in more iterations.
        maxiter: Maximum number of iterations.

    Returns:
        Tensor with missing values in `tensor` replaced by imputed values.
    """
    tensor = np.where(np.isnan(tensor), 0, tensor)
    dim = np.array(tensor.shape)
    dim_time = int(np.prod(dim) / dim[0])
    d = len(lags)
    max_lag = np.max(lags)
    mat = _ten2mat(tensor, mode=0)
    pos_missing = np.where(mat == 0)
    x = np.zeros(np.insert(dim, 0, len(dim)))
    t = np.zeros(np.insert(dim, 0, len(dim)))
    z = mat.copy()
    z[pos_missing] = np.mean(mat[mat != 0])
    a = 0.001 * np.random.rand(dim[0], d)
    it = 0
    ind = np.zeros((d, dim_time - max_lag), dtype=int)
    for i in range(d):
        ind[i, :] = np.arange(max_lag - lags[i], dim_time - lags[i])
    last_mat = mat.copy()
    snorm = np.linalg.norm(mat, 'fro')
    rho = rho0
    while True:
        rho = min(rho * 1.05, 1e5)
        for k in range(len(dim)):
            x[k] = _mat2ten(
                _svt_tnn(
                    _ten2mat(_mat2ten(z, shape=dim, mode=0) - t[k] / rho, mode=k),
                    tau=alpha[k] / rho,
                    theta=theta
                ),
                shape=dim,
                mode=k
            )
        tensor_hat = np.einsum('k, kmnt -> mnt', alpha, x)
        mat_hat = _ten2mat(tensor_hat, 0)
        mat0 = np.zeros((dim[0], dim_time - max_lag))
        if lambda0 > 0:
            for m in range(dim[0]):
                qm = mat_hat[m, ind].T
                a[m, :] = np.linalg.pinv(qm) @ z[m, max_lag:]
                mat0[m, :] = qm @ a[m, :]
            mat1 = _ten2mat(np.mean(rho * x + t, axis=0), 0)
            z[pos_missing] = np.append(
                (mat1[:, :max_lag] / rho),
                (mat1[:, max_lag:] + lambda0 * mat0) / (rho + lambda0),
                axis=1
            )[pos_missing]
        else:
            z[pos_missing] = (_ten2mat(np.mean(x + t / rho, axis=0), 0))[pos_missing]
        t = t + rho * (x - np.broadcast_to(
            _mat2ten(z, dim, 0), np.insert(dim, 0, len(dim))
        ))
        tol = np.linalg.norm((mat_hat - last_mat), 'fro') / snorm
        last_mat = mat_hat.copy()
        it += 1
        print(f"Iteration: {it}", end="\r")
        if tol < epsilon or it >= maxiter:
            break
    print(f"Iteration: {it}")
    return tensor_hat


def _tsvt(tensor: np.ndarray, phi: np.ndarray, tau: float) -> np.ndarray:
    """Tensor singular value thresholding (TSVT)."""
    dim = tensor.shape
    x = np.zeros(dim)
    tensor = np.einsum('kt, ijk -> ijt', phi, tensor)
    for t in range(dim[2]):
        u, s, v = np.linalg.svd(tensor[:, :, t], full_matrices=False)
        r = len(np.where(s > tau)[0])
        if r >= 1:
            s = s[:r]
            s[: r] = s[:r] - tau
            x[:, :, t] = u[:, :r] @ np.diag(s) @ v[:r, :]
    return np.einsum('kt, ijt -> ijk', phi, x)


def impute_latc_tubal(  # noqa: C901
    tensor: np.ndarray,
    lags: Sequence[int] = [1],
    rho0: float = 1e-7,
    lambda0: float = 2e-7,
    epsilon: float = 1e-7,
    maxiter: int = 300
) -> np.ndarray:
    """
    Impute tensor values with LATC-Tubal method by Chen, Chen and Sun (2020).

    Uses low-tubal-rank autoregressive tensor completion (LATC-Tubal).
    It is much faster than :func:`impute_latc_tnn` for very large datasets,
    with comparable accuracy.

    * description: https://arxiv.org/abs/2008.03194
    * code: https://github.com/xinychen/tensor-learning/blob/master/mats

    Args:
        tensor: Observational series in the form (series, groups, periods).
            Null values are replaced with zeros, so any zeros will be treated as null.
        lags:
        rho0:
        lambda0:
        epsilon: Convergence criterion. A smaller number will result in more iterations.
        maxiter: Maximum number of iterations.

    Returns:
        Tensor with missing values in `tensor` replaced by imputed values.
    """
    tensor = np.where(np.isnan(tensor), 0, tensor)
    dim = np.array(tensor.shape)
    dim_time = int(np.prod(dim) / dim[0])
    d = len(lags)
    max_lag = np.max(lags)
    mat = _ten2mat(tensor, 0)
    pos_missing = np.where(mat == 0)
    t = np.zeros(dim)
    z = mat.copy()
    z[pos_missing] = np.mean(mat[mat != 0])
    a = 0.001 * np.random.rand(dim[0], d)
    it = 0
    ind = np.zeros((d, dim_time - max_lag), dtype=np.int_)
    for i in range(d):
        ind[i, :] = np.arange(max_lag - lags[i], dim_time - lags[i])
    last_mat = mat.copy()
    snorm = np.linalg.norm(mat, 'fro')
    rho = rho0
    temp1 = _ten2mat(_mat2ten(z, dim, 0), 2)
    _, phi = np.linalg.eig(temp1 @ temp1.T)
    del temp1
    if dim_time > 5e3 and dim_time <= 1e4:
        sample_rate = 0.2
    elif dim_time > 1e4:
        sample_rate = 0.1
    while True:
        rho = min(rho * 1.05, 1e5)
        x = _tsvt(_mat2ten(z, dim, 0) - t / rho, phi, 1 / rho)
        mat_hat = _ten2mat(x, 0)
        mat0 = np.zeros((dim[0], dim_time - max_lag))
        temp2 = _ten2mat(rho * x + t, 0)
        if lambda0 > 0:
            if dim_time <= 5e3:
                for m in range(dim[0]):
                    qm = mat_hat[m, ind].T
                    a[m, :] = np.linalg.pinv(qm) @ z[m, max_lag:]
                    mat0[m, :] = qm @ a[m, :]
            elif dim_time > 5e3:
                for m in range(dim[0]):
                    idx = np.arange(0, dim_time - max_lag)
                    np.random.shuffle(idx)
                    idx = idx[: int(sample_rate * (dim_time - max_lag))]
                    qm = mat_hat[m, ind].T
                    a[m, :] = np.linalg.pinv(qm[idx[:], :]) @ z[m, max_lag:][idx[:]]
                    mat0[m, :] = qm @ a[m, :]
            z[pos_missing] = np.append(
                (temp2[:, :max_lag] / rho),
                (temp2[:, max_lag:] + lambda0 * mat0) / (rho + lambda0), axis=1
            )[pos_missing]
        else:
            z[pos_missing] = temp2[pos_missing] / rho
        t = t + rho * (x - _mat2ten(z, dim, 0))
        tol = np.linalg.norm((mat_hat - last_mat), 'fro') / snorm
        last_mat = mat_hat.copy()
        it += 1
        if not np.mod(it, 10):
            temp1 = _ten2mat(_mat2ten(z, dim, 0) - t / rho, 2)
            _, phi = np.linalg.eig(temp1 @ temp1.T)
            del temp1
        print(f"Iteration: {it}", end="\r")
        if tol < epsilon or it >= maxiter:
            break
    print(f"Iteration: {it}")
    return x


# ---- Anomaly detection ---- #


class Timeseries:
    """
    Multivariate timeseries for anomalies detection and imputation.

    Attributes:
        xi: Reference to the original values (can be null).
            Many methods assume that these represent chronological, regular timeseries.
        x: Copy of :attr:`xi` with any flagged values replaced with null.
        flags: Flag label for each value, or null if not flagged.
        flagged: Running list of flags that have been checked so far.
        index: Row index.
        columns: Column names.
    """

    def __init__(self, x: Union[np.ndarray, pd.DataFrame]) -> None:
        """
        Initialize a multivariate timeseries.

        Args:
            x: Timeseries with shape (n observations, m variables).
                If :class:`pandas.DataFrame`, :attr:`index` and :attr:`columns`
                are equal to `x.index` and `x.columns`, respectively.
                Otherwise, :attr:`index` and :attr:`columns` are the default
                `pandas.RangeIndex`.
        """
        self.xi: np.ndarray
        self.index: pd.Index
        self.columns: pd.Index
        if isinstance(x, pd.DataFrame):
            self.xi = x.values
            self.index = x.index
            self.columns = x.columns
        else:
            self.xi = x
            self.index = pd.RangeIndex(x.shape[0])
            self.columns = pd.RangeIndex(x.shape[1])
        self.x: np.ndarray = self.xi.copy()
        self.flags: np.ndarray = np.empty(self.x.shape, dtype=object)
        self.flagged: List[str] = []

    def to_dataframe(self, array: np.ndarray = None, copy: bool = True) -> pd.DataFrame:
        """
        Return multivariate timeseries as a :class:`pandas.DataFrame`.

        Args:
            array: Two-dimensional array to use. If `None`, uses :attr:`x`.
            copy: Whether to use a copy of `array`.
        """
        x = self.x if array is None else array
        return pd.DataFrame(x, columns=self.columns, index=self.index, copy=copy)

    def flag(self, mask: np.ndarray, flag: str) -> None:
        """
        Flag values.

        Flags values (if not already flagged) and nulls flagged values.

        Args:
            mask: Boolean mask of the values to flag.
            flag: Flag name.
        """
        # Only flag unflagged values
        mask = mask & ~np.isnan(self.x)
        self.flags[mask] = flag
        self.flagged.append(flag)
        # Null flagged values
        self.x[mask] = np.nan
        # Clear cached metrics
        for name in dir(self):
            attr = getattr(self, name)
            if hasattr(attr, 'cache_clear'):
                attr.cache_clear()

    def unflag(self, flags: Iterable[str] = None) -> None:
        """
        Unflag values.

        Unflags values by restoring their original values and removing their flag.

        Args:
            flags: Flag names. If `None`, all flags are removed.
        """
        mask = slice(None) if flags is None else np.isin(self.flags, flags)
        self.flags[mask] = None
        self.x[mask] = self.xi[mask]
        self.flagged = [f for f in self.flagged if flags is not None and f not in flags]

    def flag_negative_or_zero(self) -> None:
        """Flag negative or zero values (NEGATIVE_OR_ZERO)."""
        mask = self.x <= 0
        self.flag(mask, "NEGATIVE_OR_ZERO")

    def flag_identical_run(self, length: int = 3) -> None:
        """
        Flag the last values in identical runs (IDENTICAL_RUN).

        Args:
            length: Run length to flag.
                If `3`, the third (and subsequent) identical values are flagged.

        Raises:
            ValueError: Run length must be 2 or greater.
        """
        if length < 2:
            raise ValueError("Run length must be 2 or greater")
        mask = np.ones(self.x.shape, dtype=bool)
        mask[0] = False
        for n in range(1, length):
            mask[n:] &= self.x[n:] == self.x[:-n]
        self.flag(mask, "IDENTICAL_RUN")

    def flag_global_outlier(self, medians: float = 9) -> None:
        """
        Flag values greater or less than n times the global median (GLOBAL_OUTLIER).

        Args:
            medians: Number of times the median the value must exceed the median.
        """
        median = np.nanmedian(self.x, axis=0)
        mask = np.abs(self.x - median) > np.abs(median * medians)
        self.flag(mask, "GLOBAL_OUTLIER")

    def flag_global_outlier_neighbor(self, neighbors: int = 1) -> None:
        """
        Flag values neighboring global outliers (GLOBAL_OUTLIER_NEIGHBOR).

        Args:
            neighbors: Number of neighbors to flag on either side of each outlier.

        Raises:
            ValueError: Global outliers must be flagged first.
        """
        if "GLOBAL_OUTLIER" not in self.flagged:
            raise ValueError("Global outliers must be flagged first")
        mask = np.zeros(self.x.shape, dtype=bool)
        outliers = self.flags == "GLOBAL_OUTLIER"
        for shift in range(1, neighbors + 1):
            # Neighbors before
            mask[:-shift][outliers[shift:]] = True
            # Neighors after
            mask[shift:][outliers[:-shift]] = True
        self.flag(mask, "GLOBAL_OUTLIER_NEIGHBOR")

    @functools.lru_cache(maxsize=2)
    def rolling_median(self, window: int = 48) -> np.ndarray:
        """
        Rolling median of values.

        Args:
            window: Number of values in the moving window.
        """
        # RUGGLES: rollingDem, rollingDemLong (window=480)
        df = pd.DataFrame(self.x, copy=False)
        return df.rolling(window, min_periods=1, center=True).median().values

    def rolling_median_offset(self, window: int = 48) -> np.ndarray:
        """
        Values minus the rolling median.

        Estimates the local cycle in cyclical data by removing longterm trends.

        Args:
            window: Number of values in the moving window.
        """
        # RUGGLES: dem_minus_rolling
        return self.x - self.rolling_median(window=window)

    def median_of_rolling_median_offset(
        self,
        window: int = 48,
        shifts: Sequence[int] = range(-240, 241, 24)
    ) -> np.ndarray:
        """
        Median of the offset from the rolling median.

        Calculated by shifting the rolling median offset (:meth:`rolling_median_offset`)
        by different numbers of values, then taking the median at each position.
        Estimates the typical local cycle in cyclical data.

        Args:
            window: Number of values in the moving window for the rolling median.
            shifts: Number of values to shift the rolling median offset by.
        """
        # RUGGLES: vals_dem_minus_rolling
        offset = self.rolling_median_offset(window=window)
        # Fast numpy implementation of pd.DataFrame.shift
        shifted = np.empty([len(shifts), *offset.shape], dtype=float)
        for i, shift in enumerate(shifts):
            if shift > 0:
                shifted[i, :shift] = np.nan
                shifted[i, shift:] = offset[:-shift]
            elif shift < 0:
                shifted[i, shift:] = np.nan
                shifted[i, :shift] = offset[-shift:]
            else:
                shifted[i, :] = offset
        # Ignore warning for rows with all null values
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", category=RuntimeWarning, message="All-NaN slice encountered"
            )
            return np.nanmedian(shifted, axis=0)

    def rolling_iqr_of_rolling_median_offset(
        self,
        window: int = 48,
        iqr_window: int = 240
    ) -> np.ndarray:
        """
        Rolling interquartile range (IQR) of rolling median offset.

        Estimates the spread of the local cycles in cyclical data.

        Args:
            window: Number of values in the moving window for the rolling median.
            iqr_window: Number of values in the moving window for the rolling IQR.
        """
        # RUGGLES: dem_minus_rolling_IQR
        offset = self.rolling_median_offset(window=window)
        df = pd.DataFrame(offset, copy=False)
        rolling = df.rolling(iqr_window, min_periods=1, center=True)
        return (rolling.quantile(0.75) - rolling.quantile(0.25)).values

    def median_prediction(
        self,
        window: int = 48,
        shifts: Sequence[int] = range(-240, 241, 24),
        long_window: int = 480
    ) -> np.ndarray:
        """
        Values predicted from local and regional rolling medians.

        Calculated as `{ local median } +
        { median of local median offset } * { local median } / { regional median }`.

        Args:
            window: Number of values in the moving window for the local rolling median.
            shifts: Positions to shift the local rolling median offset by,
                for computing its median.
            long_window: Number of values in the moving window
                for the regional (long) rolling median.
        """
        # RUGGLES: hourly_median_dem_dev (multiplied by rollingDem)
        return self.rolling_median(window=window) * (
            1 + self.median_of_rolling_median_offset(window=window, shifts=shifts)
            / self.rolling_median(window=long_window)
        )

    def flag_local_outlier(
        self,
        window: int = 48,
        shifts: Sequence[int] = range(-240, 241, 24),
        long_window: int = 480,
        iqr_window: int = 240,
        multiplier: Tuple[float, float] = (3.5, 2.5)
    ) -> None:
        """
        Flag local outliers (LOCAL_OUTLIER_HIGH, LOCAL_OUTLIER_LOW).

        Flags values which are above or below the :meth:`median_prediction` by more than
        a `multiplier` times the :meth:`rolling_iqr_of_rolling_median_offset`.

        Args:
            window: Number of values in the moving window for the local rolling median.
            shifts: Positions to shift the local rolling median offset by,
                for computing its median.
            long_window: Number of values in the moving window
                for the regional (long) rolling median.
            iqr_window: Number of values in the moving window
                for the rolling interquartile range (IQR).
            multiplier: Number of times the :meth:`rolling_iqr_of_rolling_median_offset`
                the value must be above (HIGH) and below (LOW)
                the :meth:`median_prediction` to be flagged.
        """
        # Compute constants
        prediction = self.median_prediction(
            window=window, shifts=shifts, long_window=long_window
        )
        iqr = self.rolling_iqr_of_rolling_median_offset(
            window=window, iqr_window=iqr_window
        )
        mask = self.x > prediction + multiplier[0] * iqr
        self.flag(mask, "LOCAL_OUTLIER_HIGH")
        # As in original code, do not recompute constants with new nulls
        mask = self.x < prediction - multiplier[1] * iqr
        self.flag(mask, "LOCAL_OUTLIER_LOW")

    def diff(self, shift: int = 1) -> np.ndarray:
        """
        Values minus the value of their neighbor.

        Args:
            shift: Positions to shift for calculating the difference.
                Positive values select a preceding (left) neighbor.
        """
        # RUGGLES: delta_pre (shift=1), delta_post (shift=-1)
        return array_diff(self.x, shift)

    def rolling_iqr_of_diff(self, shift: int = 1, window: int = 240) -> np.ndarray:
        """
        Rolling interquartile range (IQR) of the difference between neighboring values.

        Args:
            shift: Positions to shift for calculating the difference.
            window: Number of values in the moving window for the rolling IQR.
        """
        # RUGGLES: delta_rolling_iqr
        diff = self.diff(shift=shift)
        df = pd.DataFrame(diff, copy=False)
        rolling = df.rolling(window, min_periods=1, center=True)
        return (rolling.quantile(0.75) - rolling.quantile(0.25)).values

    def flag_double_delta(
        self, iqr_window: int = 240, multiplier: float = 2
    ) -> None:
        """
        Flag values very different from their neighbors on either side (DOUBLE_DELTA).

        Flags values whose differences to both neighbors on either side exceeds a
        `multiplier` times the rolling interquartile range (IQR) of neighbor difference.

        Args:
            iqr_window: Number of values in the moving window for the rolling IQR
                of neighbor difference.
            multiplier: Number of times the rolling IQR of neighbor difference
                the value's difference to its neighbors must exceed
                for the value to be flagged.
        """
        before = self.diff(shift=1)
        after = self.diff(shift=-1)
        iqr = multiplier * self.rolling_iqr_of_diff(shift=1, window=iqr_window)
        mask = (np.minimum(before, after) > iqr) | (np.maximum(before, after) < -iqr)
        self.flag(mask, "DOUBLE_DELTA")

    @functools.lru_cache(maxsize=2)
    def relative_median_prediction(self, **kwargs: Any) -> np.ndarray:
        """
        Values divided by their value predicted from medians.

        Args:
            kwargs: Arguments to :meth:`median_prediction`.
        """
        # RUGGLES: dem_rel_diff_wrt_hourly, dem_rel_diff_wrt_hourly_long (window=480)
        return self.x / self.median_prediction(**kwargs)

    def iqr_of_diff_of_relative_median_prediction(
        self, shift: int = 1, **kwargs: Any
    ) -> np.ndarray:
        """
        Interquartile range of the running difference of the relative median prediction.

        Args:
            shift: Positions to shift for calculating the difference.
                Positive values select a preceding (left) neighbor.
            kwargs: Arguments to :meth:`relative_median_prediction`.
        """
        # RUGGLES: iqr_relative_deltas
        diff = array_diff(self.relative_median_prediction(**kwargs), shift)
        return scipy.stats.iqr(diff, nan_policy='omit', axis=0)

    def _find_single_delta(
        self,
        relative_median_prediction: np.ndarray,
        relative_median_prediction_long: np.ndarray,
        rolling_iqr_of_diff: np.ndarray,
        iqr_of_diff_of_relative_median_prediction: np.ndarray,
        reverse: bool = False
    ) -> np.ndarray:
        not_nan = ~np.isnan(self.x)
        mask = np.zeros(self.x.shape, dtype=bool)
        for col in range(self.x.shape[1]):
            indices = np.flatnonzero(not_nan[:, col])[::(-1 if reverse else 1)]
            previous, current = indices[:-1], indices[1:]
            while len(current):
                # Evaluate value pairs
                diff = np.abs(self.x[current, col] - self.x[previous, col])
                diff_relative_median_prediction = np.abs(
                    relative_median_prediction[current, col] -
                    relative_median_prediction[previous, col]
                )
                # Compare max deviation across short and long rolling median
                # to catch when outliers pull short median towards themselves.
                previous_max = np.maximum(
                    np.abs(1 - relative_median_prediction[previous, col]),
                    np.abs(1 - relative_median_prediction_long[previous, col]),
                )
                current_max = np.maximum(
                    np.abs(1 - relative_median_prediction[current, col]),
                    np.abs(1 - relative_median_prediction_long[current, col]),
                )
                flagged = (
                    (diff > rolling_iqr_of_diff[current, col]) &
                    (
                        diff_relative_median_prediction >
                        iqr_of_diff_of_relative_median_prediction[col]
                    ) &
                    (current_max > previous_max)
                )
                flagged_indices = current[flagged]
                if not flagged_indices.size:
                    break
                # Find position of flagged indices in index
                if reverse:
                    indices_idx = indices.size - (
                        indices[::-1].searchsorted(flagged_indices, side='right')
                    )
                else:
                    indices_idx = indices.searchsorted(flagged_indices, side='left')
                # Only flag first of consecutive flagged indices
                # TODO: May not be necessary after first iteration
                unflagged = np.concatenate(([False], np.diff(indices_idx) == 1))
                flagged_indices = np.delete(flagged_indices, unflagged)
                indices_idx = np.delete(indices_idx, unflagged)
                mask[flagged_indices, col] = True
                flagged[flagged] = ~unflagged
                # Bump current index of flagged pairs to next unflagged index
                # Next index always unflagged because flagged runs are not permitted
                next_indices_idx = indices_idx + 1
                if next_indices_idx[-1] == len(indices):
                    # Drop last index if out of range
                    next_indices_idx = next_indices_idx[:-1]
                current = indices[next_indices_idx]
                # Trim previous values to length of current values
                previous = previous[flagged][:len(current)]
                # Delete flagged indices
                indices = np.delete(indices, indices_idx)
        return mask

    def flag_single_delta(
        self,
        window: int = 48,
        shifts: Sequence[int] = range(-240, 241, 24),
        long_window: int = 480,
        iqr_window: int = 240,
        multiplier: float = 5,
        rel_multiplier: float = 15
    ) -> None:
        """
        Flag values very different from the nearest unflagged value (SINGLE_DELTA).

        Flags values whose difference to the nearest unflagged value,
        with respect to value and relative median prediction,
        differ by less than a multiplier times the rolling interquartile range (IQR)
        of the difference -
        `multiplier` times :meth:`rolling_iqr_of_diff` and
        `rel_multiplier` times :meth:`iqr_of_diff_of_relative_mean_prediction`,
        respectively.

        Args:
            window: Number of values in the moving window for the rolling median
                (for the relative median prediction).
            shifts: Positions to shift the local rolling median offset by,
                for computing its median (for the relative median prediction).
            long_window: Number of values in the moving window for the long rolling
                median (for the relative median prediction).
            iqr_window: Number of values in the moving window for the rolling IQR
                of neighbor difference.
            multiplier: Number of times the rolling IQR of neighbor difference
                the value's difference to its neighbor must exceed
                for the value to be flagged.
            rel_multiplier: Number of times the rolling IQR of relative median
                prediction the value's prediction difference to its neighbor must exceed
                for the value to be flagged.
        """
        # Compute constants used in both forward and reverse pass
        relative_median_prediction = self.relative_median_prediction(
            window=window, shifts=shifts, long_window=long_window
        )
        relative_median_prediction_long = self.relative_median_prediction(
            window=long_window, shifts=shifts, long_window=long_window
        )
        rolling_iqr_of_diff = multiplier * self.rolling_iqr_of_diff(
            shift=1, window=iqr_window
        )
        iqr_of_diff_of_relative_median_prediction = rel_multiplier * (
            self.iqr_of_diff_of_relative_median_prediction(
                shift=1, window=window, shifts=shifts, long_window=long_window
            )
        )
        # Set values flagged in forward pass to null before reverse pass
        mask = self._find_single_delta(
            relative_median_prediction,
            relative_median_prediction_long,
            rolling_iqr_of_diff,
            iqr_of_diff_of_relative_median_prediction,
            reverse=False
        )
        self.flag(mask, "SINGLE_DELTA")
        # Repeat in reverse to get all options.
        # As in original code, do not recompute constants with new nulls
        mask = self._find_single_delta(
            relative_median_prediction,
            relative_median_prediction_long,
            rolling_iqr_of_diff,
            iqr_of_diff_of_relative_median_prediction,
            reverse=True
        )
        self.flag(mask, "SINGLE_DELTA")

    def flag_anomalous_region(
        self, window: int = 48, threshold: float = 0.15
    ) -> None:
        """
        Flag values surrounded by flagged values (ANOMALOUS_REGION).

        Original null values are not considered flagged values.

        Args:
            width: Width of regions.
            threshold: Fraction of flagged values required for a region to be flagged.
        """
        # Check whether unflagged
        mask = np.equal(self.flags, None)
        # Check whether after or before half-width region with 1+ flagged values
        half_window = window // 2
        is_after = (
            pd.DataFrame(mask, copy=False)
            .rolling(window=half_window)
            .mean()
            .lt(1)
            .values
        )
        is_before = np.roll(is_after, -(half_window - 1), axis=0)
        # Check whether not part of a run of unflagged values longer than a half-width
        is_not_run = np.empty_like(mask)
        for col in range(mask.shape[1]):
            rvalues, rlengths = encode_run_length(mask[:, col])
            is_short_run = np.where(rvalues, rlengths, 0) <= half_window
            is_not_run[:, col] = np.repeat(is_short_run, rlengths)
        # Check whether within full-width region with too many flagged values
        is_region = (
            pd.DataFrame(~mask, copy=False)
            .rolling(window=window, center=True)
            .mean()
            .gt(threshold)
            .rolling(window=window, center=True)
            .max()
            .eq(True)
            .values
        )
        # Flag if all conditions are met
        mask &= is_after & is_before & is_not_run & is_region
        self.flag(mask, "ANOMALOUS_REGION")

    def flag_ruggles(self) -> None:
        """
        Flag values following the method of Ruggles and others (2020).

        Assumes values are hourly electricity demand.

        * description: https://doi.org/10.1038/s41597-020-0483-x
        * code: https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code
        """
        # Step 1
        self.flag_negative_or_zero()
        self.flag_identical_run(length=3)
        self.flag_global_outlier(medians=9)
        self.flag_global_outlier_neighbor(neighbors=1)
        # Step 2
        # NOTE: In original code, statistics used for the flags below are precomputed
        # here, rather than computed for each flag with nulls added by previous flags.
        window = 48
        long_window = 480
        iqr_window = 240
        shifts = range(-240, 241, 24)
        self.flag_local_outlier(
            window=window,
            shifts=shifts,
            long_window=long_window,
            iqr_window=iqr_window,
            multiplier=(3.5, 2.5),
        )
        self.flag_double_delta(iqr_window=iqr_window, multiplier=2)
        self.flag_single_delta(
            window=window,
            shifts=shifts,
            long_window=long_window,
            iqr_window=iqr_window,
            multiplier=5,
            rel_multiplier=15,
        )
        self.flag_anomalous_region(window=window + 1, threshold=0.15)

    def summarize_flags(self) -> pd.DataFrame:
        """Summarize flagged values by flag, count and median."""
        stats = {}
        for col in range(self.xi.shape[1]):
            stats[self.columns[col]] = (
                pd.Series(self.xi[:, col])
                .groupby(self.flags[:, col])
                .agg(['count', 'median'])
            )
        df = pd.concat(stats, names=['column', 'flag']).reset_index()
        # Sort flags by flagged order
        ordered = df['flag'].astype(pd.CategoricalDtype(pd.unique(self.flagged)))
        return df.assign(flag=ordered).sort_values(['column', 'flag'])

    def plot_flags(self, name: Any = 0) -> None:
        """
        Plot cleaned series and anomalous values colored by flag.

        Args:
            name: Series to plot, as either an integer index or name in :attr:`columns`.
        """
        if name not in self.columns:
            name = self.columns[name]
        col = list(self.columns).index(name)
        plt.plot(self.index, self.x[:, col], color='lightgrey', marker='.', zorder=1)
        colors = {
            'NEGATIVE_OR_ZERO': 'pink',
            'IDENTICAL_RUN': 'blue',
            'GLOBAL_OUTLIER': 'brown',
            'GLOBAL_OUTLIER_NEIGHBOR': 'brown',
            'LOCAL_OUTLIER_HIGH': 'purple',
            'LOCAL_OUTLIER_LOW': 'purple',
            'DOUBLE_DELTA': 'green',
            'SINGLE_DELTA': 'red',
            'ANOMALOUS_REGION': 'orange',
        }
        for flag in colors:
            mask = self.flags[:, col] == flag
            x, y = self.index[mask], self.xi[mask, col]
            # Set zorder manually to ensure flagged points are drawn on top
            plt.scatter(x, y, c=colors[flag], label=flag, zorder=2)
        plt.legend()

    def simulate_nulls(
        self,
        lengths: Sequence[int] = None,
        padding: int = 1,
        intersect: bool = False,
        overlap: bool = False,
    ) -> np.ndarray:
        """
        Find non-null values to null to match a run-length distribution.

        Args:
            length: Length of null runs to simulate for each series.
                By default, uses the run lengths of null values in each series.
            padding: Minimum number of non-null values between simulated null runs
                and between simulated and existing null runs.
            intersect: Whether simulated null runs can intersect each other.
            overlap: Whether simulated null runs can overlap existing null runs.
                If `True`, `padding` is ignored.

        Returns:
            Boolean mask of current non-null values to set to null.

        Raises:
            ValueError: Cound not find space for run of length {length}.

        Examples:
            >>> x = np.column_stack([[1, 2, np.nan, 4, 5, 6, 7, np.nan, np.nan]])
            >>> s = Timeseries(x)
            >>> s.simulate_nulls().ravel()
            array([ True, False, False, False, True, True, False, False, False])
            >>> s.simulate_nulls(lengths=[4], padding=0).ravel()
            array([False, False, False, True, True, True, True, False, False])
        """
        new_nulls = np.zeros(self.x.shape, dtype=bool)
        for col in range(self.x.shape[1]):
            is_null = np.isnan(self.x[:, col])
            if lengths is None:
                run_values, run_lengths = encode_run_length(is_null)
                run_lengths = run_lengths[run_values]
            else:
                run_lengths = lengths
            is_new_null = insert_run_length(
                new_nulls[:, col],
                values=np.ones(len(run_lengths), dtype=bool),
                lengths=run_lengths,
                mask=None if overlap else ~is_null,
                padding=0 if overlap else padding,
                intersect=intersect
            )
            if overlap:
                is_new_null &= ~is_null
            new_nulls[:, col] = is_new_null
        return new_nulls

    def fold_tensor(self, x: np.ndarray = None, periods: int = 24) -> np.ndarray:
        """
        Fold into a 3-dimensional tensor representation.

        Folds the series `x` (number of observations, number of series)
        into a 3-d tensor (number of series, number of groups, number of periods),
        splitting observations into groups of length `periods`.
        For example, each group may represent a day and each period the hour of the day.

        Args:
            x: Series array to fold. Uses :attr:`x` by default.
            periods: Number of consecutive values in each series to fold into a group.

        Returns:
            >>> x = np.column_stack([[1, 2, 3, 4, 5, 6], [10, 20, 30, 40, 50, 60]])
            >>> s = Timeseries(x)
            >>> tensor = s.fold_tensor(periods=3)
            >>> tensor[0]
            array([[1, 2, 3],
                   [4, 5, 6]])
            >>> np.all(x == s.unfold_tensor(tensor))
            True
        """
        tensor_shape = self.x.shape[1], self.x.shape[0] // periods, periods
        x = self.x if x is None else x
        return x.T.reshape(tensor_shape)

    def unfold_tensor(self, tensor: np.ndarray) -> np.ndarray:
        """
        Unfold a 3-dimensional tensor representation.

        Performs the reverse of :meth:`fold_tensor`.
        """
        return tensor.T.reshape(self.x.shape, order='F')

    def impute(
        self,
        mask: np.ndarray = None,
        periods: int = 24,
        blocks: int = 1,
        method: str = 'tubal',
        **kwargs: Any
    ) -> np.ndarray:
        """
        Impute null values.

        .. note::
            The imputation method requires that nulls be replaced by zeros,
            so the series cannot already contain zeros.

        Args:
            mask: Boolean mask of values to impute in addition to
                any null values in :attr:`x`.
            periods: Number of consecutive values in each series to fold into a group.
                See :meth:`fold_tensor`.
            blocks: Number of blocks into which to split the series for imputation.
                This has been found to reduce processing time for `method='tnn'`.
            method: Imputation method to use
                ('tubal': :func:`impute_latc_tubal`, 'tnn': :func:`impute_latc_tnn`).
            kwargs: Optional arguments to `method`.

        Returns:
            Array of same shape as :attr:`x` with all null values
            (and those selected by `mask`) replaced with imputed values.

        Raises:
            ValueError: Zero values present. Replace with very small value.
        """
        imputer = {'tubal': impute_latc_tubal, 'tnn': impute_latc_tnn}[method]
        if mask is None:
            x = self.x.copy()
        else:
            x = np.where(mask, np.nan, self.x)
        if (x == 0).any():
            raise ValueError("Zero values present. Replace with very small value.")
        tensor = self.fold_tensor(x, periods=periods)
        n = tensor.shape[1]
        ends = [*range(0, n, int(np.ceil(n / blocks))), n]
        for i in range(blocks):
            if blocks > 1:
                print(f"Block: {i}")
            idx = slice(None), slice(ends[i], ends[i + 1]), slice(None)
            tensor[idx] = imputer(tensor[idx], **kwargs)
        return self.unfold_tensor(tensor)

    def summarize_imputed(
        self,
        imputed: np.ndarray,
        mask: np.ndarray
    ) -> pd.DataFrame:
        """
        Summarize the fit of imputed values to actual values.

        Summarizes the agreement between actual and imputed values with the
        following statistics:

        * `mpe`: Mean percent error, `(actual - imputed) / actual`.
        * `mape`: Mean absolute percent error, `abs(mpe)`.

        Args:
            imputed: Series of same shape as :attr:`x` with imputed values.
                See :meth:`impute`.
            mask: Boolean mask of imputed values that were not null in :attr:`x`.
                See :meth:`simulate_nulls`.

        Returns:
            Table of imputed value statistics for each series.
        """
        stats = []
        for col in range(self.x.shape[1]):
            x = self.x[mask[:, col], col]
            if not x.size:
                continue
            pe = (x - imputed[mask[:, col], col]) / x
            pe = pe[~np.isnan(pe)]
            stats.append({
                'column': self.columns[col],
                'count': x.size,
                'mpe': np.mean(pe),
                'mape': np.mean(np.abs(pe)),
            })
        return pd.DataFrame(stats)
