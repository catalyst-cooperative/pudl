"""Unit tests for the deterministic parts of the cross-year record linkage graph."""

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from pudl.analysis.record_linkage.embed_dataframe import FeatureMatrix
from pudl.analysis.record_linkage.link_cross_year import (
    DistanceMatrix,
    PenalizeReportYearDistanceConfig,
    sort_records_for_clustering,
)


def _df_and_matrix() -> tuple[pd.DataFrame, FeatureMatrix]:
    """Three records, out of primary-key order, each tagged with its own feature row."""
    df = pd.DataFrame(
        {"record_id": ["r3", "r1", "r2"], "report_year": [2000, 2000, 2000]}
    )
    # Row i of the matrix belongs to df.iloc[i] -- e.g. the row for "r3" is [3.0].
    matrix = np.array([[3.0], [1.0], [2.0]])
    return df, FeatureMatrix(matrix=matrix, index=df.index)


def testsort_records_for_clustering_sorts_by_record_id():
    """The sorted frame is ordered by primary key and has a fresh RangeIndex."""
    df, feature_matrix = _df_and_matrix()
    sorted_df, _ = sort_records_for_clustering(df, feature_matrix)
    assert sorted_df["record_id"].tolist() == ["r1", "r2", "r3"]
    assert sorted_df.index.equals(pd.RangeIndex(3))


def testsort_records_for_clustering_reorders_feature_matrix_to_match():
    """Feature matrix rows must follow their records through the sort."""
    df, feature_matrix = _df_and_matrix()
    sorted_df, sorted_feature_matrix = sort_records_for_clustering(df, feature_matrix)
    # r1, r2, r3 in that order -> feature rows [1.0], [2.0], [3.0]
    np.testing.assert_array_equal(
        sorted_feature_matrix.matrix, np.array([[1.0], [2.0], [3.0]])
    )
    assert sorted_feature_matrix.index.equals(pd.RangeIndex(3))


def testsort_records_for_clustering_is_independent_of_input_order():
    """Shuffling df and feature_matrix together must not change the result."""
    df, feature_matrix = _df_and_matrix()
    shuffled_positions = [2, 0, 1]
    shuffled_df = df.iloc[shuffled_positions]
    shuffled_matrix = FeatureMatrix(
        matrix=feature_matrix.matrix[shuffled_positions],  # type: ignore[bad-argument-type]
        index=feature_matrix.index[shuffled_positions],
    )

    sorted_df, sorted_feature_matrix = sort_records_for_clustering(df, feature_matrix)
    shuffled_sorted_df, shuffled_sorted_matrix = sort_records_for_clustering(
        shuffled_df, shuffled_matrix
    )

    assert_frame_equal(sorted_df, shuffled_sorted_df)
    np.testing.assert_array_equal(
        sorted_feature_matrix.matrix, shuffled_sorted_matrix.matrix
    )


def test_distance_matrix_is_float64_with_rounding_noise_removed():
    """Float noise below the rounding precision must not survive into distances.

    Records 0 and 1 differ by 1e-9, which BLAS rounding error can produce between
    records that are really identical. Record 2 is exactly 0.5 away from record 0.
    """
    original_df = pd.DataFrame({"report_year": [2000, 2001, 2002]})
    features = np.array([[0.0], [1e-9], [0.5000000001]])
    config = PenalizeReportYearDistanceConfig(distance_penalty=10000.0)

    distances = DistanceMatrix(features, original_df, config).distance_matrix

    assert distances.dtype == np.float64
    assert distances[0, 1] == 0.0
    assert distances[0, 2] == 0.5
    assert distances[1, 2] == 0.5
    np.testing.assert_array_equal(distances, distances.T)


def test_distance_matrix_penalizes_same_year_records():
    """Records from the same report year are pushed apart; the diagonal stays zero."""
    original_df = pd.DataFrame({"report_year": [2000, 2000, 2001]})
    features = np.array([[0.0], [0.0], [1.0]])
    config = PenalizeReportYearDistanceConfig(distance_penalty=10000.0)

    distances = DistanceMatrix(features, original_df, config).distance_matrix

    assert distances[0, 1] == 10000.0
    assert distances[0, 0] == 0.0
    assert distances[0, 2] == 1.0
