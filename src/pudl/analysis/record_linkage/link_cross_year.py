"""Define a record linkage model interface and implement common functionality."""

from pathlib import Path
from tempfile import TemporaryDirectory

import mlflow
import numpy as np
import pandas as pd
from dagster import Config, graph, op
from numba import njit
from numba.typed import List
from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.metrics import pairwise_distances_chunked
from sklearn.neighbors import NearestNeighbors

import pudl
from pudl.analysis.ml_tools import experiment_tracking
from pudl.analysis.record_linkage.embed_dataframe import FeatureMatrix

logger = pudl.logging_helpers.get_logger(__name__)


class PenalizeReportYearDistanceConfig(Config):
    """Compute distance between records and add penalty to records from same year.

    The metric can be any string accepted by :func:`scipy.spatial.distance.pdist`, e.g.
    ``cosine`` or ``euclidean``.
    """

    distance_penalty: float = 10000.0
    metric: str = "euclidean"


class DistanceMatrix:
    """Class to wrap a distance matrix saved in a np.memmap."""

    def __init__(
        self,
        feature_matrix: np.ndarray,
        original_df: pd.DataFrame,
        config: PenalizeReportYearDistanceConfig,
    ):
        """Compute distance matrix from feature_matrix and write to memmap."""
        self.file_buffer = TemporaryDirectory()

        filename = Path(self.file_buffer.name) / "distance_matrix.dat"
        self.distance_matrix = np.memmap(
            filename,
            dtype="float32",
            mode="w+",
            shape=(feature_matrix.shape[0], feature_matrix.shape[0]),
        )

        # Compute distances in chunks and write to memmap
        row_start = 0
        for chunk in pairwise_distances_chunked(feature_matrix, metric=config.metric):
            self.distance_matrix[row_start : row_start + len(chunk), :] = chunk[:, :]
            self.distance_matrix.flush()
            row_start += len(chunk)

        # Apply distance penalty to records from the same year
        year_inds = original_df.groupby("report_year").indices
        for inds in year_inds.values():
            matching_year_inds = np.array(np.meshgrid(inds, inds)).T.reshape(-1, 2)
            self.distance_matrix[matching_year_inds[:, 0], matching_year_inds[:, 1]] = (
                config.distance_penalty
            )

        np.fill_diagonal(self.distance_matrix, 0)
        self.distance_matrix.flush()

        # Convert distance matrix to read only memory map
        self.distance_matrix = np.memmap(
            filename,
            dtype="float32",
            mode="r",
            shape=(feature_matrix.shape[0], feature_matrix.shape[0]),
        )


def get_cluster_distance_matrix(
    distance_matrix: np.ndarray, cluster_inds: np.ndarray
) -> np.ndarray:
    """Return a distance matrix with only distances within a cluster."""
    cluster_size = len(cluster_inds)
    dist_inds = np.array(np.meshgrid(cluster_inds, cluster_inds)).T.reshape(-1, 2)
    return distance_matrix[dist_inds[:, 0], dist_inds[:, 1]].reshape(
        (cluster_size, cluster_size)
    )


@njit
def get_average_distance_matrix(
    distance_matrix: np.ndarray,
    cluster_groups: list[list[int]],
) -> np.ndarray:
    """Compute average distance between two clusters of records given indices of each cluster."""
    # Prepare a distance matrix of (n_clusters x n_clusters)
    # Distance matrix contains average distance between each cluster
    n_clusters = len(cluster_groups)
    average_dist_matrix = np.zeros((n_clusters, n_clusters))

    # Heavy nested looping optimized by numba
    for i, cluster_i in enumerate(cluster_groups):
        for j, cluster_j in enumerate(cluster_groups[:i]):
            total_dist = 0
            for cluster_i_ind in cluster_i:
                for cluster_j_ind in cluster_j:
                    total_dist += distance_matrix[cluster_i_ind, cluster_j_ind]

            average_dist = total_dist / (len(cluster_i) + len(cluster_j))
            average_dist_matrix[i, j] = average_dist
            average_dist_matrix[j, i] = average_dist

    return average_dist_matrix


@op(tags={"memory-use": "high"})
def compute_distance_with_year_penalty(
    config: PenalizeReportYearDistanceConfig,
    feature_matrix: FeatureMatrix,
    original_df: pd.DataFrame,
) -> DistanceMatrix:
    """Compute a distance matrix and penalize records from the same year."""
    logger.info(f"Dist metric: {config.metric}")
    return DistanceMatrix(feature_matrix.matrix, original_df, config)


class DBSCANConfig(Config):
    """Configuration for DBSCAN step."""

    #: See :class:`sklearn.cluster.DBSCAN` for details.
    eps: float = 0.5
    min_samples: int = 1


@op
def cluster_records_dbscan(
    config: DBSCANConfig,
    distance_matrix: DistanceMatrix,
    original_df: pd.DataFrame,
    experiment_tracker: experiment_tracking.ExperimentTracker,
) -> pd.DataFrame:
    """Generate initial IDs using DBSCAN algorithm."""
    # DBSCAN is very efficient when passed a sparse radius neighbor graph
    neighbor_computer = NearestNeighbors(radius=config.eps, metric="precomputed")
    neighbor_computer.fit(distance_matrix.distance_matrix)
    neighbor_graph = neighbor_computer.radius_neighbors_graph(mode="distance")

    # Classify records
    classifier = DBSCAN(metric="precomputed", eps=config.eps, min_samples=2)

    # Create dataframe containing only report year and label columns
    id_year_df = pd.DataFrame(
        {
            "report_year": original_df.loc[:, "report_year"],
            "record_label": classifier.fit_predict(neighbor_graph),
        }
    )

    logger.info(
        f"{id_year_df.record_label.nunique()} unique record IDs found after DBSCAN step."
    )
    experiment_tracker.execute_logging(
        lambda: mlflow.log_metric("dbscan_unique", id_year_df.record_label.nunique())
    )
    return id_year_df


class SplitClustersConfig(Config):
    """Configuration for AgglomerativeClustering used to split overmerged clusters."""

    #: See :class:`sklearn.cluster.AgglomerativeClustering` for details.
    distance_threshold: float = 0.5


@op
def split_clusters(
    config: SplitClustersConfig,
    distance_matrix: DistanceMatrix,
    id_year_df: pd.DataFrame,
    experiment_tracker: experiment_tracking.ExperimentTracker,
) -> pd.DataFrame:
    """Split clusters with multiple records from same report_year.

    DBSCAN will sometimes match records from the same report year, which breaks the
    assumption that there should only be one record for each entity from a single
    report year. To fix this, agglomerative clustering will be applied to each
    such cluster. Agglomerative clustering could replace DBSCAN in the initial linkage
    step to avoid these matches in the first place, however, it is very inneficient on
    a large number of records, so applying to smaller sets of overmerged records is
    much faster and uses much less memory.
    """

    def _generate_cluster_ids(max_cluster_id: int) -> int:
        """Get new unique cluster id."""
        while True:
            max_cluster_id += 1
            yield max_cluster_id

    cluster_id_generator = _generate_cluster_ids(id_year_df.record_label.max())
    classifier = AgglomerativeClustering(
        metric="precomputed",
        linkage="average",
        distance_threshold=config.distance_threshold,
        n_clusters=None,
    )
    duplicated_ids = id_year_df.loc[
        id_year_df.duplicated(subset=["report_year", "record_label"]),
        "record_label",
    ]

    for duplicated_id in duplicated_ids.unique():
        # IDs of -1 will be handled separately
        if duplicated_id == -1:
            continue

        cluster_inds = id_year_df[
            id_year_df.record_label == duplicated_id
        ].index.to_numpy()
        cluster_distances = get_cluster_distance_matrix(
            distance_matrix.distance_matrix, cluster_inds
        )

        new_labels = classifier.fit_predict(cluster_distances)
        for new_label in np.unique(new_labels):
            df_inds = cluster_inds[new_labels == new_label]
            id_year_df.loc[df_inds, "record_label"] = next(cluster_id_generator)

    logger.info(
        f"{id_year_df.record_label.nunique()} unique record IDs found after split clusters step."
    )
    experiment_tracker.execute_logging(
        lambda: mlflow.log_metric(
            "split_clusters_unique", id_year_df.record_label.nunique()
        )
    )
    return id_year_df


class MatchOrphanedRecordsConfig(Config):
    """Configuration for :func:`match_orphaned_records` op."""

    #: See :class:`sklearn.cluster.AgglomerativeClustering` for details.
    distance_threshold: float = 0.5


@op(tags={"memory-use": "high"})
def match_orphaned_records(
    config: MatchOrphanedRecordsConfig,
    distance_matrix: DistanceMatrix,
    id_year_df: pd.DataFrame,
    experiment_tracker: experiment_tracking.ExperimentTracker,
) -> pd.DataFrame:
    """DBSCAN assigns 'noisy' records a label of '-1', which will be labeled by this step.

    To label orphaned records, points are separated into clusters where each orphaned record
    is a cluster of a single point. Then, a distance matrix is computed with the average
    distance between each cluster, and is used in a round of agglomerative clustering.
    This will match orphaned records to existing clusters, or assign them unique ID's if
    they don't appear close enough to any existing clusters.
    """
    classifier = AgglomerativeClustering(
        metric="precomputed",
        linkage="average",
        distance_threshold=config.distance_threshold,
        n_clusters=None,
    )

    cluster_inds = id_year_df.groupby("record_label").indices

    # Orphaned records are considered a cluster of a single record
    cluster_groups = [List([ind]) for ind in cluster_inds.get(-1, [])]

    # Get list of all points in each assigned cluster
    cluster_groups += [List(inds) for key, inds in cluster_inds.items() if key != -1]
    cluster_groups = List(cluster_groups)

    average_dist_matrix = get_average_distance_matrix(
        distance_matrix.distance_matrix, cluster_groups
    )

    # Assign new labels to all points
    new_labels = classifier.fit_predict(average_dist_matrix)
    for inds, label in zip(cluster_groups, new_labels, strict=True):
        id_year_df.loc[inds, "record_label"] = label

    logger.info(
        f"{id_year_df.record_label.nunique()} unique record IDs found after match orphaned records step."
    )
    experiment_tracker.execute_logging(
        lambda: mlflow.log_metric(
            "match_orphaned_unique", id_year_df.record_label.nunique()
        )
    )
    return id_year_df


@graph
def link_ids_cross_year(
    df: pd.DataFrame,
    feature_matrix: FeatureMatrix,
    experiment_tracker: experiment_tracking.ExperimentTracker,
):
    """Apply model and return column of estimated record labels."""
    # Compute distances and apply penalty for records from same year
    distance_matrix = compute_distance_with_year_penalty(feature_matrix, df)

    # Label records
    id_year_df = cluster_records_dbscan(distance_matrix, df, experiment_tracker)
    id_year_df = split_clusters(distance_matrix, id_year_df, experiment_tracker)
    id_year_df = match_orphaned_records(distance_matrix, id_year_df, experiment_tracker)

    return id_year_df
