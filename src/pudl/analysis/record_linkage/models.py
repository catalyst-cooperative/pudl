"""Define a record linkage model interface and implement common functionality."""
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal, Union

import numpy as np
import pandas as pd
from dagster import Config, graph, op
from pydantic import Field
from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import IncrementalPCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import pairwise_distances_chunked
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    MinMaxScaler,
    Normalizer,
    OneHotEncoder,
)

import pudl
from pudl.analysis.record_linkage.name_cleaner import CompanyNameCleaner

logger = pudl.logging_helpers.get_logger(__name__)


def _apply_cleaning_func(df, function_key: str = None):
    function_transforms = {
        "null_to_zero": lambda df: df.fillna(value=0.0),
        "null_to_empty_str": lambda df: df.fillna(value=""),
        "fix_int_na": lambda df: pudl.helpers.fix_int_na(df, columns=list(df.columns)),
    }

    return function_transforms[function_key](df)


class TfidfVectorizerConfig(Config):
    """Implement ColumnTransformation for TfidfVectorizer."""

    transformation_type: Literal["string_transform"]
    options: dict = {"analyzer": "char", "ngram_range": (2, 10)}

    def as_transformer(self):
        """Return configured TfidfVectorizer."""
        return TfidfVectorizer(**self.options)


class OneHotEncoderConfig(Config):
    """Implement ColumnTransformation for OneHotEncoder."""

    transformation_type: Literal["categorical_transform"]
    options: dict = {"categories": "auto"}

    def as_transformer(self):
        """Return configured OneHotEncoder."""
        return OneHotEncoder(**self.options)


class MinMaxScalerConfig(Config):
    """Implement ColumnTransformation for MinMaxScaler."""

    transformation_type: Literal["numerical_transform"]

    def as_transformer(self):
        """Return configured MinMaxScalerConfig."""
        return MinMaxScaler()


class NormalizerConfig(Config):
    """Implement ColumnTransformation for Normalizer."""

    transformation_type: Literal["normalize_transform"]

    def as_transformer(self):
        """Return configured NormalizerConfig."""
        return Normalizer()


class CleaningFuncConfig(Config):
    """Implement ColumnTransformation for cleaning functions."""

    transformation_type: Literal["cleaning_function_transform"]
    transform_function: str

    def as_transformer(self):
        """Return configured NormalizerConfig."""
        return FunctionTransformer(
            _apply_cleaning_func, kw_args={"function_key": self.transform_function}
        )


class NameCleanerConfig(Config):
    """Implement ColumnTransformation for CompanyNameCleaner."""

    transformation_type: Literal["name_cleaner_transform"]

    def as_transformer(self):
        """Return configured CompanyNameCleaner."""
        cleaner = CompanyNameCleaner()
        return FunctionTransformer(cleaner.apply_name_cleaning)


class ColumnTransform(Config):
    """Union of all transform classes."""

    transform: Union[  # noqa: UP007
        TfidfVectorizerConfig,
        OneHotEncoderConfig,
        MinMaxScalerConfig,
        NormalizerConfig,
        CleaningFuncConfig,
        NameCleanerConfig,
    ] = Field(discriminator="transformation_type")


def column_transform_from_key(key: str, **options) -> "ColumnTransform":
    """Format column transform config for dagster."""
    return {"transform": {key: options}}


class TransformGrouping(Config):
    """Define a set of transformations to apply to one or more columns."""

    transforms: list[ColumnTransform]
    weight: float = 1.0
    columns: list[str]

    def as_pipeline(self):
        """Return :class:`sklearn.pipeline.Pipeline` with configuration."""
        return Pipeline(
            [
                (
                    config.transform.transformation_type,
                    config.transform.as_transformer(),
                )
                for config in self.transforms
            ]
        )


class EmbedDataFrameTrainConfig(Config):
    """This ModelComponent performs a series of column transformations on a DataFrame.

    Under the hood this uses :class:`sklearn.compose.ColumnTransformer`. As
    configuration it takes as configuration a mapping of column names to a list of
    transformations to apply. Transformations can be specified either by passing an
    instance of a :class:`sklearn.base.BaseEstimator`, or a string to select from
    several common/generic transformers defined by this class. If a string is used, it
    should be one of the following:

    * ``string`` - Applies a TfidfVectorizer to the column.
    * ``category`` - Applies a OneHotEncoder to the column.
    * ``number`` - Applies a MinMaxScaler to the column.
    """

    #: Maps step name to list of transformations.
    transform_steps: dict[str, TransformGrouping]

    def _construct_transformer(self) -> ColumnTransformer:
        """Use configuration to construct :class:`sklearn.compose.ColumnTransformer`."""
        return ColumnTransformer(
            transformers=[
                (name, column_transform.as_pipeline(), column_transform.columns)
                for name, column_transform in self.transform_steps.items()
            ],
            transformer_weights={
                name: column_transform.weight
                for name, column_transform in self.transform_steps.items()
            },
        )


@op
def train_dataframe_embedder(config: EmbedDataFrameTrainConfig, df: pd.DataFrame):
    """Train :class:`sklearn.compose.ColumnTransformer` on input."""
    transformer = config._construct_transformer()
    return transformer.fit(df)


class EmbedDataFrameConfig(Config):
    """Define embed step config."""

    #: Applying column transformations may produce a sparse matrix.
    #: If this flag is set, the matrix will automatically be made dense before returning.
    make_dense: bool = False


@op
def embed_dataframe(config: EmbedDataFrameConfig, df: pd.DataFrame, transformer):
    """Use :class:`sklearn.compose.ColumnTransformer` to transform input."""
    transformed = transformer.fit_transform(df)

    if config.make_dense:
        transformed = np.array(transformed.todense())

    return transformed


class IncrementalPCAConfig(Config):
    """:class:`DataFrameEmbedder` subclass, using IncrementalPCA to reduce dimensions.

    This class differs from :class:`ReducedDimDataFrameEmbedder` in that it applies
    IncrementalPCA instead of a normal PCA implementation. This implementation is
    an approximation of a true PCA, but it operates with constant memory usage of
    batch_size * n_features (where n_features is the number of columns in the input
    matrix) and it can operate on a sparse input matrix.
    """

    #: Passed to :class:`sklearn.decomposition.IncrementalPCA` param n_components
    output_dims: int | None = 500
    batch_size: int = 500


@op
def train_incremental_pca(config: IncrementalPCAConfig, X):  # noqa: N803
    """Apply PCA to output of :class:`DataFrameEmbedder`."""
    pca = IncrementalPCA(
        copy=False, n_components=config.output_dims, batch_size=config.batch_size
    )

    return pca.fit(X)


@op
def apply_incremental_pca(X, pca):  # noqa: N803
    """Apply PCA to output of :class:`DataFrameEmbedder`."""
    return pca.transform(X)


class PenalizeReportYearDistanceConfig(Config):
    """Compute distance between records and add penalty to records from same year."""

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
            self.distance_matrix[
                matching_year_inds[:, 0], matching_year_inds[:, 1]
            ] = config.distance_penalty

        np.fill_diagonal(self.distance_matrix, 0)
        self.distance_matrix.flush()

        # Convert distance matrix to read only memory map
        self.distance_matrix = np.memmap(
            filename,
            dtype="float32",
            mode="r",
            shape=(feature_matrix.shape[0], feature_matrix.shape[0]),
        )

    def get_cluster_distance_matrix(self, cluster_inds: np.ndarray) -> np.ndarray:
        """Return a small distance matrix with distances between points in a cluster."""
        cluster_size = len(cluster_inds)
        dist_inds = np.array(np.meshgrid(cluster_inds, cluster_inds)).T.reshape(-1, 2)
        return self.distance_matrix[dist_inds[:, 0], dist_inds[:, 1]].reshape(
            (cluster_size, cluster_size)
        )

    def average_dist_between_clusters(
        self, set_1: list[int], set_2: list[int]
    ) -> float:
        """Compute average distance between two sets of clusters given indices into the distance matrix."""
        dist_inds = np.array(np.meshgrid(set_1, set_2)).T.reshape(-1, 2)
        dists = self.distance_matrix[dist_inds[:, 0], dist_inds[:, 1]]
        return dists.mean()


@op
def compute_distance_with_year_penalty(
    config: PenalizeReportYearDistanceConfig, feature_matrix, original_df: pd.DataFrame
) -> DistanceMatrix:
    """Create penalty matrix and add to distances."""
    return DistanceMatrix(feature_matrix, original_df, config)


class DBSCANConfig(Config):
    """Configuration for DBSCAN step."""

    #: See :class:`sklearn.cluster.DBSCAN` for details.
    eps: float = 0.1
    min_samples: int = 2


@op
def cluster_records_dbscan(
    config: DBSCANConfig, distance_matrix: DistanceMatrix, original_df: pd.DataFrame
) -> pd.DataFrame:
    """Use dbscan clustering algorithm to classify records."""
    neighbor_computer = NearestNeighbors(radius=config.eps, metric="precomputed")
    neighbor_computer.fit(distance_matrix.distance_matrix)
    neighbor_graph = neighbor_computer.radius_neighbors_graph(mode="distance")

    classifier = DBSCAN(metric="precomputed", eps=config.eps, min_samples=2)
    id_year_df = original_df.loc[:, ["report_year", "plant_name_ferc1"]]
    id_year_df["record_label"] = classifier.fit_predict(neighbor_graph)
    return id_year_df


class SplitClustersConfig(Config):
    """Configuration for AgglomerativeClustering used to split overmerged clusters."""

    #: See :class:`sklearn.cluster.AgglomerativeClustering` for details.
    distance_threshold: float = 0.3


@op
def split_clusters(
    config: SplitClustersConfig,
    distance_matrix: DistanceMatrix,
    id_year_df: pd.DataFrame,
) -> pd.DataFrame:
    """Apply AgglomerativeClustering to all clusters with more than one record from the same year."""

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
        # IDs of -1 will be handled seperately
        if duplicated_id == -1:
            continue

        cluster_inds = id_year_df[
            id_year_df.record_label == duplicated_id
        ].index.to_numpy()
        cluster_distances = distance_matrix.get_cluster_distance_matrix(cluster_inds)

        new_labels = classifier.fit_predict(cluster_distances)
        for new_label in np.unique(new_labels):
            df_inds = cluster_inds[new_labels == new_label]
            id_year_df.loc[df_inds, "record_label"] = next(cluster_id_generator)

    return id_year_df


class MatchOrpahnedRecordsConfig(Config):
    """DBSCAN assigns 'noisy' records a label of '-1', which will be labeled by this step."""

    distance_threshold: float = 0.3


@op
def match_orphaned_records(
    config: MatchOrpahnedRecordsConfig,
    distance_matrix: DistanceMatrix,
    id_year_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compute average distance from orphaned records to existing clusters, and merge."""
    classifier = AgglomerativeClustering(
        metric="precomputed",
        linkage="average",
        distance_threshold=config.distance_threshold,
        n_clusters=None,
    )

    label_inds = id_year_df.groupby("record_label").indices
    label_groups = [[ind] for ind in label_inds[-1]]
    label_groups += [inds for key, inds in label_inds.items() if key != -1]

    # Prepare a reduced distance matrix
    dist_matrix_size = len(label_groups)
    reduced_dist_matrix = np.zeros((dist_matrix_size, dist_matrix_size))
    for i, x_cluster_inds in enumerate(label_groups):
        for j, y_cluster_inds in enumerate(label_groups[:i]):
            reduced_dist_matrix[i, j] = distance_matrix.average_dist_between_clusters(
                x_cluster_inds, y_cluster_inds
            )
            reduced_dist_matrix[j, i] = reduced_dist_matrix[i, j]

    new_labels = classifier.fit_predict(reduced_dist_matrix)
    for inds, label in zip(label_groups, new_labels):
        id_year_df.loc[inds, "record_label"] = label

    return id_year_df


@graph
def link_ids_cross_year(df: pd.DataFrame):
    """Apply model and return column of estimated record labels."""
    # Embed dataframe
    transformer = train_dataframe_embedder(df)
    feature_matrix = embed_dataframe(df, transformer)

    # Compute distances and apply penalty for records from same year
    distance_matrix = compute_distance_with_year_penalty(feature_matrix, df)

    # Label records
    id_year_df = cluster_records_dbscan(distance_matrix, df)
    id_year_df = split_clusters(distance_matrix, id_year_df)
    id_year_df = match_orphaned_records(distance_matrix, id_year_df)

    return id_year_df
