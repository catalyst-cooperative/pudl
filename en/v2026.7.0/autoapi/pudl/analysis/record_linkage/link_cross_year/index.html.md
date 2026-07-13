# pudl.analysis.record_linkage.link_cross_year

Define a record linkage model interface and implement common functionality.

## Attributes

| [`logger`](#pudl.analysis.record_linkage.link_cross_year.logger)   |    |
|--------------------------------------------------------------------|----|

## Classes

| [`PenalizeReportYearDistanceConfig`](#pudl.analysis.record_linkage.link_cross_year.PenalizeReportYearDistanceConfig)   | Compute distance between records and add penalty to records from same year.                                              |
|------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| [`DistanceMatrix`](#pudl.analysis.record_linkage.link_cross_year.DistanceMatrix)                                       | Class to wrap a distance matrix saved in a np.memmap.                                                                    |
| [`DBSCANConfig`](#pudl.analysis.record_linkage.link_cross_year.DBSCANConfig)                                           | Configuration for DBSCAN step.                                                                                           |
| [`SplitClustersConfig`](#pudl.analysis.record_linkage.link_cross_year.SplitClustersConfig)                             | Configuration for AgglomerativeClustering used to split overmerged clusters.                                             |
| [`MatchOrphanedRecordsConfig`](#pudl.analysis.record_linkage.link_cross_year.MatchOrphanedRecordsConfig)               | Configuration for [`match_orphaned_records()`](#pudl.analysis.record_linkage.link_cross_year.match_orphaned_records) op. |

## Functions

| [`get_cluster_distance_matrix`](#pudl.analysis.record_linkage.link_cross_year.get_cluster_distance_matrix)(→ numpy.ndarray)                | Return a distance matrix with only distances within a cluster.                          |
|--------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| [`get_average_distance_matrix`](#pudl.analysis.record_linkage.link_cross_year.get_average_distance_matrix)(→ numpy.ndarray)                | Compute average distance between two clusters of records given indices of each cluster. |
| [`compute_distance_with_year_penalty`](#pudl.analysis.record_linkage.link_cross_year.compute_distance_with_year_penalty)(→ DistanceMatrix) | Compute a distance matrix and penalize records from the same year.                      |
| [`cluster_records_dbscan`](#pudl.analysis.record_linkage.link_cross_year.cluster_records_dbscan)(→ pandas.DataFrame)                       | Generate initial IDs using DBSCAN algorithm.                                            |
| [`split_clusters`](#pudl.analysis.record_linkage.link_cross_year.split_clusters)(→ pandas.DataFrame)                                       | Split clusters with multiple records from same report_year.                             |
| [`match_orphaned_records`](#pudl.analysis.record_linkage.link_cross_year.match_orphaned_records)(→ pandas.DataFrame)                       | DBSCAN assigns 'noisy' records a label of '-1', which will be labeled by this step.     |
| [`link_ids_cross_year`](#pudl.analysis.record_linkage.link_cross_year.link_ids_cross_year)(df, feature_matrix, experiment_tracker)         | Apply model and return column of estimated record labels.                               |

## Module Contents

### pudl.analysis.record_linkage.link_cross_year.logger

### *class* pudl.analysis.record_linkage.link_cross_year.PenalizeReportYearDistanceConfig(\*\*config_dict)

Bases: [`dagster.Config`](https://docs.dagster.io/api/dagster/config/#dagster.Config)

Compute distance between records and add penalty to records from same year.

The metric can be any string accepted by [`scipy.spatial.distance.pdist()`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.pdist.html#scipy.spatial.distance.pdist), e.g.
`cosine` or `euclidean`.

#### distance_penalty *: [float](https://docs.python.org/3/library/functions.html#float)* *= 10000.0*

#### metric *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'euclidean'*

### *class* pudl.analysis.record_linkage.link_cross_year.DistanceMatrix(feature_matrix: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), original_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), config: [PenalizeReportYearDistanceConfig](#pudl.analysis.record_linkage.link_cross_year.PenalizeReportYearDistanceConfig))

Class to wrap a distance matrix saved in a np.memmap.

#### file_buffer

#### distance_matrix

### pudl.analysis.record_linkage.link_cross_year.get_cluster_distance_matrix(distance_matrix: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), cluster_inds: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Return a distance matrix with only distances within a cluster.

### pudl.analysis.record_linkage.link_cross_year.get_average_distance_matrix(distance_matrix: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), cluster_groups: [list](https://docs.python.org/3/library/stdtypes.html#list)[[list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]]) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Compute average distance between two clusters of records given indices of each cluster.

### pudl.analysis.record_linkage.link_cross_year.compute_distance_with_year_penalty(config: [PenalizeReportYearDistanceConfig](#pudl.analysis.record_linkage.link_cross_year.PenalizeReportYearDistanceConfig), feature_matrix: [pudl.analysis.record_linkage.embed_dataframe.FeatureMatrix](../embed_dataframe/index.md#pudl.analysis.record_linkage.embed_dataframe.FeatureMatrix), original_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [DistanceMatrix](#pudl.analysis.record_linkage.link_cross_year.DistanceMatrix)

Compute a distance matrix and penalize records from the same year.

### *class* pudl.analysis.record_linkage.link_cross_year.DBSCANConfig(\*\*config_dict)

Bases: [`dagster.Config`](https://docs.dagster.io/api/dagster/config/#dagster.Config)

Configuration for DBSCAN step.

#### eps *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.5*

#### min_samples *: [int](https://docs.python.org/3/library/functions.html#int)* *= 1*

### pudl.analysis.record_linkage.link_cross_year.cluster_records_dbscan(config: [DBSCANConfig](#pudl.analysis.record_linkage.link_cross_year.DBSCANConfig), distance_matrix: [DistanceMatrix](#pudl.analysis.record_linkage.link_cross_year.DistanceMatrix), original_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Generate initial IDs using DBSCAN algorithm.

### *class* pudl.analysis.record_linkage.link_cross_year.SplitClustersConfig(\*\*config_dict)

Bases: [`dagster.Config`](https://docs.dagster.io/api/dagster/config/#dagster.Config)

Configuration for AgglomerativeClustering used to split overmerged clusters.

#### distance_threshold *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.5*

### pudl.analysis.record_linkage.link_cross_year.split_clusters(config: [SplitClustersConfig](#pudl.analysis.record_linkage.link_cross_year.SplitClustersConfig), distance_matrix: [DistanceMatrix](#pudl.analysis.record_linkage.link_cross_year.DistanceMatrix), id_year_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Split clusters with multiple records from same report_year.

DBSCAN will sometimes match records from the same report year, which breaks the
assumption that there should only be one record for each entity from a single
report year. To fix this, agglomerative clustering will be applied to each
such cluster. Agglomerative clustering could replace DBSCAN in the initial linkage
step to avoid these matches in the first place, however, it is very inneficient on
a large number of records, so applying to smaller sets of overmerged records is
much faster and uses much less memory.

### *class* pudl.analysis.record_linkage.link_cross_year.MatchOrphanedRecordsConfig(\*\*config_dict)

Bases: [`dagster.Config`](https://docs.dagster.io/api/dagster/config/#dagster.Config)

Configuration for [`match_orphaned_records()`](#pudl.analysis.record_linkage.link_cross_year.match_orphaned_records) op.

#### distance_threshold *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.5*

### pudl.analysis.record_linkage.link_cross_year.match_orphaned_records(config: [MatchOrphanedRecordsConfig](#pudl.analysis.record_linkage.link_cross_year.MatchOrphanedRecordsConfig), distance_matrix: [DistanceMatrix](#pudl.analysis.record_linkage.link_cross_year.DistanceMatrix), id_year_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

DBSCAN assigns ‘noisy’ records a label of ‘-1’, which will be labeled by this step.

To label orphaned records, points are separated into clusters where each orphaned record
is a cluster of a single point. Then, a distance matrix is computed with the average
distance between each cluster, and is used in a round of agglomerative clustering.
This will match orphaned records to existing clusters, or assign them unique ID’s if
they don’t appear close enough to any existing clusters.

### pudl.analysis.record_linkage.link_cross_year.link_ids_cross_year(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), feature_matrix: [pudl.analysis.record_linkage.embed_dataframe.FeatureMatrix](../embed_dataframe/index.md#pudl.analysis.record_linkage.embed_dataframe.FeatureMatrix), experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker))

Apply model and return column of estimated record labels.
