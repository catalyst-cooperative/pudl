"""Collection of Dagster resources for PUDL."""

from collections.abc import Callable
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
from dagster import ConfigurableResource, Field, ResourceDependency, resource

from pudl.settings import DatasetsSettings, FercToSqliteSettings, create_dagster_config
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths


class RuntimeSettings(ConfigurableResource):
    """Encodes runtime settings for the ferc_to_sqlite graphs."""

    xbrl_num_workers: None | int = None
    xbrl_batch_size: int = 50


@resource(config_schema=create_dagster_config(DatasetsSettings()))
def dataset_settings(init_context) -> DatasetsSettings:
    """Dagster resource for parameterizing PUDL ETL assets.

    This resource allows us to specify the years we want to process for each datasource
    in the Dagit UI.
    """
    return DatasetsSettings(**init_context.resource_config)


@resource(config_schema=create_dagster_config(FercToSqliteSettings()))
def ferc_to_sqlite_settings(init_context) -> FercToSqliteSettings:
    """Dagster resource for parameterizing the ``ferc_to_sqlite`` graph.

    This resource allows us to specify the years we want to process for each datasource
    in the Dagit UI.
    """
    return FercToSqliteSettings(**init_context.resource_config)


@resource(
    config_schema={
        "gcs_cache_path": Field(
            str,
            description="Load datastore resources from Google Cloud Storage.",
            default_value="",
        ),
        "use_local_cache": Field(
            bool,
            description="If enabled, the local file cache for datastore will be used.",
            default_value=True,
        ),
    },
)
def datastore(init_context) -> Datastore:
    """Dagster resource to interact with Zenodo archives."""
    ds_kwargs = {}
    ds_kwargs["gcs_cache_path"] = init_context.resource_config["gcs_cache_path"]

    if init_context.resource_config["use_local_cache"]:
        # TODO(rousik): we could also just use PudlPaths().input_dir here, because
        # it should be initialized to the right values.
        ds_kwargs["local_cache_path"] = PudlPaths().input_dir
    return Datastore(**ds_kwargs)


class PudlParquetTransformer(ConfigurableResource):
    """Resource to manage using parquet files on disk for performing transforms.

    Tools like duckdb and Polars are very good at using parquet files on
    disk to do fast and memory efficient transforms. This resource provides
    tooling to manage offloading data to parquet files where they can be
    used for this use case. These files will be stored in the
    `${PUDL_WORKSPACE}/storage/parquet` directory, rather than in the
    output directory where parquet files are stored for publication.
    """

    ds: ResourceDependency[Datastore]

    def _get_parquet_dir(self, table_name: str) -> Path:
        """Get path to directory for writing/reading parquet files."""
        parquet_path = PudlPaths().parquet_transform_dir / table_name
        parquet_path.mkdir(exist_ok=True, parents=True)
        return parquet_path

    def offload_table(
        self, df: pd.DataFrame | pl.LazyFrame, table_name: str, **partitions: dict
    ) -> Path:
        """Write data from DataFrame or LazyFrame to disk as a parquet file.

        Offloading data to disk allows Polars and duckdb to perform highly efficient
        transforms.
        """
        parquet_path = self._get_parquet_name(table_name, **partitions)
        if isinstance(df, pd.DataFrame):
            df.to_parquet(parquet_path)
        else:
            df.sink_parquet(parquet_path, engine="streaming")
        return Path(parquet_path)

    def get_df(self, table_name: str, filters: list[str] = []) -> pd.DataFrame:
        """Read data from a set of parquet files and return a pandas DataFrame.

        Args:
            table_name: Name of raw or intermediate table to load.
            filters: A set of filters to apply when reading data.
                See ``pd.DataFrame.read_parquet``.
        """
        return pd.read_parquet(self._get_parquet_dir(table_name), filters=filters)

    def get_lf(self, table_name: str, **partitions) -> pl.LazyFrame:
        """Return LazyFrame pointing to parquet table on disk."""
        if partitions == {}:
            return pl.scan_parquet(self._get_parquet_dir(table_name))
        return pl.scan_parquet(self._get_parquet_name(table_name, **partitions))

    def get_duckdb_table(
        self, conn, table_name: str, alias: str | None = None
    ) -> duckdb.DuckDBPyRelation:
        """Create a duckdb relation to read from parquet files."""
        rel = conn.read_parquet(f"{self._get_parquet_dir(table_name)}/*.parquet")
        if alias is not None:
            rel.set_alias(alias)
        return rel

    def _get_parquet_name(self, table_name: str, **partitions: dict) -> str:
        base_path = self._get_parquet_dir(table_name)
        if partitions is not None:
            filename = "_".join(map(str, partitions.values())) + ".parquet"
        else:
            filename = "monolithic.parquet"
        return str(base_path / filename)

    def extract_csv_to_parquet(
        self,
        dataset: str,
        table_name: str,
        partition_paths: list[tuple[dict, str]],
        add_partition_columns: list[str] | None = None,
        custom_transforms: Callable[[duckdb.DuckDBPyRelation], duckdb.DuckDBPyRelation]
        | None = None,
    ):
        """Extract a set of partitioned CSV files from raw archives to a set of parquet files.

        This method is developed for a common pattern where raw archives which contain
        one zipfile per partition, and each zipfile contains a CSV of data corresponding
        to that partition. This method will loop through these zipfiles, and extract
        the data and write it to a parquet file. This will produce a directory with
        one parquet file per extracted CSV file.

        Args:
            dataset: Name of dataset used to fetch CSV files using DataStore.
            table_name: Name of raw table used to construct path for storing parquet files.
            partition_paths: Set of tuples containing a dictionary of partition
                key value pairs to get a zipfile, and corresponding path
                within the zipfile to get a CSV file.
            add_partition_columns: Partition keys that should be made columns
                in the generated parquet files. The key should correspond
                to a key in the ``partition_paths`` dicts. This is useful
                for things like adding a ``report_year`` column to tables
                partitioned by year.
            custom_transforms: Callable that takes a duckdb relation and returns a duckdb
                relation, allowing for custom transforms to be applied using duckdb
                before writing to parquet.
        """
        # Loop through zipfiles and translate data from CSV to parquet
        for partitions, path in partition_paths:
            with (
                duckdb.connect() as conn,
                self.ds.get_zipfile_resource(dataset=dataset, **partitions) as zf,
                zf.open(path) as csv,
            ):
                rel = conn.read_csv(csv)

                # Add any partition columns if requested
                if add_partition_columns is not None:
                    rel = rel.select(
                        "*, "
                        + ", ".join(
                            [
                                f"{partitions[partition_col]} AS {partition_col}"
                                for partition_col in add_partition_columns
                            ]
                        )
                    )

                # Apply custom transforms
                if custom_transforms is not None:
                    rel = custom_transforms(rel)

                # Write extracted/transformed data to parquet
                rel.to_parquet(self._get_parquet_name(table_name, **partitions))


pudl_parquet_transformer = PudlParquetTransformer(ds=datastore)
