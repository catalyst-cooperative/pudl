"""CLI for managing raw data inputs to the PUDL data processing pipeline."""

from __future__ import annotations

import pathlib
import sys
from typing import TYPE_CHECKING

import click

from pudl.workspace.datastore import ZenodoDoiSettings

if TYPE_CHECKING:
    from pudl.workspace.datastore import Datastore

_KNOWN_DATASETS = sorted(ZenodoDoiSettings.model_fields)


def _print_partitions(dstore: Datastore, datasets: list[str]) -> None:
    """Print known partition keys and values for each of the datasets."""
    from pudl.workspace.datastore import ZenodoFetcher  # noqa: PLC0415

    for single_ds in datasets:
        partitions = dstore.get_datapackage_descriptor(single_ds).get_partitions()

        print(f"\nPartitions for {single_ds} ({ZenodoFetcher().get_doi(single_ds)}):")
        for partition_key in sorted(partitions):
            # try-except required because ferc2 has parts with heterogenous types
            # that therefore can't be sorted: [1, 2, None]
            try:
                parts = sorted(partitions[partition_key])
            except TypeError:
                parts = partitions[partition_key]
            print(f"  {partition_key}: {', '.join(str(x) for x in parts)}")
        if not partitions:
            print("  -- no known partitions --")


def _parse_key_values(
    ctx: click.core.Context,
    param: click.Option,
    values: str,
) -> dict[str, str]:
    """Parse key-value pairs into a Python dictionary.

    Transforms a command line argument of the form: k1=v1,k2=v2,k3=v3...
    into: {k1:v1, k2:v2, k3:v3, ...}
    """
    out_dict = {}
    for val in values:
        for key_value in val.split(","):
            key, value = key_value.split("=")
            out_dict[key] = value
    return out_dict


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument(
    "datasets",
    nargs=-1,
    type=click.Choice(_KNOWN_DATASETS),
)
@click.option(
    "--all",
    "all_datasets",
    is_flag=True,
    default=False,
    help=(
        "Operate on all known datasets. Mutually exclusive with specifying individual "
        "DATASETS arguments. Useful for automation where maintaining an explicit list "
        "of dataset names would risk getting out of sync with ZenodoDoiSettings."
    ),
)
@click.option(
    "--validate",
    is_flag=True,
    default=False,
    help="Validate the contents of locally cached data, but don't download anything.",
)
@click.option(
    "--list-partitions",
    help="List the available partition keys and values for each specified dataset.",
    is_flag=True,
    default=False,
)
@click.option(
    "--partition",
    "-p",
    multiple=True,
    help=(
        "Only operate on dataset partitions matching these conditions. The argument "
        "should have the form: key1=val1,key2=val2,... Conditions are combined with "
        "a boolean AND, functionally meaning each key can only appear once. "
        "If a key is repeated, only the last value is used. "
        "So state=ca,year=2022 will retrieve all California data for 2022, and "
        "state=ca,year=2021,year=2022 will also retrieve California data for 2022, "
        "while state=ca by itself will retrieve all years of California data."
    ),
    callback=_parse_key_values,
)
@click.option(
    "--bypass-local-cache",
    is_flag=True,
    default=False,
    help=(
        "If enabled, locally cached data will not be used. Instead, a new copy will be "
        "downloaded from Zenodo or the cloud cache if specified."
    ),
)
@click.option(
    "--cloud-cache-path",
    type=str,
    default="s3://pudl.catalyst.coop/zenodo",
    help=(
        "Load cached inputs from cloud object storage (S3 or GCS) . This is typically "
        "much faster and more reliable than downloading from Zenodo directly. By "
        "default we read from the cache in PUDL's free, public AWS Open Data Registry "
        "bucket."
    ),
)
@click.option(
    "--logfile",
    help="If specified, write logs to this file.",
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.option(
    "--loglevel",
    default="INFO",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
)
def main(
    datasets: tuple[str, ...],
    all_datasets: bool,
    validate: bool,
    list_partitions: bool,
    partition: dict[str, int | str],
    cloud_cache_path: str,
    bypass_local_cache: bool,
    logfile: pathlib.Path,
    loglevel: str,
):
    """Manage the raw data inputs to the PUDL data processing pipeline.

    Download the raw FERC Form 2 data:

    pudl_datastore ferc2

    Download the raw FERC Form 2 data only for 2021:

    pudl_datastore ferc2 --partition year=2021

    Re-download the raw FERC Form 2 data for 2021 even if you already have it:

    pudl_datastore ferc2 --partition year=2021 --bypass-local-cache

    Validate all California EPA CEMS data in the local datastore:

    pudl_datastore epacems --validate --partition state=ca

    List the available partitions in the EIA-860 and EIA-923 datasets:

    pudl_datastore eia860 eia923 --list-partitions

    Download all known datasets (e.g. in automation):

    pudl_datastore --all
    """
    # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
    import pudl  # noqa: PLC0415
    from pudl.workspace.datastore import (  # noqa: PLC0415
        Datastore,
        fetch_resources,
        validate_cache,
    )
    from pudl.workspace.setup import PudlPaths  # noqa: PLC0415

    logger = pudl.logging_helpers.get_logger(__name__)
    pudl.logging_helpers.configure_root_logger(
        logfile=str(logfile) if logfile else None, loglevel=loglevel
    )

    if all_datasets and datasets:
        raise click.UsageError("Cannot combine --all with explicit DATASETS arguments.")
    if not all_datasets and not datasets:
        logger.warning("No datasets specified, nothing to do.")
        return 0

    dataset_list = _KNOWN_DATASETS if all_datasets else list(datasets)

    cache_path = None
    if not bypass_local_cache:
        cache_path = PudlPaths().input_dir

    dstore = Datastore(
        cloud_cache_path=cloud_cache_path,
        local_cache_path=cache_path,
    )

    if partition:
        logger.info(f"Only considering resource partitions: {partition}")

    if list_partitions:
        _print_partitions(dstore, dataset_list)
    elif validate:
        validate_cache(dstore, dataset_list, partition)
    else:
        fetch_resources(
            dstore=dstore,
            datasets=dataset_list,
            partition=partition,
            cloud_cache_path=cloud_cache_path,
            bypass_local_cache=bypass_local_cache,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
