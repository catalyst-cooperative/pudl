"""Datastore manages file retrieval for PUDL datasets."""

import hashlib
import io
import json
import pathlib
import re
import sys
import zipfile
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any, Self
from urllib.parse import ParseResult, urlparse

import click
import frictionless
import requests
from google.auth.exceptions import DefaultCredentialsError
from pydantic import HttpUrl, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pudl
from pudl.helpers import retry
from pudl.workspace import resource_cache
from pudl.workspace.resource_cache import PudlResourceKey
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

ZenodoDoi = Annotated[
    str,
    StringConstraints(
        strict=True, min_length=16, pattern=r"(10\.5072|10\.5281)/zenodo.([\d]+)"
    ),
]


class ChecksumMismatchError(ValueError):
    """Resource checksum (md5) does not match."""


class DatapackageDescriptor:
    """A simple wrapper providing access to datapackage.json contents."""

    def __init__(self, datapackage_json: dict, dataset: str, doi: ZenodoDoi):
        """Constructs DatapackageDescriptor.

        Args:
          datapackage_json: parsed datapackage.json describing this datapackage.
          dataset: The name (an identifying string) of the dataset.
          doi: A versioned Digital Object Identifier for the dataset.
        """
        self.datapackage_json = datapackage_json
        self.dataset = dataset
        self.doi = doi
        self._validate_datapackage(datapackage_json)

    def get_resource_path(self, name: str) -> str:
        """Returns zenodo url that holds contents of given named resource."""
        res = self._get_resource_metadata(name)
        # In older cached archives, "remote_url" was used to refer to the original path
        # to the file, while the canonical "path" field was updated by the datastore
        # to refer to the local path to the associated file relative to the location of
        # datapackage.json. This behavior is deprecated and no longer used, but we need
        # to retain this logic to support older cached archives, e.g. censusdp1tract
        # which hasn't changed since 2020.
        resource_path = res.get("remote_url") or res.get("path")
        parsed_path = urlparse(resource_path)
        if parsed_path.path.startswith("/api/files"):
            record_number = self.doi.lower().rsplit("zenodo.", 1)[-1]
            new_path = f"/records/{record_number}/files/{name}"
            new_url = ParseResult(**(parsed_path._asdict() | {"path": new_path}))

            return new_url.geturl()
        return resource_path

    def _get_resource_metadata(self, name: str) -> dict:
        for res in self.datapackage_json["resources"]:
            if res["name"] == name:
                return res
        raise KeyError(f"Resource {name} not found for {self.dataset}/{self.doi}")

    def get_download_size(self) -> int:
        """Returns the total download size of all the resources in MB."""
        total_bytes = 0
        for res in self.datapackage_json["resources"]:
            total_bytes += res["bytes"]
        return int(total_bytes / 1000000)

    def validate_checksum(self, name: str, content: str) -> bool:
        """Returns True if content matches checksum for given named resource."""
        expected_checksum = self._get_resource_metadata(name)["hash"]
        m = hashlib.md5()  # noqa: S324 Unfortunately md5 is required by Zenodo
        m.update(content)
        if m.hexdigest() != expected_checksum:
            raise ChecksumMismatchError(
                f"Checksum for resource {name} does not match."
                f"Expected {expected_checksum}, got {m.hexdigest()}"
            )

    def _matches(self, res: dict, **filters: Any):
        for k, v in filters.items():
            if str(v) != str(v).lower():
                logger.warning(
                    f"Resource filter values should be all lowercase: {k}={v}"
                )
        parts = res.get("parts", {})
        # Each selected file should match for all partitions specified.
        matches = [self._match_from_partition(parts, k, v) for k, v in filters.items()]
        return all(matches)

    def _match_from_partition(
        self, parts: dict[str, str], k: str, v: str | list[str, str]
    ):
        if isinstance(
            parts.get(k), list
        ):  # If partitions are list, match whole list if it contains desired element
            return any(str(part).lower() == str(v).lower() for part in parts.get(k))
        return str(parts.get(k)).lower() == str(v).lower()

    def get_resources(
        self: Self, name: str = None, **filters: Any
    ) -> Iterator[PudlResourceKey]:
        """Returns series of PudlResourceKey identifiers for matching resources.

        Args:
            name: if specified, find resource(s) with this name.
            filters (dict): if specified, find resource(s) matching these key=value
                constraints. The constraints are matched against the 'parts' field of
                the resource entry in the datapackage.json.
        """
        for res in self.datapackage_json["resources"]:
            if name and res["name"] != name:
                continue
            if self._matches(res, **filters):
                yield PudlResourceKey(
                    dataset=self.dataset, doi=self.doi, name=res["name"]
                )

    def get_partitions(self, name: str = None) -> dict[str, set[str]]:
        """Return mapping of known partition keys to their allowed known values."""
        partitions: dict[str, set[str]] = defaultdict(set)
        for res in self.datapackage_json["resources"]:
            if name and res["name"] != name:
                continue
            for k, v in res.get("parts", {}).items():
                if isinstance(v, list):
                    partitions[k] |= set(v)  # Add all items from list
                else:
                    partitions[k].add(v)
        return partitions

    def get_partition_filters(self, **filters: Any) -> Iterator[dict[str, str]]:
        """Returns list of all known partition mappings.

        This can be used to iterate over all resources as the mappings can be directly
        used as filters and should map to unique resource.

        Args:
            filters: additional constraints for selecting relevant partitions.
        """
        for res in self.datapackage_json["resources"]:
            if self._matches(res, **filters):
                yield dict(res.get("parts", {}))

    def _validate_datapackage(self, datapackage_json: dict):
        """Checks the correctness of datapackage.json metadata.

        Throws ValueError if invalid.
        """
        report = frictionless.Package.validate_descriptor(datapackage_json)
        if not report.valid:
            msg = f"Found {len(report.errors)} datapackage validation errors:\n"
            for e in report.errors:
                msg = msg + f"  * {e}\n"
            raise ValueError(msg)

    def get_json_string(self) -> str:
        """Exports the underlying json as normalized (sorted, indented) json string."""
        return json.dumps(self.datapackage_json, sort_keys=True, indent=4)


class ZenodoDoiSettings(BaseSettings):
    """Digital Object Identifiers pointing to currently used Zenodo archives."""

    censusdp1tract: ZenodoDoi = "10.5281/zenodo.4127049"
    censuspep: ZenodoDoi = "10.5281/zenodo.14648211"
    eia176: ZenodoDoi = "10.5281/zenodo.14589676"
    eia191: ZenodoDoi = "10.5281/zenodo.10607837"
    eia757a: ZenodoDoi = "10.5281/zenodo.10607839"
    eia860: ZenodoDoi = "10.5281/zenodo.15629904"
    eia860m: ZenodoDoi = "10.5281/zenodo.15516497"
    eia861: ZenodoDoi = "10.5281/zenodo.13907096"
    eia923: ZenodoDoi = "10.5281/zenodo.15680893"
    eia930: ZenodoDoi = "10.5281/zenodo.15341001"
    eiawater: ZenodoDoi = "10.5281/zenodo.10806016"
    eiaaeo: ZenodoDoi = "10.5281/zenodo.10838488"
    eiaapi: ZenodoDoi = "10.5281/zenodo.15351327"
    epacamd_eia: ZenodoDoi = "10.5281/zenodo.14834878"
    epacems: ZenodoDoi = "10.5281/zenodo.15340994"
    ferc1: ZenodoDoi = "10.5281/zenodo.15294981"
    ferc2: ZenodoDoi = "10.5281/zenodo.15315314"
    ferc6: ZenodoDoi = "10.5281/zenodo.15340979"
    ferc60: ZenodoDoi = "10.5281/zenodo.15340980"
    ferc714: ZenodoDoi = "10.5281/zenodo.13149091"
    gridpathratoolkit: ZenodoDoi = "10.5281/zenodo.10892394"
    nrelatb: ZenodoDoi = "10.5281/zenodo.12658647"
    phmsagas: ZenodoDoi = "10.5281/zenodo.10493790"
    sec10k: ZenodoDoi = "10.5281/zenodo.15161694"
    vcerare: ZenodoDoi = "10.5281/zenodo.15166129"

    model_config = SettingsConfigDict(
        env_prefix="pudl_zenodo_doi_", env_file=".env", extra="ignore"
    )


class ZenodoFetcher:
    """API for fetching datapackage descriptors and resource contents from zenodo."""

    _descriptor_cache: dict[str, DatapackageDescriptor]
    zenodo_dois: ZenodoDoiSettings
    timeout: float

    def __init__(
        self: Self, zenodo_dois: ZenodoDoiSettings | None = None, timeout: float = 15.0
    ):
        """Constructs ZenodoFetcher instance."""
        if not zenodo_dois:
            self.zenodo_dois = ZenodoDoiSettings()

        self.timeout = timeout

        retries = Retry(
            backoff_factor=2, total=3, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.http = requests.Session()
        self.http.mount("http://", adapter)
        self.http.mount("https://", adapter)
        self._descriptor_cache = {}

    def get_doi(self: Self, dataset: str) -> ZenodoDoi:
        """Returns DOI for given dataset."""
        try:
            doi = self.zenodo_dois.__getattribute__(dataset)
        except AttributeError as err:
            raise AttributeError(f"No Zenodo DOI found for dataset {dataset}.") from err
        return doi

    def get_known_datasets(self: Self) -> list[str]:
        """Returns list of supported datasets."""
        return [name for name, doi in sorted(self.zenodo_dois)]

    def _get_url(self: Self, doi: ZenodoDoi) -> HttpUrl:
        """Construct a Zenodo depsition URL based on its Zenodo DOI."""
        match = re.search(r"(10\.5072|10\.5281)/zenodo.([\d]+)", doi)

        if match is None:
            raise ValueError(f"Invalid Zenodo DOI: {doi}")

        doi_prefix = match.groups()[0]
        zenodo_id = match.groups()[1]
        if doi_prefix == "10.5072":
            api_root = "https://sandbox.zenodo.org/api"
        elif doi_prefix == "10.5281":
            api_root = "https://zenodo.org/api"
        else:
            raise ValueError(f"Invalid Zenodo DOI: {doi}")
        return f"{api_root}/records/{zenodo_id}/files"

    def _fetch_from_url(self: Self, url: HttpUrl) -> requests.Response:
        logger.info(f"Retrieving {url} from zenodo")
        response = self.http.get(url, timeout=self.timeout)
        if response.status_code == requests.codes.ok:
            logger.debug(f"Successfully downloaded {url}")
            return response
        raise ValueError(f"Could not download {url}: {response.text}")

    def get_descriptor(self: Self, dataset: str) -> DatapackageDescriptor:
        """Returns class:`DatapackageDescriptor` for given dataset."""
        doi = self.get_doi(dataset)
        if doi not in self._descriptor_cache:
            dpkg = self._fetch_from_url(self._get_url(doi))
            for f in dpkg.json()["entries"]:
                if f["key"] == "datapackage.json":
                    resp = self._fetch_from_url(f["links"]["content"])
                    self._descriptor_cache[doi] = DatapackageDescriptor(
                        resp.json(), dataset=dataset, doi=doi
                    )
                    break
            else:
                raise RuntimeError(
                    f"Zenodo datapackage for {dataset}/{doi} does not contain valid datapackage.json"
                )
        return self._descriptor_cache[doi]

    def get_resource(self: Self, res: PudlResourceKey) -> bytes:
        """Given resource key, retrieve contents of the file from zenodo."""
        desc = self.get_descriptor(res.dataset)
        url = desc.get_resource_path(res.name)
        content = self._fetch_from_url(url).content
        desc.validate_checksum(res.name, content)
        return content


class Datastore:
    """Handle connections and downloading of Zenodo Source archives."""

    def __init__(
        self,
        local_cache_path: Path | None = None,
        gcs_cache_path: str | None = None,
        timeout: float = 15.0,
    ):
        # TODO(rousik): figure out an efficient way to configure datastore caching
        """Datastore manages file retrieval for PUDL datasets.

        Args:
            local_cache_path: if provided, LocalFileCache pointed at the data
                subdirectory of this path will be used with this Datastore.
            gcs_cache_path: if provided, GoogleCloudStorageCache will be used
                to retrieve data files. The path is expected to have the following
                format: gs://bucket[/path_prefix]
            timeout: connection timeouts (in seconds) to use when connecting
                to Zenodo servers.
        """
        self._cache = resource_cache.LayeredCache()
        self._datapackage_descriptors: dict[str, DatapackageDescriptor] = {}

        if local_cache_path:
            logger.info(f"Adding local cache layer at {local_cache_path}")
            self._cache.add_cache_layer(resource_cache.LocalFileCache(local_cache_path))
        if gcs_cache_path:
            try:
                logger.info(f"Adding GCS cache layer at {gcs_cache_path}")
                self._cache.add_cache_layer(
                    resource_cache.GoogleCloudStorageCache(gcs_cache_path)
                )
            except (DefaultCredentialsError, OSError) as e:
                logger.info(
                    f"Unable to obtain credentials for GCS Cache at {gcs_cache_path}. "
                    f"Falling back to Zenodo if necessary. Error was: {e}"
                )

        self._zenodo_fetcher = ZenodoFetcher(timeout=timeout)

    def get_known_datasets(self) -> list[str]:
        """Returns list of supported datasets."""
        return self._zenodo_fetcher.get_known_datasets()

    def get_datapackage_descriptor(self, dataset: str) -> DatapackageDescriptor:
        """Fetch datapackage descriptor for dataset either from cache or Zenodo."""
        doi = self._zenodo_fetcher.get_doi(dataset)
        if doi not in self._datapackage_descriptors:
            res = PudlResourceKey(dataset, doi, "datapackage.json")
            if self._cache.contains(res):
                self._datapackage_descriptors[doi] = DatapackageDescriptor(
                    json.loads(self._cache.get(res).decode("utf-8")),
                    dataset=dataset,
                    doi=doi,
                )
            else:
                desc = self._zenodo_fetcher.get_descriptor(dataset)
                self._datapackage_descriptors[doi] = desc
                self._cache.add(res, bytes(desc.get_json_string(), "utf-8"))
        return self._datapackage_descriptors[doi]

    def get_resources(
        self,
        dataset: str,
        cached_only: bool = False,
        skip_optimally_cached: bool = False,
        **filters: Any,
    ) -> Iterator[tuple[PudlResourceKey, bytes]]:
        """Return content of the matching resources.

        Args:
            dataset: name of the dataset to query.
            cached_only: if True, only retrieve resources that are present in the cache.
            skip_optimally_cached: if True, only retrieve resources that are not optimally
                cached. This triggers attempt to optimally cache these resources.
            filters (key=val): only return resources that match the key-value mapping in their
            metadata["parts"].

        Yields:
            (PudlResourceKey, io.BytesIO) holding content for each matching resource
        """
        desc = self.get_datapackage_descriptor(dataset)
        for res in desc.get_resources(**filters):
            if self._cache.is_optimally_cached(res) and skip_optimally_cached:
                logger.info(f"{res} is already optimally cached.")
                continue
            if self._cache.contains(res):
                contents = self._cache.get(res)
                logger.info(f"Retrieved {res} from cache.")
                if not self._cache.is_optimally_cached(res):
                    logger.info(f"{res} was not optimally cached yet, adding.")
                    self._cache.add(res, contents)
                yield (res, contents)
            elif not cached_only:
                logger.info(f"Retrieved {res} from zenodo.")
                contents = self._zenodo_fetcher.get_resource(res)
                self._cache.add(res, contents)
                yield (res, contents)

    def remove_from_cache(self, res: PudlResourceKey) -> None:
        """Remove given resource from the associated cache."""
        self._cache.delete(res)

    def get_unique_resource(self, dataset: str, **filters: Any) -> bytes:
        """Returns content of a resource assuming there is exactly one that matches."""
        res = self.get_resources(dataset, **filters)
        try:
            _, content = next(res)
        except StopIteration as err:
            raise KeyError(f"No resources found for {dataset}: {filters}") from err
        try:
            next(res)
        except StopIteration:
            return content
        raise KeyError(f"Multiple resources found for {dataset}: {filters}")

    def get_zipfile_resource(self, dataset: str, **filters: Any) -> zipfile.ZipFile:
        """Retrieves unique resource and opens it as a ZipFile."""
        resource_bytes = self.get_unique_resource(dataset, **filters)
        resource = io.BytesIO(resource_bytes)
        md5sum = hashlib.file_digest(resource, "md5").hexdigest()
        logger.info(
            f"Got resource {dataset=}, {filters=}, {md5sum=}, "
            f"{len(resource_bytes)} bytes; turning into ZipFile"
        )
        return retry(zipfile.ZipFile, retry_on=(zipfile.BadZipFile), file=resource)

    def get_zipfile_resources(
        self, dataset: str, **filters: Any
    ) -> Iterator[tuple[PudlResourceKey, zipfile.ZipFile]]:
        """Iterates over resources that match filters and opens each as ZipFile."""
        for resource_key, content in self.get_resources(dataset, **filters):
            yield (
                resource_key,
                retry(zipfile.ZipFile, retry_on=(zipfile.BadZipFile), file=content),
            )

    def get_zipfile_file_names(self, zip_file: zipfile.ZipFile):
        """Given a zipfile, return a list of the file names in it."""
        return zipfile.ZipFile.namelist(zip_file)


def print_partitions(dstore: Datastore, datasets: list[str]) -> None:
    """Prints known partition keys and its values for each of the datasets."""
    for single_ds in datasets:
        partitions = dstore.get_datapackage_descriptor(single_ds).get_partitions()

        print(f"\nPartitions for {single_ds} ({ZenodoFetcher().get_doi(single_ds)}):")
        for partition_key in sorted(partitions):
            # try-except required because ferc2 has parts with heterogenous types that
            # therefore can't be sorted: [1, 2, None]
            try:
                parts = sorted(partitions[partition_key])
            except TypeError:
                parts = partitions[partition_key]
            print(f"  {partition_key}: {', '.join(str(x) for x in parts)}")
        if not partitions:
            print("  -- no known partitions --")


def validate_cache(
    dstore: Datastore, datasets: list[str], partition: dict[str, str]
) -> None:
    """Validate elements in the datastore cache.

    Delete invalid entries from cache.
    """
    for single_ds in datasets:
        num_total = 0
        num_invalid = 0
        descriptor = dstore.get_datapackage_descriptor(single_ds)
        for res, content in dstore.get_resources(
            single_ds, cached_only=True, **partition
        ):
            try:
                num_total += 1
                descriptor.validate_checksum(res.name, content)
            except ChecksumMismatchError:
                num_invalid += 1
                logger.warning(
                    f"Resource {res} has invalid checksum. Removing from cache."
                )
                dstore.remove_from_cache(res)
        logger.info(
            f"Checked {num_total} resources for {single_ds}. Removed {num_invalid}."
        )


def fetch_resources(
    dstore: Datastore,
    datasets: list[str],
    partition: dict[str, int | str],
    gcs_cache_path: str,
    bypass_local_cache: bool,
) -> None:
    """Retrieve all matching resources and store them in the cache."""
    for single_ds in datasets:
        for res, contents in dstore.get_resources(
            single_ds, skip_optimally_cached=True, **partition
        ):
            logger.info(f"Retrieved {res}.")
            # If the gcs_cache_path is specified and we don't want
            # to bypass the local cache, populate the local cache.
            if gcs_cache_path and not bypass_local_cache:
                dstore._cache.add(res, contents)


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
@click.option(
    "--dataset",
    "-d",
    type=click.Choice(ZenodoFetcher().get_known_datasets()),
    default=list(ZenodoFetcher().get_known_datasets()),
    multiple=True,
    help=(
        "Specifies what dataset to work with. The default is to download all datasets. "
        "Note that downloading all datasets may take hours depending on network speed. "
        "This option may be applied multiple times to specify multiple datasets."
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
    help=(
        "List the available partition keys and values for each dataset specified "
        "using the --dataset argument, or all datasets if --dataset is not used."
    ),
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
        "downloaded from Zenodo or the GCS cache if specified."
    ),
)
@click.option(
    "--gcs-cache-path",
    type=str,
    help=(
        "Load cached inputs from Google Cloud Storage if possible. This is usually "
        "much faster and more reliable than downloading from Zenodo directly. The "
        "path should be a URL of the form gs://bucket[/path_prefix]. Internally we use "
        "gs://internal-zenodo-cache.catalyst.coop. A public cache is available at "
        "gs://zenodo-cache.catalyst.coop but requires GCS authentication and a billing "
        "project to pay data egress costs."
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
def pudl_datastore(
    dataset: list[str],
    validate: bool,
    list_partitions: bool,
    partition: dict[str, int | str],
    gcs_cache_path: str,
    bypass_local_cache: bool,
    logfile: pathlib.Path,
    loglevel: str,
):
    """Manage the raw data inputs to the PUDL data processing pipeline.

    Download all the raw FERC Form 2 data:

    pudl_datastore --dataset ferc2

    Download the raw FERC Form 2 data only for 2021

    pudl_datastore --dataset ferc2 --partition year=2021

    Re-download the raw FERC Form 2 data for 2021 even if you already have it:

    pudl_datastore --dataset ferc2 --partition year=2021 --bypass-local-cache

    Validate all California EPA CEMS data in the local datastore:

    pudl_datastore --dataset epacems --validate --partition state=ca

    List the available partitions in the EIA-860 and EIA-923 datasets:

    pudl_datastore --dataset eia860 --dataset eia923 --list-partitions
    """
    pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)

    cache_path = None
    if not bypass_local_cache:
        cache_path = PudlPaths().input_dir

    dstore = Datastore(
        gcs_cache_path=gcs_cache_path,
        local_cache_path=cache_path,
    )

    if partition:
        logger.info(f"Only considering resource partitions: {partition}")

    if list_partitions:
        print_partitions(dstore, dataset)
    elif validate:
        validate_cache(dstore, dataset, partition)
    else:
        fetch_resources(
            dstore=dstore,
            datasets=dataset,
            partition=partition,
            gcs_cache_path=gcs_cache_path,
            bypass_local_cache=bypass_local_cache,
        )

    return 0


if __name__ == "__main__":
    sys.exit(pudl_datastore())
