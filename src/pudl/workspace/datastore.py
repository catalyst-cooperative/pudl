"""Datastore manages file retrieval for PUDL datasets."""
import argparse
import hashlib
import io
import json
import re
import sys
import zipfile
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self

import datapackage
import requests
from google.auth.exceptions import DefaultCredentialsError
from pydantic import BaseSettings, HttpUrl, constr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pudl
from pudl.workspace import resource_cache
from pudl.workspace.resource_cache import PudlResourceKey
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

PUDL_YML = Path.home() / ".pudl.yml"
ZenodoDoi = constr(regex=r"(10\.5072|10\.5281)/zenodo.([\d]+)")


class ChecksumMismatchError(ValueError):
    """Resource checksum (md5) does not match."""

    pass


class DatapackageDescriptor:
    """A simple wrapper providing access to datapackage.json contents."""

    def __init__(self, datapackage_json: dict, dataset: str, doi: str):
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
        # remote_url is sometimes set on the local cached version of datapackage.json
        # so we should be using that if it exists.
        return res.get("remote_url") or res.get("path")

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
        m = hashlib.md5()  # noqa: S324 MD5 is required by Zenodo
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
        return all(
            str(parts.get(k)).lower() == str(v).lower() for k, v in filters.items()
        )

    def get_resources(
        self: Self, name: str = None, **filters: Any
    ) -> Iterator[PudlResourceKey]:
        """Returns series of PudlResourceKey identifiers for matching resources.

        Args:
          name: if specified, find resource(s) with this name.
          filters (dict): if specified, find resoure(s) matching these key=value constraints.
            The constraints are matched against the 'parts' field of the resource
            entry in the datapackage.json.
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
        dp = datapackage.Package(datapackage_json)
        if not dp.valid:
            msg = f"Found {len(dp.errors)} datapackage validation errors:\n"
            for e in dp.errors:
                msg = msg + f"  * {e}\n"
            raise ValueError(msg)

    def get_json_string(self) -> str:
        """Exports the underlying json as normalized (sorted, indented) json string."""
        return json.dumps(self.datapackage_json, sort_keys=True, indent=4)


class ZenodoDoiSettings(BaseSettings):
    """Digital Object Identifiers pointing to currently used Zenodo archives."""

    # Sandbox DOIs are provided for reference
    censusdp1tract: ZenodoDoi = "10.5281/zenodo.4127049"
    # censusdp1tract: ZenodoDoi = "10.5072/zenodo.674992"
    eia860: ZenodoDoi = "10.5281/zenodo.8164776"
    # eia860: ZenodoDoi = "10.5072/zenodo.1222854"
    eia860m: ZenodoDoi = "10.5281/zenodo.8188017"
    # eia860m: ZenodoDoi = "10.5072/zenodo.1225517"
    eia861: ZenodoDoi = "10.5281/zenodo.8231268"
    # eia861: ZenodoDoi = "10.5072/zenodo.1229930"
    eia923: ZenodoDoi = "10.5281/zenodo.8172818"
    # eia923: ZenodoDoi = "10.5072/zenodo.1217724"
    eia_bulk_elec: ZenodoDoi = "10.5281/zenodo.7067367"
    # eia_bulk_elec: ZenodoDoi = "10.5072/zenodo.1103572"
    epacamd_eia: ZenodoDoi = "10.5281/zenodo.7900974"
    # epacamd_eia: ZenodoDoi = "10.5072/zenodo.1199170"
    epacems: ZenodoDoi = "10.5281/zenodo.8235497"
    # epacems": ZenodoDoi = "10.5072/zenodo.1228519"
    ferc1: ZenodoDoi = "10.5281/zenodo.8326634"
    # ferc1: ZenodoDoi = "10.5072/zenodo.1234455"
    ferc2: ZenodoDoi = "10.5281/zenodo.8006881"
    # ferc2: ZenodoDoi = "10.5072/zenodo.1236695"
    ferc6: ZenodoDoi = "10.5281/zenodo.7130141"
    # ferc6: ZenodoDoi = "10.5072/zenodo.1236703"
    ferc60: ZenodoDoi = "10.5281/zenodo.7130146"
    # ferc60: ZenodoDoi = "10.5072/zenodo.1236694"
    ferc714: ZenodoDoi = "10.5281/zenodo.7139875"
    # ferc714: ZenodoDoi = "10.5072/zenodo.1237565"

    class Config:
        """Pydantic config, reads from .env file."""

        env_prefix = "pudl_zenodo_doi_"
        env_file = ".env"


class ZenodoFetcher:
    """API for fetching datapackage descriptors and resource contents from zenodo."""

    _descriptor_cache: dict[str, DatapackageDescriptor]
    zenodo_dois: ZenodoDoiSettings
    timeout: float
    http: requests.Session

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
        except AttributeError:
            raise AttributeError(f"No Zenodo DOI found for dataset {dataset}.")
        return doi

    def get_known_datasets(self: Self) -> list[str]:
        """Returns list of supported datasets."""
        return [name for name, doi in sorted(self.zenodo_dois)]

    def _get_token(self: Self, url: HttpUrl) -> str:
        """Return the appropriate read-only Zenodo personal access token.

        These tokens are associated with the pudl@catalyst.coop Zenodo account, which
        owns all of the Catalyst raw data archives.
        """
        if "sandbox" in url:
            token = "qyPC29wGPaflUUVAv1oGw99ytwBqwEEdwi4NuUrpwc3xUcEwbmuB4emwysco"  # noqa: S105
        else:
            token = "KXcG5s9TqeuPh1Ukt5QYbzhCElp9LxuqAuiwdqHP0WS4qGIQiydHn6FBtdJ5"  # noqa: S105
        return token

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
        return f"{api_root}/deposit/depositions/{zenodo_id}"

    def _fetch_from_url(self: Self, url: HttpUrl) -> requests.Response:
        logger.info(f"Retrieving {url} from zenodo")
        response = self.http.get(
            url, params={"access_token": self._get_token(url)}, timeout=self.timeout
        )
        if response.status_code == requests.codes.ok:
            logger.debug(f"Successfully downloaded {url}")
            return response
        raise ValueError(f"Could not download {url}: {response.text}")

    def get_descriptor(self: Self, dataset: str) -> DatapackageDescriptor:
        """Returns class:`DatapackageDescriptor` for given dataset."""
        doi = self.get_doi(dataset)
        if doi not in self._descriptor_cache:
            dpkg = self._fetch_from_url(self._get_url(doi))
            for f in dpkg.json()["files"]:
                if f["filename"] == "datapackage.json":
                    resp = self._fetch_from_url(f["links"]["download"])
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
                pass

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
                logger.info(f"Retrieved {res} from cache.")
                contents = self._cache.get(res)
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
        except StopIteration:
            raise KeyError(f"No resources found for {dataset}: {filters}")
        try:
            next(res)
        except StopIteration:
            return content
        raise KeyError(f"Multiple resources found for {dataset}: {filters}")

    def get_zipfile_resource(self, dataset: str, **filters: Any) -> zipfile.ZipFile:
        """Retrieves unique resource and opens it as a ZipFile."""
        return zipfile.ZipFile(io.BytesIO(self.get_unique_resource(dataset, **filters)))

    def get_zipfile_resources(
        self, dataset: str, **filters: Any
    ) -> Iterator[tuple[PudlResourceKey, zipfile.ZipFile]]:
        """Iterates over resources that match filters and opens each as ZipFile."""
        for resource_key, content in self.get_resources(dataset, **filters):
            yield resource_key, zipfile.ZipFile(io.BytesIO(content))

    def get_zipfile_file_names(self, zip_file: zipfile.ZipFile):
        """Given a zipfile, return a list of the file names in it."""
        return zipfile.ZipFile.namelist(zip_file)


class ParseKeyValues(argparse.Action):
    """Transforms k1=v1,k2=v2,...

    into dict(k1=v1, k2=v2, ...).
    """

    def __call__(self, parser, namespace, values, option_string=None):
        """Parses the argument value into dict."""
        d = getattr(namespace, self.dest, {})
        if isinstance(values, str):
            values = [values]
        for val in values:
            for kv in val.split(","):
                k, v = kv.split("=")
            d[k] = v
        setattr(namespace, self.dest, d)


def parse_command_line():
    """Collect the command line arguments."""
    known_datasets = "\n".join(
        [f"    - {x}" for x in ZenodoFetcher().get_known_datasets()]
    )

    dataset_msg = f"""
Available Datasets:
{known_datasets}"""

    parser = argparse.ArgumentParser(
        description="Download and cache ETL source data from Zenodo.",
        epilog=dataset_msg,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--dataset",
        help="Download the specified dataset only. See below for available options. "
        "The default is to download all datasets, which may take hours depending on "
        "network speed.",
    )
    parser.add_argument(
        "--validate",
        help="Validate locally cached datapackages, but don't download anything.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    parser.add_argument(
        "--logfile",
        default=None,
        type=str,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "--quiet",
        help="Do not send logging messages to stdout.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--gcs-cache-path",
        type=str,
        help="""Load datastore resources from Google Cloud Storage. Should be gs://bucket[/path_prefix].
The main zenodo cache bucket is gs://zenodo-cache.catalyst.coop.
If specified without --bypass-local-cache, the local cache will be populated from the GCS cache.
If specified with --bypass-local-cache, the GCS cache will be populated by Zenodo.""",
    )
    parser.add_argument(
        "--bypass-local-cache",
        action="store_true",
        default=False,
        help="""If enabled, the local file cache for datastore will not be used.""",
    )
    parser.add_argument(
        "--partition",
        default={},
        action=ParseKeyValues,
        metavar="KEY=VALUE,...",
        help="Only retrieve resources matching these conditions.",
    )
    parser.add_argument(
        "--list-partitions",
        action="store_true",
        default=False,
        help="List available partition keys and values for each dataset.",
    )

    return parser.parse_args()


def print_partitions(dstore: Datastore, datasets: list[str]) -> None:
    """Prints known partition keys and its values for each of the datasets."""
    for single_ds in datasets:
        parts = dstore.get_datapackage_descriptor(single_ds).get_partitions()

        print(f"\nPartitions for {single_ds} ({dstore.get_doi(single_ds)}):")
        for pkey in sorted(parts):
            print(f'  {pkey}: {", ".join(str(x) for x in sorted(parts[pkey]))}')
        if not parts:
            print("  -- no known partitions --")


def validate_cache(
    dstore: Datastore, datasets: list[str], args: argparse.Namespace
) -> None:
    """Validate elements in the datastore cache.

    Delete invalid entires from cache.
    """
    for single_ds in datasets:
        num_total = 0
        num_invalid = 0
        descriptor = dstore.get_datapackage_descriptor(single_ds)
        for res, content in dstore.get_resources(
            single_ds, cached_only=True, **args.partition
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
    dstore: Datastore, datasets: list[str], args: argparse.Namespace
) -> None:
    """Retrieve all matching resources and store them in the cache."""
    for single_ds in datasets:
        for res, contents in dstore.get_resources(
            single_ds, skip_optimally_cached=True, **args.partition
        ):
            logger.info(f"Retrieved {res}.")
            # If the gcs_cache_path is specified and we don't want
            # to bypass the local cache, populate the local cache.
            if args.gcs_cache_path and not args.bypass_local_cache:
                dstore._cache.add(res, contents)


def main():
    """Cache datasets."""
    args = parse_command_line()

    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    cache_path = None
    if not args.bypass_local_cache:
        cache_path = PudlPaths().input_dir

    dstore = Datastore(
        gcs_cache_path=args.gcs_cache_path,
        local_cache_path=cache_path,
    )

    datasets = [args.dataset] if args.dataset else dstore.get_known_datasets()

    if args.partition:
        logger.info(f"Only retrieving resources for partition: {args.partition}")

    if args.list_partitions:
        print_partitions(dstore, datasets)
    elif args.validate:
        validate_cache(dstore, datasets, args)
    else:
        fetch_resources(dstore, datasets, args)


if __name__ == "__main__":
    sys.exit(main())
