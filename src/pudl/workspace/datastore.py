"""Datastore manages file retrieval for PUDL datasets."""

import argparse
import copy
import hashlib
import json
import logging
import re
import sys
import zipfile
import io
from pathlib import Path
from typing import Dict, Iterator, Optional, Any, List, Tuple, NamedTuple
from abc import ABC, abstractmethod
from urllib.parse import urlparse

import coloredlogs
import datapackage
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from google.cloud.storage.blob import Blob
from google.cloud import storage

logger = logging.getLogger(__name__)


class PudlResourceKey(NamedTuple):
    """Uniquely identifies a specific resource."""
    dataset: str
    doi: str
    name: str

    def __repr__(self) -> str:
        return f'Resource({self.dataset}/{self.doi}/{self.name})'

    def get_local_path(self) -> Path:
        doi_dirname = self.doi.replace("/", "-")
        return Path(self.dataset) / doi_dirname / self.name


PUDL_YML = Path.home() / ".pudl.yml"


class DatapackageDescriptor:
    """A simple wrapper providing access to datapackage.json."""
    """An abstract representation of the datapackage resources."""
    def __init__(self, datapackage_json: dict, dataset: str, doi: str):
        self.datapackage_json = datapackage_json
        self.dataset = dataset
        self.doi = doi 
        self._validate_datapackage(datapackage_json)

    def get_resource_path(self, name: str) -> str:
        """Returns zenodo url that holds contents of given named resource."""
        for res in self.datapackage_json["resources"]:
            if res["name"] == name:
                # remote_url is sometimes set on the local cached version of datapackage.json
                # so we should be using that if it exists.
                return res.get("remote_url") or res.get("path")
        raise KeyError(f"Resource {name} not found for {self.dataset}/{self.doi}")

    def _matches(self, res: dict, **filters: Any):
        parts = res.get('parts', {})
        return all(parts.get(k) == v for k, v in filters.items())

    def get_resources(self, name: str = None, **filters: Any) -> Iterator[PudlResourceKey]:
        for res in self.datapackage_json["resources"]:
            if name and res["name"] != name:
                continue
            if self._matches(res, **filters):
                yield PudlResourceKey(
                    dataset=self.dataset,
                    doi=self.doi,
                    name=res["name"])

    def _validate_datapackage(self, datapackage_json: dict):
        """Checks the correctness of datapackage.json metadata. Throws ValueError if invalid."""
        dp = datapackage.Package(datapackage_json)
        if not dp.valid:
            msg = f"Found {len(dp.errors)} datapackage validation errors:\n"
            for e in dp.errors:
                msg = msg + f"  * {e}\n"
            raise ValueError(msg)

    def get_json_string(self) -> str:
        """Exports the underlying json as normalized (sorted, indented) json string."""
        return json.dumps(self.datapackage_json, sort_keys=True, indent=4)


class ZenodoFetcher:
    """API for fetching datapackage descriptors and resource contents from zenodo."""


    # Zenodo tokens recorded here should have read-only access to our archives.
    # Including them here is correct in order to allow public use of this tool, so
    # long as we stick to read-only keys.
    TOKEN = {
        # Read-only personal access tokens for pudl@catalyst.coop:
        "sandbox": "qyPC29wGPaflUUVAv1oGw99ytwBqwEEdwi4NuUrpwc3xUcEwbmuB4emwysco",
        "production": "KXcG5s9TqeuPh1Ukt5QYbzhCElp9LxuqAuiwdqHP0WS4qGIQiydHn6FBtdJ5"
    }

    DOI = {
        "sandbox": {
            "censusdp1tract": "10.5072/zenodo.674992",
            "eia860": "10.5072/zenodo.672210",
            "eia860m": "10.5072/zenodo.692655",
            "eia861": "10.5072/zenodo.687052",
            "eia923": "10.5072/zenodo.687071",
            "epacems": "10.5072/zenodo.672963",
            "ferc1": "10.5072/zenodo.687072",
            "ferc714": "10.5072/zenodo.672224",
        },
        "production": {
            "censusdp1tract": "10.5281/zenodo.4127049",
            "eia860": "10.5281/zenodo.4127027",
            "eia860m": "10.5281/zenodo.4281337",
            "eia861": "10.5281/zenodo.4127029",
            "eia923": "10.5281/zenodo.4127040",
            "epacems": "10.5281/zenodo.4127055",
            "ferc1": "10.5281/zenodo.4127044",
            "ferc714": "10.5281/zenodo.4127101",
        },
    }
    API_ROOT = {
        "sandbox": "https://sandbox.zenodo.org/api",
        "production": "https://zenodo.org/api",
    }

    def __init__(self, sandbox: bool = False, timeout: float = 15.0):
        backend = "sandbox" if sandbox else "production"
        self._api_root = self.API_ROOT[backend]
        self._token = self.TOKEN[backend]
        self._dataset_to_doi = self.DOI[backend]
        self._descriptor_cache = {}  # type: Dict[str, DatapackageDescriptor]

        self.timeout = timeout
        retries = Retry(backoff_factor=2, total=3,
                        status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)

        self.http = requests.Session()
        self.http.mount("http://", adapter)
        self.http.mount("https://", adapter)

    def _fetch_from_url(self, url: str) -> requests.Response:
        # logger.info(f"Retrieving {url} from zenodo")
        response = self.http.get(
            url,
            params={"access_token": self._token},
            timeout=self.timeout)
        if response.status_code == requests.codes.ok:
            # logger.info(f"Successfully downloaded {url}")
            return response
        else:
            raise ValueError(f"Could not download {url}: {response.text}")

    def _doi_to_url(self, doi: str) -> str:
        """Returns url that holds the datapackage for given doi."""
        match = re.search(r"zenodo.([\d]+)", doi)
        if match is None:
            raise ValueError(f"Invalid doi {doi}")

        zen_id = int(match.groups()[0])
        return f"{self._api_root}/deposit/depositions/{zen_id}"

    def get_descriptor(self, dataset: str) -> DatapackageDescriptor:
        doi = self._dataset_to_doi.get(dataset)
        if not doi:
            raise KeyError(f"No doi found for dataset {dataset}")
        if doi not in self._descriptor_cache:
            dpkg = self._fetch_from_url(self._doi_to_url(doi))
            for f in dpkg.json()["files"]:
                if f["filename"] == "datapackage.json":
                    resp = self._fetch_from_url(f["links"]["download"])
                    self._descriptor_cache[doi] = DatapackageDescriptor(resp.json(), dataset=dataset, doi=doi)
                    break
            else:
                raise RuntimeError(f"Zenodo datapackage for {dataset}/{doi} does not contain valid datapackage.json")
        return self._descriptor_cache[doi]

    def get_resource_key(self, dataset: str, name: str) -> PudlResourceKey:
        """Returns PudlResourceKey for given resource."""
        return PudlResourceKey(dataset, self._dataset_to_doi[dataset], name)

    def get_doi(self, dataset: str) -> str:
        """Returns DOI for given dataset."""
        return self._dataset_to_doi[dataset]

    def get_resource(self, res: PudlResourceKey) -> bytes:
        """Given resource key, retrieve contents of the file from zenodo."""
        url = self.get_descriptor(res.dataset).get_resource_path(res.name)
        return self._fetch_from_url(url).content


class AbstractCache(ABC):
    @abstractmethod
    def get(self, resource: PudlResourceKey) -> bytes:
        """Retrieves content of given resource or throws KeyError."""
        pass

    @abstractmethod
    def set(self, resource: PudlResourceKey, content: bytes) -> None:
        """Adds resource to the cache and sets the content."""
        pass
    
    @abstractmethod
    def delete(self, resource: PudlResourceKey) -> None:
        """Removes the resource from cache."""
        pass

    @abstractmethod
    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if the resource is present in the cache."""
        pass


class LocalFileCache(AbstractCache):
    """Simple key-value store mapping PudlResourceKeys to ByteIO contents."""
    def __init__(self, cache_root_dir: Path):
        self.cache_root_dir = cache_root_dir

    def _resource_path(self, resource: PudlResourceKey) -> Path:
        doi_dirname = re.sub("/", "-", resource.doi)
        return self.cache_root_dir / resource.get_local_path() 

    def get(self, resource: PudlResourceKey) -> bytes:
        return self._resource_path(resource).open("rb").read()

    def set(self, resource: PudlResourceKey, content: bytes):
        path = self._resource_path(resource)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.open("wb").write(content)

    def delete(self, resource: PudlResourceKey):
        self._resource_path(resource).unlink(missing_ok=True)

    def contains(self, resource: PudlResourceKey) -> bool:
        return self._resource_path(resource).exists()


class GoogleCloudStorageCache(AbstractCache):
    """Implements file cache backed by Google Cloud Storage bucket."""

    def __init__(self, gcs_path: str):
        """Constructs new cache that stores files in Google Cloud Storage:

        Args:
            gcs_path (str): path to where the data should be stored. This should
              be in the form of gs://{bucket-name}/{optional-path-prefix}
        """
        parsed_url = urlparse(gcs_path)
        if parsed_url.scheme != "gs":
            raise ValueError(f"gsc_path should start with gs:// (found: {gcs_path})")
        self._path_prefix = Path(parsed_url.path)
        self._bucket = storage.Client().bucket(parsed_url.netloc)

    def _blob(self, resource: PudlResourceKey) -> Blob:
        """Retrieve Blob object associated with given resource."""
        return self._bucket.blob(self._path_prefix / resource.get_local_path())

    def get(self, resource: PudlResourceKey) -> bytes:
        return self._blob(resource).download_as_bytes()

    def set(self, resource: PudlResourceKey, value: bytes):
        return self._blob(resource).upload_from_string(value)

    def delete(self, resource: PudlResourceKey): 
        self._blob(resource).delete()

    def contains(self, resource: PudlResourceKey) -> bool:
        return self._blob(resource).exists()


class LayeredCache(AbstractCache):
    """Implements multi-layered system of caches.

    This allows building multi-layered system of caches. The idea is that you can
    have faster local caches with fall-back to the more remote or expensive caches
    that can be acessed in case of missing content.

    Only the closest layer is being written to (set, delete), while all remaining 
    layers are read-only (get).
    """
    def __init__(self, *caches: List[AbstractCache]):
        """Creates layered cache consisting of given cache layers.

        Args:
            caches: List of caching layers to uses. These are given in the order
              of decreasing priority.
        """
        self._caches = list(*caches) or []  # type: List[AbstractCache]

    def add_cache_layer(self, cache: AbstractCache):
        """Adds caching layer. The priority is below all other."""
        self._caches.append(cache)

    def num_layers(self):
        return len(self._caches)

    def get(self, resource: PudlResourceKey) -> bytes:
        for cache in self._caches:
            if cache.contains(resource):
                return cache.get(resource)
        raise KeyError(f"{resource} not found in the layered cache")

    def set(self, resource: PudlResourceKey, value):
        if self._caches:
            self._caches[0].set(resource, value)

    def delete(self, resource: PudlResourceKey):
        if self._caches:
            self._caches[0].delete(resource)

    def contains(self, resource: PudlResourceKey) -> bool:
        for cache in self._caches:
            if cache.contains(resource):
                return True
        return False


class Datastore:
    """Handle connections and downloading of Zenodo Source archives."""

    def __init__(
        self,
        local_cache_path: Optional[Path] = None,
        gcs_cache_path: Optional[str] = None,
        sandbox: bool = False,
        timeout: float = 15
    ):
    # TODO(rousik): figure out an efficient way to configure datastore caching
        """
        Datastore manages file retrieval for PUDL datasets.

        Args:
            local_cache_path (Path): if provided, LocalFileCache pointed at the data
              subdirectory of this path will be used with this Datastore.
            gcs_cache_path (str): if provided, GoogleCloudStorageCache will be used
              to retrieve data files. The path is expected to have the following
              format: gs://bucket[/path_prefix]
            sandbox (bool): if True, use sandbox zenodo backend when retrieving files,
              otherwise use production. This affects which zenodo servers are contacted
              as well as dois used for each dataset.
            timeout (floaTR): connection timeouts (in seconds) to use when connecting
              to Zenodo servers.

        """
        self._cache = LayeredCache()
        self._datapackage_descriptors = {}  # type: Dict[str, DatapackageDescriptor]

        if local_cache_path:
            self._cache.add_cache_layer(
                LocalFileCache(local_cache_path))
        if gcs_cache_path:
            self._cache.add_cache_layer(GoogleCloudStorageCache(gcs_cache_path))

        self._zenodo_fetcher = ZenodoFetcher(
            sandbox=sandbox,
            timeout=timeout)

    def get_datapackage_descriptor(self, dataset: str) -> DatapackageDescriptor:
        """Fetch datapackage descriptor for given dataset either from cache or from zenodo."""
        doi = self._zenodo_fetcher.get_doi(dataset)
        if doi not in self._datapackage_descriptors:
            res = PudlResourceKey(dataset, doi, "datapackage.json")
            if self._cache.contains(res):
                self._datapackage_descriptors[doi] = DatapackageDescriptor(
                    json.loads(self._cache.get(res).decode('utf-8')),
                    dataset=dataset,
                    doi=doi)
            else:
                desc = self._zenodo_fetcher.get_descriptor(dataset)
                self._datapackage_descriptors[doi] = desc
                self._cache.set(res, bytes(desc.get_json_string(), "utf-8"))
        return self._datapackage_descriptors[doi]

    def get_resources(self, dataset: str, **filters: Any) -> Iterator[Tuple[PudlResourceKey, bytes]]:
        """Return content of the matching resources.

        Args:
            dataset (str): name of the dataset to query
            **filters (key=val): only return resources that match the key-value mapping in their
            metadata["parts"].

        Yields:
            (PudlResourceKey, io.BytesIO) holding content for each matching resource
        """
        desc = self.get_datapackage_descriptor(dataset)
        for res in desc.get_resources(**filters):
            if self._cache.contains(res):
                logger.debug(f"Retrieved {res} from cache.")
                yield (res, self._cache.get(res))
            else:
                logger.debug(f"Retrieved {res} from zenodo.")
                contents = self._zenodo_fetcher.get_resource(res)
                self._cache.set(res, contents)
                yield (res, contents)

    def get_unique_resource(self, dataset: str, **filters: Any) -> bytes:
        """Returns content of a resource assuming there is exactly one that matches."""
        res = self.get_resources(dataset, **filters)
        if not res:
            raise ValueError(f"No resources found for {dataset}: {filters}")

        _, content = next(res)
        try:
            next(res)
        except StopIteration:
            return content
        raise ValueError(f"Multiple resources found for {dataset}: {filters}")

    def get_zipfile_resource(self, dataset: str, **filters: Any) -> zipfile.ZipFile:
        return zipfile.ZipFile(io.BytesIO(self.get_unique_resource(dataset, **filters)))


def parse_command_line():
    """Collect the command line arguments."""
    prod_dois = "\n".join([f"    - {x}" for x in ZenodoFetcher.DOI["production"]])
    sand_dois = "\n".join([f"    - {x}" for x in ZenodoFetcher.DOI["sandbox"]])

    dataset_msg = f"""
Available Production Datasets:
{prod_dois}

Available Sandbox Datasets:
{sand_dois}"""

    parser = argparse.ArgumentParser(
        description="Download and cache ETL source data from Zenodo.",
        epilog=dataset_msg,
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "--dataset",
        help="Download the specified dataset only. See below for available options. "
        "The default is to download all, which may take an hour or more."
        "speed."
    )
    parser.add_argument(
        "--pudl_in",
        help="Override pudl_in directory, defaults to setting in ~/.pudl.yml",
    )
    parser.add_argument(
        "--validate",
        help="Validate locally cached datapackages, but don't download anything.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--sandbox",
        help="Download data from Zenodo sandbox server. For testing purposes only.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    parser.add_argument(
        "--quiet",
        help="Do not send logging messages to stdout.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--populate-gcs-cache",
        default=None,
        help="If specified, upload data resources to this GCS bucket"
        )
    return parser.parse_args()


def main():
    """Cache datasets."""
    args = parse_command_line()

    # logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    logger.setLevel(args.loglevel)
    # if not args.quiet:
    #    logger.addHandler(logging.StreamHandler())

    dataset = args.dataset
    pudl_in = args.pudl_in

    if pudl_in is None:
        with PUDL_YML.open() as f:
            cfg = yaml.safe_load(f)
            pudl_in = Path(cfg["pudl_in"])
    else:
        pudl_in = Path(pudl_in)

    ds = Datastore(
        pudl_in,
        sandbox=args.sandbox
    )

    if dataset is None:
        if args.sandbox:
            datasets = DOI["sandbox"].keys()
        else:
            datasets = DOI["production"].keys()
    else:
        datasets = [dataset]

    if args.populate_gcs_cache:
        ds = Datastore(gcs_cache_path=args.populate_gcs_cache, sandbox=sandbox)

    for selection in datasets:
        if args.validate:
            ds.validate(selection)
        else:
            list(ds.get_resources())

if __name__ == "__main__":
    sys.exit(main())
