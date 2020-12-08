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

import coloredlogs
import datapackage
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from google.cloud.storage.blob import Blob
from google.cloud import storage

logger = logging.getLogger(__name__)

# The Zenodo tokens recorded here should have read-only access to our archives.
# Including them here is correct in order to allow public use of this tool, so
# long as we stick to read-only keys.



class PudlResourceKey(NamedTuple):
    """Uniquely identifies a specific resource."""
    dataset: str
    doi: str
    name: str

    def __repr__(self) -> str:
        return f'Resource({dataset}/{doi}/{name})'


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

    def get_resources(self, **filters: Any) -> Iterator[PudlResourceKey]:
        for res in self.datapackage_json["resources"]:
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

    TOKEN = {
        # Read-only personal access tokens for pudl@catalyst.coop:
        "sandbox": "qyPC29wGPaflUUVAv1oGw99ytwBqwEEdwi4NuUrpwc3xUcEwbmuB4emwysco",
        "production": "KXcG5s9TqeuPh1Ukt5QYbzhCElp9LxuqAuiwdqHP0WS4qGIQiydHn6FBtdJ5"
    }

    DOI = {
        "sandbox": {
            "censusdp1tract": "10.5072/zenodo.674992",
            "eia860": "10.5072/zenodo.672210",
            "eia861": "10.5072/zenodo.687052",
            "eia923": "10.5072/zenodo.687071",
            "epacems": "10.5072/zenodo.672963",
            "ferc1": "10.5072/zenodo.687072",
            "ferc714": "10.5072/zenodo.672224",
        },
        "production": {
            "censusdp1tract": "10.5281/zenodo.4127049",
            "eia860": "10.5281/zenodo.4127027",
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

    def __init__(self, sandbox: bool = False, timeout: int = 15):
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

    def get_resource(self, res: PudlResourceKey) -> io.BytesIO:
        """Given resource key, retrieve contents of the file from zenodo."""
        url = self.get_descriptor(res.dataset).get_resource_path(res.name)
        return io.BytesIO(self._fetch_from_url(url).content)


# class GoogleCloudStorageDatapackage(RemoteDatapackage):
#     def __init__(self, bucket: str, path_prefix: str = ''):
#         self.bucket = storage.Client().bucket(bucket_name)
#         self.path_prefix = path_prefix

#     def _blob(self, resource: PudlResourceKey: VersionedDataset, name: str) -> Blob:
#         return self.bucket.blob(



#             resource.get_local_path().as_posix())

        
#     def get_descriptor(self, dataset: VersionedDataset) -> dict:


#     def get_resource(self, dataset: VersionedDataset, name: str) -> bytes:
#         pass

# class PudlResourceKey:
#     """Representation of the file resource.

#     Resource is a basic piece of data that will be consumed by the pudl ETL. It can be either stored locally, or in some caching layer
#     or fetched from zenodo store. Each resoure is identified by its dataset, doi (unique version) and name. It may have arbitrary key=value
#     properties associated with it that can be used for filtering purposes.

#     It is expected that PudlResourceKey can be used as a key for dict-like storage.
#     """

#     def __init__(self, dataset: str, doi: str, name: str, metadata: dict = None):
#         self.doi = doi
#         self.dataset = dataset
#         self.name = name
#         self.metadata = metadata or dict()

#     def __str__(self):
#         return f"Resource({self.get_local_path()})"

#     def __hash__(self):
#         return hash((self.dataset, self.doi, self.name))

#     def get_path(self) -> str: 
#         """Returns the remote (zenodo) path where the file is stored.

#         In order to maintain backward compatibility with prior versions of the datastore
#         we will check if remote_url attributes has been added to metadata. If yes, use that,
#         otherwise use path.
#         """
#         p = self.metadata.get('remote_url') or self.metadata.get('path')
#         if not p:
#             raise KeyError(f"Metadata for {self} does not contain remote_url or path attributes")
#         return p

#     def get_local_path(self) -> Path:
#         doi_dirname = re.sub("/", "-", self.doi)
#         return Path(self.dataset) / doi_dirname / self.name

#     def content_matches_checksum(self, content: bytes) -> bool:
#         m = hashlib.md5()  # nosec
#         m.update(content)
#         logger.info(f'calculated hash: {m.hexdigest()}')
#         return m.hexdigest() == self.metadata["hash"]






# class ZenodoFetcher:
#     """Simple interface for fetching datapackage resources and descriptors from zenodo."""

#     def __init__(self, sandbox: bool = False, timeout: int = 15):
#         self.timeout = timeout
#         backend_type = "sandbox" if sandbox else "production"
#         self._doi = DOI[backend_type]
#         self.token = TOKEN[backend_type]
#         self.api_root = API_ROOT[backend_type]

#         # HTTP Requests
#         self.timeout = timeout
#         retries = Retry(backoff_factor=2, total=3,
#                         status_forcelist=[429, 500, 502, 503, 504])
#         adapter = HTTPAdapter(max_retries=retries)

#         self.http = requests.Session()
#         self.http.mount("http://", adapter)
#         self.http.mount("https://", adapter)

#     def get_dataset_dois(self) -> Dict[str, str]:
#         """Returns dict mapping datasets to their DOIs."""
#         return dict(self._doi)

#     def get_descriptors_as_resources(self) -> Iterator[PudlResourceKey]:
#         """Returns list of known datapackage.json files as PudlResourceKeys."""
#         for dataset, doi in self._doi.items():
#             yield PudlResourceKey(dataset=dataset, doi=doi, name='datapackage.json')

#     def get_doi(self, dataset: str) -> str:
#         """
#         Produce the DOI for a given dataset, or log & raise error.

#         Args:
#             datapackage: the name of the datapackage

#         Returns:
#             str, doi for the datapackage

#         """
#         if dataset not in self._doi:
#             raise KeyError(f"No DOI available for {dataset}")
#         return self._doi[dataset]

#     def _doi_to_url(self, doi: str) -> str:
#         """
#         Given a DOI, produce the API url to retrieve it.

#         Args:
#             doi (str): the doi (concept doi) to retrieve, per
#                 https://help.zenodo.org/

#         Returns:
#             url to get the deposition from the api

#         """
#         match = re.search(r"zenodo.([\d]+)", doi)

#         if match is None:
#             raise ValueError(f"Invalid doi {doi}")

#         zen_id = int(match.groups()[0])
#         return f"{self.api_root}/deposit/depositions/{zen_id}"

#     def _fetch_from_url(self, url: str) -> requests.Response:
#         logger.info(f"Retrieving {url} from zenodo")
#         response = self.http.get(
#             url,
#             params={"access_token": self.token},
#             timeout=self.timeout)
#         if response.status_code == requests.codes.ok:
#             logger.info(f"Successfully downloaded {url}")
#             return response
#         else:
#             raise ValueError(f"Could not download {url}: {response.text}")

#     def fetch_datapackage_descriptor(self, dataset: str) -> DatapackageDescriptor:
#         """Retrieves DatapackageDescriptor for given dataset.

#         Descriptor will be loaded from local cache first and if not present, it will be
#         fetches from zenodo.

#         Args:
#           dataset (str): specifies which dataset should be fetched. 

#         Returns:
#           DatapackageDescriptor 
#         """
#         doi = self.get_doi(dataset)  # type: str
#         dpkg_url = self._doi_to_url(doi)
#         response = self._fetch_from_url(dpkg_url)
#         for f in response.json()["files"]:
#             if f["filename"] == "datapackage.json":
#                 response = self._fetch_from_url(f["links"]["download"])
#                 return DatapackageDescriptor(response.json(), dataset=dataset, doi=doi)
#         raise ValueError(f"No datapackage.json found for doi {doi}")

#     def fetch_resource(self, resource: PudlResourceKey) -> bytes:
#         """Retrieves resource content from zenodo and validates that checkum matches."""
#         response = self._fetch_from_url(resource.get_path())
#         if not resource.content_matches_checksum(response.content):
#             raise ValueError(f'Resource {resource} has invalid checksum')
#         return response.content


# class AbstractCache(ABC):
#     @abstractmethod
#     def get(self, resource: PudlResourceKey) -> bytes:
#         """Retrieves content of given resource or throws KeyError."""
#         pass

#     @abstractmethod
#     def set(self, resource: PudlResourceKey, content: str) -> None:
#         """Sets the content for given resource and adds the resource to cache."""
#         pass
    
#     @abstractmethod
#     def delete(self, resource: PudlResourceKey) -> None:
#         """Removes the resource from cache."""
#         pass

#     @abstractmethod
#     def contains(self, resource: PudlResourceKey) -> bool:
#         """Returns True if the resource is present in the cache."""
#         pass


# class LocalFileCache(AbstractCache):
#     def __init__(self, cache_root_dir: Path):
#         self.cache_root_dir = cache_root_dir

#     def _resource_path(self, resource: PudlResourceKey) -> Path:
#         doi_dirname = re.sub("/", "-", resource.doi)
#         return self.cache_root_dir / resource.get_local_path() 

#     def get(self, resource: PudlResourceKey) -> bytes:
#         logger.info(f'LocalCache.get({resource})')
#         return self._resource_path(resource).open("rb").read()

#     def set(self, resource: PudlResourceKey, value: str):
#         logger.info(f'LocalCache.set({resource})')
#         path = self._resource_path(resource)
#         path.parent.mkdir(parents=True, exist_ok=True)
#         path.open("w").write(value)

#     def delete(self, resource: PudlResourceKey):
#         self._resource_path(resource).unlink(missing_ok=True)

#     def contains(self, resource: PudlResourceKey) -> bool:
#         return self._resource_path(resource).exists()


# class GoogleCloudStorageCache(AbstractCache):
#     def __init__(self, bucket_name: str):
#         self.bucket = storage.Client().bucket(bucket_name)

#     def _blob(self, resource: PudlResourceKey) -> Blob:
#         """Retrieve Blob object associated with given resource."""
#         return self.bucket.blob(resource.get_local_path().as_posix())

#     def get(self, resource: PudlResourceKey) -> bytes:
#         return self._blob(resource).download_as_bytes()

#     def set(self, resource: PudlResourceKey, value):
#         return self._blob(resource).upload_from_string(value)

#     def delete(self, resource: PudlResourceKey): 
#         self._blob(resource).delete()

#     def contains(self, resource: PudlResourceKey) -> bool:
#         return self._blob(resource).exists()


# class LayeredCache:
#     def __init__(self, *caches):
#         """Creates system of layered caches.

#         Content is written to the first-layer cache but retrieval traverses caches until element is
#         found.
#         """
#         self.caches = caches

#     def get(self, resource: PudlResourceKey) -> bytes:
#         for cache in self.caches:
#             if cache.contains(resource):
#                 return cache.get(resource)
#         raise KeyError(f"{resource} not found in the layered cache")

#     def set(self, resource: PudlResourceKey, value):
#         self.caches[0].set(resource, value)

#     def delete(self, resource: PudlResourceKey):
#         self.caches[0].delete(resource)

#     def contains(self, resource: PudlResourceKey) -> bool:
#         for cache in self.caches:
#             if cache.contains(resource):
#                 return True
#         return False


class Datastore:
    """Handle connections and downloading of Zenodo Source archives."""

    def __init__(
        self,
        pudl_in: Optional[Path] = None,
        gcs_bucket: Optional[str] = None,
        sandbox: bool = False,
        timeout: int = 15
    ):
    # TODO(rousik): figure out an efficient way to configure datastore caching
        """
        Datastore manages file retrieval for PUDL datasets.

        Args:
            pudl_in (Path): path to the root pudl data directory where LocalFileCache should be storing resources.
            gcs_bucket (str): bucket to use with Google Cloud Storage cache
            loglevel (str): logging level.
            verbose (bool): If true, logs printed to stdout.
            sandbox (bool): If true, use the sandbox server instead of production
            timeout (float): Network timeout for http requests.

        """
        caches = []  # type: List[AbstractCache]
        if pudl_in:
            caches.append(LocalFileCache(pudl_in / "data"))
        if gcs_bucket:
            caches.append(GoogleCloudStorageCache(bucket_name=gcs_bucket))
        self.local_cache = LayeredCache(*caches)

        self.zenodo_fetcher = ZenodoFetcher(
            sandbox=sandbox,
            timeout=timeout)

        self.datapackage_descriptors = {}  # type: Dict[str, DatapackageDescriptor]
        # TODO(rousik): this API is kind of dirty, we might perhaps defer the loading of these 
        # files locally or remotely when they're needed?
        for res in self.zenodo_fetcher.get_descriptors_as_resources():
            if self.local_cache.contains(res):
                self.datapackage_descriptors[res.doi] = DatapackageDescriptor(
                    json.loads(self.local_cache.get(res)),
                    dataset=res.dataset, doi=res.doi)

    def get_dataset_resources(self, dataset: str) -> DatapackageDescriptor:
        """Returns DatapackageDescriptor instance for given dataset.

        If the DatapackageDescriptor is not locally available, download it from zenodo and store it in local_cache.
        """
        doi = self.zenodo_fetcher.get_doi(dataset)
        if doi not in self.datapackage_descriptors:
            self.datapackage_descriptors[doi] = self.zenodo_fetcher.fetch_datapackage_descriptor(dataset)
            res = PudlResourceKey(dataset=dataset, doi=doi, name=self.DATAPACKAGE_DESCRIPTOR)
            self.local_cache.set(res, self.datapackage_descriptors[doi].to_bytes())
        return self.datapackage_descriptors[doi]

    def get_resources(self, dataset: str, **filters: Any) -> Iterator[Tuple[PudlResourceKey, io.BytesIO]]:
        """Return content of the matching resources.

        Args:
            dataset (str): name of the dataset to query
            **filters (key=val): only return resources that match the key-value mapping in their
            metadata["parts"].

        Yields:
            io.BytesIO holding content for each matching resource
        """
        for res in self.get_dataset_resources(dataset).get_resources(**filters):
            content = None
            if self.local_cache.contains(res):
                content = self.local_cache.get(res)
            else:
                # TODO(rousik): This could be simplified with layered cache if ZenodoFetcher implemented
                # cache API for retrieval.
                content = self.zenodo_fetcher.fetch_resource(res)
            yield (res, io.BytesIO(content))

    def get_unique_resource(self, dataset: str, **filters: Any) -> io.BytesIO:
        # TODO(rousik): should we error check to ensure that there is exactly one resource (and no more)?
        res = self.get_resources(dataset, **filters)
        _, content = next(res)
        try:
            next(res)
        except StopIteration:
            return content
        raise ValueError(f"Multiple resources found for {dataset}: {filters}")

    def get_zipfile_resource(self, dataset: str, **filters: Any) -> zipfile.ZipFile:
        return zipfile.ZipFile(self.get_unique_resource(dataset, **filters))


def parse_command_line():
    """Collect the command line arguments."""
    prod_dois = "\n".join([f"    - {x}" for x in DOI["production"].keys()])
    sand_dois = "\n".join([f"    - {x}" for x in DOI["sandbox"].keys()])

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

    remote_cache = None
    if args.populate_gcs_cache:
        remote_cache = GoogleCloudStorageCache(args.populate_gcs_cache)
    for selection in datasets:
        if args.validate:
            ds.validate(selection)
        else:
            if args.populate_gcs_cache:
                for res, content in ds.get_resources(selection):
                    if remote_cache.contains(res):
                        logger.info(f'Remote cache already contains {res}')
                    else:
                        logger.info(f'Uploading {res} to GCS')
                        remote_cache.set(res, content.read())
            else:
                ds.get_resources(selection)


if __name__ == "__main__":
    sys.exit(main())
