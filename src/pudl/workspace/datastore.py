"""Datastore manages file retrieval for PUDL datasets."""

import argparse
import copy
import hashlib
import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional

import coloredlogs
import datapackage
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# The Zenodo tokens recorded here should have read-only access to our archives.
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
    }
}
API_ROOT = {
    "sandbox": "https://sandbox.zenodo.org/api",
    "production": "https://zenodo.org/api",
}

PUDL_YML = Path.home() / ".pudl.yml"


class PudlFileResource:
    """"This class represents a file containing arbitrary data.

    These resources will be fetched from Zenodo, persisted either in a local
    or some cloud-based cache and their content will be processed by the ETL
    tasks.

    Each PudlFileResource should uniquely identify a piece of
    """

    def __init__(self, dataset: str, name: str, doi: str, metadata: Optional[dict] = None):
        self.doi = doi
        self.dataset = dataset
        self.name = name
        self.content = None  # type: Optional[str]
        self.metadata = None  # type: Optional[dict]

    def __str__(self):
        return f"Res({self.dataset}/{self.doi}/{self.name})"

    # TODO(rousik): do we need to know about doi or do we know the mapping
    # from dataset to doi (based on sandbox vs prod)


class LocalFileCache:
    """Simple implementation of file-backed cache for zenodo files."""

    def __init__(self, cache_root_dir: Path):
        self.cache_root_dir = cache_root_dir

    def _resource_path(self, resource: PudlFileResource) -> Path:
        doi_dirname = re.sub("/", "-", resource.doi)
        return self.cache_root_dir / resource.dataset / doi_dirname / resource.name

    def exists(self, resource: PudlFileResource) -> bool:
        return self._resource_path(resource).exists()

    def get_resource(self, resource: PudlFileResource) -> PudlFileResource:
        """Downloads resource from local file and populates resource.content withi it.

        Returns:
            resource, with resource.content populated with the data from the local
            cache.
        """
        path = self._resource_path(resource)
        if not path.exists():
            raise KeyError(f'{resource} not found in the cache.')
        resource.content = path.open("r").read()
        return resource

    def add_resource(self, resource: PudlFileResource) -> None:
        path = self._resource_path(resource)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            f.write(resource.content)
#            if type(resource.content) == str:
            #f.write(bytes(resource.content, 'utf-8'))
            # else:
            # f.write(resource.content)

    def remove_resource(self, resource: PudlFileResource) -> None:
        """Removes given resource from the local cache."""
        self._resource_path(resource).unlink(missing_ok=True)


class ZenodoFetcher:
    """Simple interface for fetching datapackages and resources from zenodo."""

    def __init__(self, sandbox: bool = False, timeout: int = 15):
        self.timeout = timeout
        backend_type = "sandbox" if sandbox else "production"
        self._doi = DOI[backend_type]
        self.token = TOKEN[backend_type]
        self.api_root = API_ROOT[backend_type]

        # HTTP Requests
        self.timeout = timeout
        retries = Retry(backoff_factor=2, total=3,
                        status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)

        self.http = requests.Session()
        self.http.mount("http://", adapter)
        self.http.mount("https://", adapter)

    def get_dataset_dois(self):
        """Returns dict mapping datasets to their DOIs."""
        return dict(self._doi)

    def doi(self, dataset: str) -> str:
        """
        Produce the DOI for a given dataset, or log & raise error.

        Args:
            datapackage: the name of the datapackage

        Returns:
            str, doi for the datapackage

        """
        try:
            return self._doi[dataset]
        except KeyError:
            # TODO(rousik) this exception wrapping seems a bit pointless
            raise ValueError(f"No DOI available for {dataset}")

    def doi_to_url(self, doi: str) -> str:
        """
        Given a DOI, produce the API url to retrieve it.

        Args:
            doi (str): the doi (concept doi) to retrieve, per
                https://help.zenodo.org/

        Returns:
            url to get the deposition from the api

        """
        match = re.search(r"zenodo.([\d]+)", doi)

        if match is None:
            raise ValueError(f"Invalid doi {doi}")

        zen_id = int(match.groups()[0])
        return f"{self.api_root}/deposit/depositions/{zen_id}"

    def _fetch_from_url(self, url: str):
        response = self.http.get(
            url,
            params={"access_token": self.token},
            timeout=self.timeout)
        if response.status_code == requests.codes.ok:
            return response
        else:
            raise ValueError(f"Could not download {url}: {response.text}")

    def download_datapackage_json(self, doi: str = None, dataset: str = None) -> dict:
        """
        Produce the contents of a remote datapackage.json.

        Args:
            doi (str): fetch datapackage for given doi.
            dataset (str): fetch datapackage for given dataset. Dataset
              will be converted to the corresponding doi first.

        Returns:
            dict representation of the datapackage.json file as available on
            Zenodo, or raises an error
        """
        if (not doi and not dataset) or (doi and dataset):
            raise AssertionError(f"Exactly one of doi, dataset needs to be set")
        doi = doi or self.doi(dataset)  # type: str
        dpkg_url = self.doi_to_url(doi)
        response = self._fetch_from_url(dpkg_url)

        for f in response.json()["files"]:
            if f["filename"] == "datapackage.json":
                response = self._fetch_from_url(f["links"]["download"])
                return response.json()
                # TODO(rousik): originally, json.loads(response.text) was used but
                # perhaps response.json is equivalent
        raise ValueError(f"No datapackage.json found for doi {doi}")

    def get_matching_resources(self, dataset: str = None, **filters):
        dpkg = self.download_datapackage_json(dataset=dataset)
        for res in dpkg["resources"]:
            if self.passes_filters(res, **filters):
                yield PudlFileResource(
                    dataset=dataset,
                    name=res["name"],
                    doi=self.doi(dataset),
                    metadata=res)

    def _verify_checksum(self, content: bytes, md5: str) -> bool:
        m = hashlib.md5()  # nosec
        m.update(content)
        return m.hexdigest() == md5

    def download_resource(self, resource: PudlFileResource, retries: int = 3) -> bytes:
        """
        Download a frictionless datapackage resource.

        Args:
            resource (PudlFileResource): wrapper identifying the resource to
              download.
            retries (int): how many times should we try if file with wrong
              checksum is retrieved.

        Returns:
            bytes with the contents of the resource.
        """

        response = self._fetch_from_url(resource.metadata["path"])
        if not self._verify_checksum(response.content, resource.metadata["hash"]):
            if retries > 0:
                return self.download_resource(resource, retries=(retries - 1))
            raise RuntimeError(f"Could not download valid {resource}")
        else:
            return response.content

# TODO(rousik): add gcs backed cache and add class that will allow caches to be
# stacked on top of one another.


class Datastore:
    """Handle connections and downloading of Zenodo Source archives."""

    def __init__(
        self,
        pudl_in: Path,
        sandbox: bool = False,
        timeout: int = 15
    ):
        """
        Datastore manages file retrieval for PUDL datasets.

        Args:
            pudl_in (Path): path to the root pudl data directory
            loglevel (str): logging level.
            verbose (bool): If true, logs printed to stdout.
            sandbox (bool): If true, use the sandbox server instead of production
            timeout (float): Network timeout for http requests.

        """
        self.local_cache = LocalFileCache(
            pudl_in / "data")
        self.zenodo_fetcher = ZenodoFetcher(
            sandbox=sandbox,
            timeout=timeout)

        self.descriptors = {}  # type: Dict[str, dict]
        self.load_descriptors()

    def load_descriptors(self, local_only=False, force_download=False):
        """Loads datapackage descriptors either from local cache or from zenodo.

        Args:
            local_only (bool): if True, do not attempt to load descriptors from
              zenodo.
            force_download (bool): if True, always download descriptors from
              zenodo. This assumes local_only=False
        """
        if force_download and local_only:
            raise AssertionError("force_download and local_only are incompatible")

        for dataset, doi in self.zenodo_fetcher.get_dataset_dois().items():
            res = PudlFileResource(name="datapackage.json", dataset=dataset, doi=doi)
            if self.local_cache.exists(res) and not force_download:
                try:
                    self.descriptors[doi] = json.loads(
                        self.local_cache.get_resource(res))
                    continue
                except json.JSONDecodeError as err:
                    self.local_cache.remove_resource(res)

            if not local_only:
                logger.info(f"Loading resource {res} from zenodo")
                remote = self.zenodo_fetcher.download_datapackage_json(doi=doi)
                self.descriptors[doi] = remote
                res.content = json.dumps(remote, sort_keys=True, indent=4)
                self.local_cache.add_resource(res)
            if not self._validate_datapackage(self.descriptors[doi]):
                raise ValueError(f"Datapackage for doi {doi} is invalid")

    def passes_filters(self, resource, **filters):
        """
        Test whether file metadata passes given filters.

        Args:
            resource (dict): a "resource" descriptior from a frictionless
                datapackage
            **filters: key value assignments that must be matched for the
                resource to pass

        Returns:
            bool: True if the resource parts pass the filters

        """
        for key, value in filters.items():
            part_val = resource["parts"].get(key, None)
            if part_val != value:
                logger.debug(
                    f"Filtered {resource['name']} on {key}: "
                    f"{part_val} != {value}"
                )
                return False
        return True

    def download_resource(self, resource, directory, retries=3):
        """
        Download a frictionless datapackage resource.

        Args:
            resource: dict, a remotely located resource descriptior from a frictionless
                datapackage.
            directory: the directory where the resource should be saved

        Returns:
            Path of the saved resource, or none on failure

        """
        response = self.http.get(
            resource["remote_url"],
            params={"access_token": self.token},
            timeout=self.timeout)

        if response.status_code >= 299:
            msg = f"Failed to download {resource['path']}, {response.text}"

            logger.error(msg)
            raise RuntimeError(msg)

        local_path = directory / resource["name"]

        with local_path.open("wb") as f:
            f.write(response.content)
            logger.debug(f"Cached {local_path}")

        if not self._validate_file(local_path, resource["hash"]):
            logger.error(
                f"Invalid md5 after download of {local_path}.\n"
                f"Response: {response}\n"
                f"Resource: {resource}\n"
                f"Retries left: {retries}"
            )

            if retries > 0:
                self.download_resource(resource, directory,
                                       retries=retries - 1)
            else:
                raise RuntimeError(
                    f"Could not download valid {resource['path']}")

        return local_path

    def _validate_datapackage(self, datapackage_json: dict):
        """
        Validate the datapackage.json metadata against the datapackage standard.

        Args:
            dataset (str): Name of a dataset.

        Returns:
            bool: True if the datapackage.json is valid. False otherwise.

        """
        dp = datapackage.Package(datapackage_json)
        if not dp.valid:
            msg = f"Found {len(dp.errors)} datapackage validation errors:\n"
            for e in dp.errors:
                msg = msg + f"  * {e}\n"
            logger.warning(msg)
        return dp.valid

    def validate(self):
        """Verifies that checksums of locally cached files match the descriptor"""
        for dataset, doi in self.zenodo_fetcher.get_dataset_dois().items():
            logger.info(f"Validating dataset {dataset}")
            self._validate_datapackage(self.descritpors[doi])
            for res in self.get_resources(dataset, local_only=True):
                logger.info(f"Verifying checksum for {res}")
                if not self.zenodo_fetcher._verify_checksum(res.content, res.metadata["hash"]):
                    raise ValueError(f"Downloaded resource {res} has invalid checksum")

    def get_resources(self, dataset: str, local_only: bool = False, **filters) -> Iterable[PudlFileResource]:
        """
        Produce resource descriptors as requested.

        Any resource listed as remote will be downloaded and the
        datapackage.json will be updated.

        Args:
            dataset (str): name of the dataset, must be available in the DOIS
            local_only (bool): if True, skip over resources that are not available locally
            **filters: limit retrieved files to those where the datapackage.json["parts"]
                key & val pairs match provided keywords. Eg. year=2011 or state="md"

        Returns:
            list of PudlFileResource, each representing a resource per the frictionless
            datapackage spec. The spec itself is contained in the resource.metadata
            field of PudlFileResource.
            Content of the resource is contained in res.content field.

        """
        for res in self.zenodo_fetcher.get_matching_resources(dataset=dataset, **filters):
            # First, try to download from zenodo and add to cache
            if not self.local_cache.exists(res) and not local_only:
                res.content = self.zenodo_fetcher.download_resource(res)
                self.local_cache.add_resource(res)
            try:
                yield self.local_cache.get_resource(res)
            except KeyError:
                continue

    def get_unique_resource(self, dataset: str, **filters) -> str:
        """Variant of get_resources that assumes exacly one result."""
        res = list(self.get_resources(dataset, **filters))
        if len(res) != 1:
            raise ValueError(
                f"Requested resource not unique. Found {len(res)} matching results.")
        else:
            return res[0].content

    # TODO(rousik): zip files are prevalent, maybe we should offer a wrapper that will
    # open zipfile.


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

    for selection in datasets:

        if args.validate:
            ds.validate(selection)
        else:
            ds.get_resources(selection)


if __name__ == "__main__":
    sys.exit(main())
