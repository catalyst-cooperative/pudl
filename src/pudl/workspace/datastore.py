"""Datastore manages file retrieval for PUDL datasets."""

import argparse
import copy
import hashlib
import json
import logging
import re
import sys
from pathlib import Path

import datapackage
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
    }
}

PUDL_YML = Path.home() / ".pudl.yml"


class Datastore:
    """Handle connections and downloading of Zenodo Source archives."""

    def __init__(
        self,
        pudl_in,
        loglevel="WARNING",
        verbose=False,
        sandbox=False,
        timeout=15
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
        self.pudl_in = pudl_in
        logger = logging.Logger(__name__)
        logger.setLevel(loglevel)

        if verbose:
            logger.addHandler(logging.StreamHandler())

        self.logger = logger
        logger.info(f"Logging at loglevel {loglevel}")

        if sandbox:
            self._dois = DOI["sandbox"]
            self.token = TOKEN["sandbox"]
            self.api_root = "https://sandbox.zenodo.org/api"

        else:
            self._dois = DOI["production"]
            self.token = TOKEN["production"]
            self.api_root = "https://zenodo.org/api"

        # HTTP Requests
        self.timeout = timeout
        retries = Retry(backoff_factor=2, total=3,
                        status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)

        self.http = requests.Session()
        self.http.mount("http://", adapter)
        self.http.mount("https://", adapter)

    # Location conversion & helpers

    def doi(self, dataset):
        """
        Produce the DOI for a given dataset, or log & raise error.

        Args:
            datapackage: the name of the datapackage

        Returns:
            str, doi for the datapackage

        """
        try:
            return self._dois[dataset]
        except KeyError:
            msg = f"No DOI available for {dataset}"
            self.logger.error(msg)
            raise ValueError(msg)

    def doi_to_url(self, doi):
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
            msg = f"Invalid doi {doi}"
            self.logger.error(msg)
            raise ValueError(msg)

        zen_id = int(match.groups()[0])
        return f"{self.api_root}/deposit/depositions/{zen_id}"

    def local_path(self, dataset, filename=None):
        """
        Produce the local absolute path for a given dataset.

        Args:
            dataset: the name of the dataset
            filename: optional filename as it would appear, if included in the
                path

        Return:
            str: a path
        """
        doi_dirname = re.sub("/", "-", self.doi(dataset))
        directory = self.pudl_in / "data" / dataset / doi_dirname

        if filename is None:
            return directory

        return directory / filename

    def save_datapackage_json(self, dataset, dpkg):
        """
        Save a datapackage.json file.  Overwrite any previous version.

        Args:
            dataset (str): name of the dataset, as available in DOI
            dpkg (dict): dict matching frictionless datapackage spec

        Returns:
            Path of the saved datapackage
        """
        path = self.local_path(dataset, filename="datapackage.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(dpkg, sort_keys=True, indent=4)

        with path.open("w") as f:
            f.write(text)
            self.logger.debug(f"{path} saved.")
        self._validate_datapackage(dataset)

        return path

    # Datapackage metadata

    def remote_datapackage_json(self, doi):
        """
        Produce the contents of a remote datapackage.json.

        Args:
            doi: the DOI

        Returns:
            dict representation of the datapackage.json file as available on
            Zenodo, or raises an error
        """
        dpkg_url = self.doi_to_url(doi)
        response = self.http.get(
            dpkg_url, params={"access_token": self.token}, timeout=self.timeout)

        if response.status_code > 299:
            msg = f"Failed to retrieve {dpkg_url}"
            self.logger.error(msg)
            raise ValueError(msg)

        jsr = response.json()
        files = {x["filename"]: x for x in jsr["files"]}

        response = self.http.get(
            files["datapackage.json"]["links"]["download"],
            params={"access_token": self.token},
            timeout=self.timeout)

        if response.status_code > 299:
            msg = f"Failed to retrieve datapackage for {doi}: {response.text}"
            self.logger.error(msg)
            raise ValueError(msg)

        return json.loads(response.text)

    def datapackage_json(self, dataset, force_download=False):
        """
        Produce the contents of datapackage.json, cache as necessary.

        Args:
            dataset (str): name of the dataset, must be available in the DOIS

        Return:
            dict: representation of the datapackage.json

        """
        path = self.local_path(dataset, filename="datapackage.json")

        if force_download or not path.exists():
            doi = self.doi(dataset)
            dpkg = self.remote_datapackage_json(doi)
            self.save_datapackage_json(dataset, dpkg)

        with path.open("r") as f:
            return yaml.safe_load(f)

    # Remote resource retrieval

    def is_remote(self, resource):
        """
        Determine whether a described resource is located on a remote server.

        Args:
            resource: dict, a resource descriptor from a frictionless data
                package

        Returns:
            bool
        """
        return resource["path"][:8] == "https://" or \
            resource["path"][:7] == "http://"

    def passes_filters(self, resource, filters):
        """
        Test whether file metadata passes given filters.

        Args:
            resource (dict): a "resource" descriptior from a frictionless
                datapackage
            filters (dict): pairs that must match in order for a
                resource to pass

        Returns:
            bool: True if the resource parts pass the filters

        """
        for key, _ in filters.items():

            part_val = resource["parts"].get(key, None)

            if part_val != filters[key]:
                self.logger.debug(
                    f"Filtered {resource['name']} on {key}: "
                    f"{part_val} != {filters[key]}"
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

            self.logger.error(msg)
            raise RuntimeError(msg)

        local_path = directory / resource["name"]

        with local_path.open("wb") as f:
            f.write(response.content)
            self.logger.debug(f"Cached {local_path}")

        if not self._validate_file(local_path, resource["hash"]):
            self.logger.error(
                f"Invalid md5 after download of {local_path}.\n"
                f"Response: {response}\n"
                f"Resource: {resource}\n"
                f"Retries left: {retries}"
            )

            if retries > 0:
                self.download_resource(resource, directory,
                                       retries=retries - 1)
            else:
                raise RuntimeError(f"Could not download valid {resource['path']}")

        return local_path

    def _validate_file(self, path, hsh):
        """
        Validate a filename by md5 hash.

        Args:
            path: path to the resource on disk
            hsh: string, expected md5hash

        Returns:
            bool: True if the resource md5sum matches the descriptor.

        """
        if not path.exists():
            return False

        with path.open("rb") as f:
            m = hashlib.md5()  # nosec
            m.update(f.read())

        if m.hexdigest() == hsh:
            self.logger.debug(f"{path} md5 hash is valid")
            return True

        self.logger.warning(f"{path} md5 mismatch")
        return False

    def _validate_dataset(self, dataset):
        """
        Validate datapackage.json and check each resource on disk has correct md5sum.

        Args:
            dataset: name of a dataset

        Returns:
            bool: True if local datapackage.json is valid and resources have good md5
            checksums.

        """
        dp = self.datapackage_json(dataset)
        ok = self._validate_datapackage(dataset)

        for r in dp["resources"]:

            if self.is_remote(r):
                self.logger.debug(f"{r['path']} not cached, skipping validation")
                continue

            # We verify and warn on every resource. Even though a single
            # failure could end the algorithm, we want to see which ones are
            # invalid.

            ok = self._validate_file(
                self.local_path(dataset, r["path"]), r["hash"]) and ok

        return ok

    def _validate_datapackage(self, dataset):
        """
        Validate the datapackage.json metadata against the datapackage standard.

        Args:
            dataset (str): Name of a dataset.

        Returns:
            bool: True if the datapackage.json is valid. False otherwise.

        """
        dp = datapackage.Package(self.datapackage_json(dataset))
        if not dp.valid:
            msg = f"Found {len(dp.errors)} datapackage validation errors:\n"
            for e in dp.errors:
                msg = msg + f"  * {e}\n"
            self.logger.warning(msg)
        else:
            self.logger.debug(
                f"{self.local_path(dataset, filename='datapackage.json')} is valid"
            )
        return dp.valid

    def validate(self, dataset=None):
        """
        Validate all datasets, or just the specified one.

        Args:
            dataset: name of a dataset.

        Returns:
            bool: True if local resources appear to be correct.

        """
        if dataset is not None:
            return self._validate_dataset(dataset)

        valid = True

        for dataset in self._dois.keys():
            valid = valid and self._validate_dataset(dataset)

        return valid

    def get_resources(self, dataset, **kwargs):
        """
        Produce resource descriptors as requested.

        Any resource listed as remote will be downloaded and the
        datapackage.json will be updated.

        Args:
            dataset (str): name of the dataset, must be available in the DOIS
            kwargs: limit retrieved files to those where the datapackage.json["parts"]
                key & val pairs match provided keywords. Eg. year=2011 or state="md"

        Returns:
            list of dicts, each representing a resource per the frictionless
            datapackage spec, except that local paths are modified to be absolute.

        """
        filters = dict(**kwargs)

        dpkg = self.datapackage_json(dataset)
        self.logger.debug(
            f"{dataset} datapackage lists {len(dpkg['resources'])} resources"
        )

        for r in dpkg["resources"]:

            if self.passes_filters(r, filters):

                if self.is_remote(r) or not self._validate_file(
                        self.local_path(dataset, r["path"]), r["hash"]):
                    local = self.download_resource(r, self.local_path(dataset))

                    # save with a relative path
                    r["path"] = str(local.relative_to(self.local_path(dataset)))
                    self.logger.debug(f"resource local relative path: {r['path']}")
                    self.save_datapackage_json(dataset, dpkg)

                r_abspath = copy.deepcopy(r)
                r_abspath["path"] = str(self.local_path(dataset, r["path"]))
                yield r_abspath


def main_arguments():
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
        "The default is to download all, which takes a long time depending on network "
        "speed."
    )
    parser.add_argument(
        "--pudl_in",
        help="Override pudl_in directory, defaults to setting in ~/.pudl.yml",
    )
    parser.add_argument(
        "--validate",
        help="Validate locally cached datapackages, but don't download anything.",
        action="store_const",
        const=True,
        default=False,
    )
    parser.add_argument(
        "--sandbox",
        help="Download data from Zenodo sandbox server. For testing purposes only.",
        action="store_const",
        const=True,
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
        action="store_const",
        const=True,
        default=False,
    )

    return parser.parse_args()


def main():
    """Cache datasets."""
    args = main_arguments()
    dataset = getattr(args, "dataset", None)
    pudl_in = getattr(args, "pudl_in", None)

    if pudl_in is None:
        with PUDL_YML.open() as f:
            cfg = yaml.safe_load(f)
            pudl_in = Path(cfg["pudl_in"])
    else:
        pudl_in = Path(pudl_in)

    ds = Datastore(
        pudl_in,
        loglevel=args.loglevel,
        verbose=not args.quiet,
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
            continue

        list(ds.get_resources(selection))


if __name__ == "__main__":
    sys.exit(main())
