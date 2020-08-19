#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Datastore manages file retrieval for PUDL datasets."""

import argparse
import hashlib
import json
import logging
import re
import sys
from pathlib import Path

import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# The Zenodo tokens recorded here should have read-only access to our archives.
# Including them here is correct in order to allow public use of this tool, so
# long as we stick to read-only keys.

TOKEN = {
    "sandbox": "WX1FW2JOrkcCvoGutoZaGS69W5P9A73wybsToLeo4nt6NP3moeIg7sPBw8nI",
    "production": "N/A"
}

DOI = {
    "sandbox": {
        "eia860": "10.5072/zenodo.657345",
        "eia861": "10.5072/zenodo.658437",
        "eia923": "10.5072/zenodo.657350",
        "epaipm": "10.5072/zenodo.602953",
        "epacems": "10.5072/zenodo.638878",
        "ferc1": "10.5072/zenodo.656695"
    },
    "production": {}
}

PUDL_YML = Path.home() / ".pudl.yml"


class Datastore:
    """Handle connections and downloading of Zenodo Source archives."""

    def __init__(self, pudl_in, loglevel="WARNING", verbose=False,
                 sandbox=False, timeout=7):
        """
        Datastore manages file retrieval for PUDL datasets.

        Args:
            pudl_in (Path): path to the root pudl data directory
            loglevel (str): logging level
            verbose (bool): If true, logs printed to stdout
            sandbox (bool): If true, use the sandbox server instead of
                production
            timeout (float): Network timeout for http requests.
        """
        self.pudl_in = pudl_in
        logger = logging.Logger(__name__)
        logger.setLevel(loglevel)

        if verbose:
            logger.addHandler(logging.StreamHandler())

        self.logger = logger
        logger.info("Logging at loglevel %s" % loglevel)

        if sandbox:
            self._dois = DOI["sandbox"]
            self.token = TOKEN["sandbox"]
            self.api_root = "https://sandbox.zenodo.org/api"

        else:
            raise NotImplementedError(
                "Production archive not ready. Use --sandbox.")
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
            msg = "No DOI available for %s" % dataset
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
            msg = "Invalid doi %s" % doi
            self.logger.error(msg)
            raise ValueError(msg)

        zen_id = int(match.groups()[0])
        return "%s/deposit/depositions/%d" % (self.api_root, zen_id)

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
        text = json.dumps(dpkg, sort_keys=True)

        with path.open("w") as f:
            f.write(text)
            self.logger.debug("%s saved.", path)

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
            msg = "Failed to retrieve %s" % dpkg_url
            self.logger.error(msg)
            raise ValueError(msg)

        jsr = response.json()
        files = {x["filename"]: x for x in jsr["files"]}

        response = self.http.get(
            files["datapackage.json"]["links"]["download"],
            params={"access_token": self.token},
            timeout=self.timeout)

        if response.status_code > 299:
            msg = "Failed to retrieve datapackage for %s: %s" % (
                doi, response.text)
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
                    "Filtered %s on %s: %s != %s", resource["name"], key,
                    part_val, filters[key])
                return False

        return True

    def download_resource(self, resource, directory, retries=3):
        """
        Download a frictionless datapackage resource.

        Args:
            resource: dict, a remotely located resource descriptior from a
                      frictionless datapackage.
            directory: the directory where the resource should be saved

        Returns:
            Path of the saved resource, or none on failure

        """
        response = self.http.get(
            resource["parts"]["remote_url"],
            params={"access_token": self.token},
            timeout=self.timeout)

        if response.status_code >= 299:
            msg = "Failed to download %s, %s" % (resource["path"], response.text)

            self.logger.error(msg)
            raise RuntimeError(msg)

        local_path = directory / resource["name"]

        with local_path.open("wb") as f:
            f.write(response.content)
            self.logger.debug("Cached %s" % local_path)

        if not self._validate_file(local_path, resource["hash"]):
            self.logger.error(
                "Invalid md5 after download of %s.\n"
                "Response: %s\n"
                "Resource: %s\n"
                "Retries left: %d",
                local_path, response, resource, retries)

            if retries > 0:
                self.download_resource(resource, directory,
                                       retries=retries - 1)
            else:
                raise RuntimeError("Could not download valid %s",
                                   resource["path"])

        return local_path

    def _validate_file(self, path, hsh):
        """
        Validate a filename by md5 hash.

        Args:
            path: path to the resource on disk
            hsh: string, expected md5hash
        Returns:
            Boolean: True if the resource md5sum matches the descriptor.
        """
        if not path.exists():
            return False

        with path.open("rb") as f:
            m = hashlib.md5()  # nosec
            m.update(f.read())

        if m.hexdigest() == hsh:
            self.logger.debug("%s appears valid" % path)
            return True

        self.logger.warning("%s md5 mismatch" % path)
        return False

    def _validate_dataset(self, dataset):
        """
        Make sure each resource on disc has the right md5sum.

        Args:
            dataset: name of a dataset

        Returns:
            Bool, True if local resources appear to be correct.

        """
        dpkg = self.datapackage_json(dataset)
        ok = True

        for r in dpkg["resources"]:

            if self.is_remote(r):
                self.logger.debug("%s not cached, skipping validation"
                                  % r["path"])
                continue

            # We verify and warn on every resource. Even though a single
            # failure could end the algorithm, we want to see which ones are
            # invalid.

            ok = self._validate_file(Path(r["path"]), r["hash"]) and ok

        return ok

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
            **kwargs: limit retrieved files to those where the
                datapackage.json["parts"] key & val pairs match provided
                keywords. Eg. year=2011 or state="md"

        Returns:
            list of dicts, each representing a resource per the frictionless
            datapackage spec, except that locas paths are modified to be absolute.
        """
        filters = dict(**kwargs)

        dpkg = self.datapackage_json(dataset)
        self.logger.debug("%s datapackage lists %d resources" %
                          (dataset, len(dpkg["resources"])))

        for r in dpkg["resources"]:

            if self.passes_filters(r, filters):

                if self.is_remote(r) or not self._validate_file(
                        self.local_path(dataset, r["path"]), r["hash"]):
                    local = self.download_resource(r, self.local_path(dataset))

                    # save with a relative path
                    r["path"] = str(local.relative_to(self.local_path(dataset)))
                    self.save_datapackage_json(dataset, dpkg)

                r["path"] = str(self.local_path(dataset, r["path"]))
                yield r


def main_arguments():
    """Collect the command line arguments."""
    prod_dois = "\n".join(["    - %s" % x for x in DOI["production"].keys()])
    sand_dois = "\n".join(["    - %s" % x for x in DOI["sandbox"].keys()])

    dataset_msg = "--Available datasets--\n \nProduction:\n%s\n \nSandbox:\n%s" \
        % (prod_dois, sand_dois)

    parser = argparse.ArgumentParser(
        description="Download and cache ETL source data from Zenodo.",
        epilog=dataset_msg,
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "--dataset", help="Get only a specified dataset. Default gets all.")
    parser.add_argument(
        "--pudl_in",
        help="Override pudl_in directory, defaults to option .pudl.yml")
    parser.add_argument(
        "--validate", help="Validate existing cache.", const=True,
        default=False, action="store_const")
    parser.add_argument(
        "--sandbox", help="Use sandbox server instead of production.",
        action="store_const", const=True, default=False)
    parser.add_argument(
        "--loglevel", help="Set logging level", default="WARNING")
    parser.add_argument(
        "--verbose", help="Display logging messages", default=False,
        action="store_const", const=True)

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

    ds = Datastore(pudl_in, loglevel=args.loglevel, verbose=args.verbose,
                   sandbox=args.sandbox)

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
