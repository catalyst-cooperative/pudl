#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Datastore manages file retrieval for PUDL datasets."""

import os
from pathlib import Path
import logging
import re
import requests

import yaml


# The Zenodo tokens recorded here should have read-only access to our archives.
# Including them here is correct in order to allow public use of this tool, so
# long as we stick to read-only keys.

TOKEN = {
    "sandbox": "WX1FW2JOrkcCvoGutoZaGS69W5P9A73wybsToLeo4nt6NP3moeIg7sPBw8nI",
    "production": "N/A"
}

DOI = {
    "sandbox": {
        "eia860": "10.5072/zenodo.504556",
        "eia861": "10.5072/zenodo.504558",
        "eia923": "10.5072/zenodo.504558",
        "epaipm": "10.5072/zenodo.504564",
        "ferc1": "10.5072/zenodo.504562"
    },
    "production": {}
}

PUDL_YML = Path.home() / ".pudl.yml"


class Datastore:
    """Handle connections and downloading of Zenodo Source archives."""

    def __init__(self, loglevel="WARNING", verbose=False, sandbox=False):
        """
        Datastore manages file retrieval for PUDL datasets.

        It pulls files from Zenodo archives as needed, and caches them
        locally.

        Args:
            loglevel: str, logging level
            verbose: boolean. If true, logs printed to stdout
            sandbox: boolean. If true, use the sandbox server instead of
                     production
        """
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
            self._dois = DOI["production"]
            self.token = TOKEN["production"]
            self.api_root = "https://zenodo.org/api"

        with PUDL_YML.open() as f:
            cfg = yaml.load(f.read(), Loader=yaml.FullLoader)
            self.pudl_in = Path(os.environ.get("PUDL_IN", cfg["pudl_in"]))

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
            doi: str, the doi (concept doi) to retrieve, per
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
        Produce the local path for a given dataset.

        Args:
            dataset: the name of the dataset
            filename: optional filename as it would appear, if included in the
                      path

        Return:
            str: a path
        """
        doi_dirname = re.sub("/", "-", self.doi(dataset))
        directory = self.pudl_in / dataset / "data" / doi_dirname

        if filename is None:
            return directory

        return directory / filename

    def remote_datapackage_json_text(self, doi):
        """
        Produce the contents of a remote datapackage.json.

        Args:
            doi: the DOI

        Returns:
            string contents of the datapackage.json file as available on
            Zenodo, or raises an error
        """
        dpkg_url = self.doi_to_url(doi)
        response = requests.get(
            dpkg_url, params={"access_token": self.token})

        if response.status_code > 299:
            msg = "Failed to retrieve %s" % dpkg_url
            self.logger.error(msg)
            raise ValueError(msg)

        jsr = response.json()
        files = {x["filename"]: x for x in jsr["files"]}

        response = requests.get(
            files["datapackage.json"]["links"]["download"],
            params={"access_token": self.token})

        if response.status_code > 299:
            msg = "Failed to retrieve datapackage for %s: %s" % (
                doi, response.text)
            self.logger.error(msg)
            raise ValueError(msg)

        return response.text

    def datapackage_json(self, dataset, force_download=False):
        """
        Produce the contents of datapackage.json, cache as necessary.

        Args:
            dataset (str): name of the dataset, must be available in the DOIS

        Return:
            dict representation of the datapackage.json
        """
        path = self.local_path(dataset, filename="datapackage.json")

        if force_download or not path.exists():
            doi = self.doi(dataset)
            dpkg = self.remote_datapackage_json_text(doi)
            path.parent.mkdir(parents=True, exist_ok=True)

            with path.open("w") as f:
                f.write(dpkg)

            self.logger.debug("Cached %s", path)

        with path.open("r") as f:
            return yaml.load(f.read(), Loader=yaml.FullLoader)

    def collect(self, dataset, filters=None):
        """
        Download dataset files to PUDL_IN.

        Args:
            dataset (str): name of the dataset, must be available in the DOIS
            filters (dict): limit retrieved files to those where the
                            datapackage.json["parts"] key & val pairs match
                            those in the filter
        Returns:
            None
        """
        raise NotImplementedError("Rewrite this")
        if filters is None:
            filters = {}

        dpkg = self.datapackage_contents(dataset)

        output_dir = os.path.join(self.output_root, dataset)
        os.makedirs(output_dir, exist_ok=True)

        self.logger.debug("Datapackage lists %d resources" %
                          len(dpkg["resources"]))

        for r in dpkg["resources"]:

            if self.passes_filters(r, filters):
                local_path = self.download_resource(r, output_dir)
                self.logger.info("Downloaded %s" % local_path)

    def passes_filters(self, resource, filters):
        """
        Test whether file metadata passes given filters.

        Args:
            resource: dict, a "resource" descriptior from a frictionless
                      datapackage
            filters: dict, pairs that must match in order for a
                     resource to pass
        Returns:
            boolean, True if the resource parts pass the filters
        """
        for key, _ in filters.items():

            part_val = resource["parts"].get(key, None)

            if part_val != filters[key]:
                self.logger.debug(
                    "Filtered %s on %s: %s != %s", resource["name"], key,
                    part_val, filters[key])
                return False

        return True

    def download_resource(self, resource, output_dir):
        """
        Download a frictionless datapackage resource.

        Args:
            resource: dict, a "resource" descriptior from a frictionless
                      datapackage
            output_dir: str, the output directory, must already exist

        Returns:
            str, path to the locally saved resource, or None on failure
        """
        raise NotImplementedError("REDO")
        local_path = os.path.join(output_dir, resource["name"])
        response = requests.get(
            resource["path"], params={"access_token": self.token})

        if response.status_code >= 299:
            self.logger.warning(
                "Failed to download %s, %s", resource["path"], response.text)
            return

        with open(local_path, "wb") as f:
            f.write(response.content)
            self.logger.debug("Downloaded %s", local_path)

        return local_path
