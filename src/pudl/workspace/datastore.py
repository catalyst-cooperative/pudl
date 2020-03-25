#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import re
import requests

import sys
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


class Datastore:
    """
    Handle connections and downloading of Zenodo Source archives
    """

    def __init__(self, loglevel="WARNING", verbose=False, sandbox=False):
        """
        Create a ZenDownloader instance

        Args:
            loglevel: str, logging level
            verbose: boolean. If true, logs printed to stdout
            sandbox: boolean. If true, use the sandbox server instead of
                     production

        Returns:
            ZenDownloader instance
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

        with open(os.path.expanduser("~/.pudl.yml"), "r") as f:
            cfg = yaml.load(f.read(), Loader=yaml.FullLoader)
            self.pudl_in = os.environ.get("PUDL_IN", cfg["pudl_in"])

    def doi(self, dataset):
        """
        Produce the DOI for a given dataset, or log & raise error

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
        doi_path = re.sub("/", "-", self.doi(dataset))
        directory = os.path.join(self.pudl_in, dataset, "data", doi_path)

        if filename is None:
            return directory

        return os.path.join(directory, filename)

    def collect(self, dataset, filters=None):
        """
        Download the files from the provided dataset to the appropriate
        PUDL_IN directory, coordinating other methods as needed

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

    def datapackage_contents(self, dataset, force_download=False):
        """
        Produce the contents of the datapackage.json file, caching locally as
        necessary

        Args:
            dataset (str): name of the dataset, must be available in the DOIS

        Return:
            dict representation of the datapackage.json
        """
        def download_datapackage_json(doi, path):
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

            os.makedirs(os.path.dirname(path), exist_ok=True)

            with open(path, "w") as f:
                f.write(response.text)

            self.logger.debug("Datapackage cached: %s" % path)
            return path
            # ------------------------------------------------------

        path = self.local_path(dataset, filename="datapackage.json")

        if force_download or not os.path.exists(path):
            download_datapackage_json(self.doi(dataset), path)

        with open(path, "r") as f:
            return yaml.load(f.read(), Loader=yaml.FullLoader)

    def doi_to_url(self, doi):
        """
        Given a DOI, produce the API url to retrieve it

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

    def passes_filters(self, resource, filters):
        """
        Test whether file metadata passes given filters

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
        Download a frictionless datapackage resource

        Args:
            resource: dict, a "resource" descriptior from a frictionless
                      datapackage
            output_dir: str, the output directory, must already exist

        Returns:
            str, path to the locally saved resource, or None on failure
        """
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


def main_arguments():
    """
    Parse the command line arguments.

    Args: None
    Returns: args object
    """

    parser = argparse.ArgumentParser(
        description="Download PUDL source data from Zenodo archives",
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--year", type=int, help="Limit results by (4 digit) year")
    parser.add_argument("--month", type=int,
                        help="Limit results by (2 digit) month")
    parser.add_argument("--state", type=str,
                        help="Limit results by (2 char) US state")

    parser.add_argument("--sandbox", action="store_const", const=True,
                        default=False,
                        help="Use sandbox instead of production sources")
    parser.add_argument("--verbose", action="store_const",
                        const=True, default=False, help="Log to stdout")
    parser.add_argument("--loglevel", type=str, default="WARNING",
                        help="Set log level")
    parser.add_argument(
        "archive", type=str,
        help="Dataset to download, or 'list' to see what's available.")

    return parser.parse_args()


def available_archives():
    """
    List available sources, as found in the ~/.pudl.yml file

    Args: None
    Returns: str, describe available sources
    """
    cfg = ZenodoDownload.load_config()

    try:
        sandboxes = ", ".join(cfg["zenodo_download"]["sandbox"]["dois"].keys())
    except:
        sandboxes = "None"

    try:
        production = ", ".join(cfg["zenodo_download"]["production"]["dois"].keys())
    except:
        production = "None"

    return """Available Archives
------------------

 - Production: %s
 - Sandbox: %s
""" % (production, sandboxes)


def main():
    args = main_arguments()

    if args.archive == "list":
        print(available_archives())
        return 0

    filters = {}

    if getattr(args, "year", None) is not None:
        filters["year"] = args.year

    if getattr(args, "month", None) is not None:
        filters["month"] = args.month

    if getattr(args, "state", None) is not None:
        filters["state"] = args.state

    zen_dl = ZenodoDownload(loglevel=args.loglevel, verbose=args.verbose,
                            sandbox=args.sandbox)

    zen_dl.collect(args.archive, filters=filters)
    zen_dl.logger.debug("Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
