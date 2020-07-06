"""Functions for merging compatible PUDL datapackges together."""
import logging
import pathlib
import shutil

import pandas as pd

import pudl

logger = logging.getLogger(__name__)


def check_identical_vals(dps, required_vals, optional_vals=()):
    """
    Verify that datapackages to be merged have required identical values.

    This only works for elements with simple (hashable) datatypes, which can be
    added to a set.

    Args:
        dps (iterable): a list of tabular datapackage objects, output by PUDL.
        required_vals (iterable): A list of strings indicating which top level
            metadata elements should be compared between the datapackages. All
            must be present in every datapackage.
        optional_vals (iterable): A list of strings indicating top level
            metadata elements to be compared between the datapackages. They do
            not need to appear in all datapackages, but if they do appear,
            they must be identical.

    Returns:
        None

    Raises:
        ValueError: if any of the required or optional metadata elements have
            different values in the different data packages.
        KeyError: if a required metadata element is not found in any of the
            datapackages.

    """
    vals = list(required_vals)
    vals.extend(list(optional_vals))
    for val in vals:
        test_vals = set()
        for dp in dps:
            try:
                test_vals.add(dp.descriptor[val])
            except KeyError:
                if val in optional_vals:
                    continue
        if len(test_vals) > 1:
            raise ValueError(
                f"Multiple values of {val}. Datapackages cannot be merged.")


def check_etl_params(dps):
    """
    Verify that datapackages to be merged have compatible ETL params.

    Given that all of the input data packages come from the same ETL run, which
    means they will have used the same input data, the only way they should
    potentially differ is in the ETL parameters which were used to generate
    them. This function pulls the data source specific ETL params which we
    store in each datapackage descriptor and checks that within a given data
    source (e.g. eia923, ferc1) all of the ETL parameters are identical (e.g.
    the years, states, and tables loaded).

    Args:
        dps (iterable): A list of datapackage.Package objects, representing the
            datapackages to be merged.

    Returns:
        None

    Raises:
        ValueError: If the PUDL ETL parameters associated with any given data
            source are not identical across all instances of that data source
            within the datapackages to be merged. Also if the ETL UUIDs for all
            of the datapackages to be merged are not identical.

    """
    # These are all the possible datasets right now... note that this is
    # slightly different from the data *source* codes, because we have merged
    # the EIA 860 and EIA 923 souces into a single dataset called EIA...
    dataset_codes = ["eia", "epacems", "ferc1", "epaipm"]

    # For each of the unique source codes, verify that all ETL parameters
    # associated with it in any of the input data packages are identical:
    for dataset_code in dataset_codes:
        etl_params = []
        for dp in dps:
            for dataset in dp.descriptor["etl-parameters-pudl"]:
                if dataset_code in dataset.keys():
                    etl_params.append(dataset[dataset_code])
        for params in etl_params:
            if not params == etl_params[0]:
                raise ValueError(
                    f"Mismatched PUDL ETL parameters for {dataset_code}.")


def merge_data(dps, out_path):
    """
    Copy the CSV files into the merged datapackage's data directory.

    Iterates through all of the resources in the input datapackages and copies
    the files they refer to into the data directory associated with the merged
    datapackage (a directory named "data" inside the out_path directory).

    Function assumes that a fresh (empty) data directory has been created. If a
    file with the same name already exists, it is not overwritten, in order to
    prevent unnecessary copying of resources which appear in multiple input
    packages.

    Args:
        dps (iterable): A list of datapackage.Package objects, representing the
            datapackages to be merged.
        out_path (path like): Base directory for the newly created datapackage.
            The final path element will also be used as the name of the merged
            data package.

    Returns:
        None

    """
    data_path = pathlib.Path(out_path, "data")
    for dp in dps:
        for resource in dp.descriptor["resources"]:
            src = pathlib.Path(dp.base_path, resource["path"])
            dst = pathlib.Path(data_path, src.name)
            if not dst.exists():
                shutil.copy(src, dst)


def merge_meta(dps, datapkg_name):
    """Merge the JSON descriptors of datapackages into one big descriptor.

    This function builds up a new tabular datapackage JSON descriptor as a
    python dictionary, containing the merged metadata from all of the input
    datapackages.

    The process is complex for two reasons. First, there are several different
    datatypes in the descriptor that need to be merged, and the processes for
    each of them are different. Second, what constitutes a "merge" may vary
    depending on the semantic content of the metadata. E.g. the ``created``
    timestamp is a simple string, but we need to choose one of the several
    values (the earliest one) for inclusion in the merged datapackage, while
    many other simple string fields are required to be identical across all
    of the input data packages (e.g. ``datapkg-bundle-uuid``):

    Args:
        dps (iterable): A collection of datapackage objects, whose metadata
            will be merged to create a single datapackage descriptor
            representing the union of all the data in the input datapackages.
        datapkg_name (str): The name associated with the newly merged
            datapackage. This should be the same as the name of the directory
            in which the datapackage is found.

    Returns:
        dict: a Python dictionary representing a tabular datapackage JSON
        descriptor, encoded as a python dictionary, containing the merged
        metadata of the input datapackages.

    """
    # Set up the initial datapackage descriptor dictionary:
    descriptor = {
        "name": datapkg_name,
        "title": "An merged bundle of PUDL tabular datapackages.",
    }
    required_vals = [
        "datapkg-bundle-uuid",
        "python-package-version",
        "python-package-name",
        "homepage",
        "licenses",  # NOT checked for matching. Should it be derived?
    ]
    optional_vals = ["datapkg-bundle-doi"]
    identical_vals = required_vals
    identical_vals.extend(optional_vals)
    for val in identical_vals:
        try:  # Just grab the value from the first datapackage:
            descriptor[val] = dps[0].descriptor[val]
        except KeyError:
            if val in optional_vals:
                continue
    # Set created time to be the earliest created time of any input datapkg:
    descriptor["created"] = (
        min(pd.to_datetime([dp.descriptor["created"] for dp in dps]))
        .isoformat()
        .replace("+00:00", "Z")
    )
    # Take the union of all input datapackage keywords:
    keywords = set()
    for keyword_list in [dp.descriptor["keywords"] for dp in dps]:
        for keyword in keyword_list:
            keywords.add(keyword)
    keywords = list(keywords)
    keywords.sort()
    descriptor["keywords"] = keywords
    # Use a helper function to merge the dictionaries of autoincrement IDs:
    descriptor["autoincrement"] = pudl.helpers.merge_dicts(
        list_of_dicts=[dp.descriptor["autoincrement"] for dp in dps])

    # The contributors, sources, and resources are all (effectively) lists of
    # dictionaries, and each of them contains an element which should name
    # them uniquely. This allows us to deduplicate the lists:
    keys = {
        "resources": "name",
        "contributors": "title",
        "sources": "path",
    }
    for key in keys:
        # Build a single list of all the dictionaries for a given element, from
        # the values found in all of the input datapackages:
        list_of_dicts = []
        for dp in dps:
            list_of_dicts.extend(dp.descriptor[key])
        # Use that list to create a dictionary, keyed by the unique value for
        # that metadata element. This effectively deduplicates the list, since
        # latter additions to the dictionary with a given key overwrite any
        # previous additions. This process *assumes* but does not verify that
        # the dictionaries being added/overwritten are identical.
        dict_of_dicts = {}
        for d in list_of_dicts:
            dict_of_dicts[d[keys[key]]] = d
        # Make a list out of the values of the resulting dict of dicts, and
        # use that list of dicts as the metadata element for the merged
        # datapackage descriptor which we return:
        descriptor[key] = list(dict_of_dicts.values())

    return descriptor


def merge_datapkgs(dps, out_path, clobber=False):
    """
    Merge several compatible datapackages into one larger datapackage.

    Args:
        dps (iterable): A collection of tabular data package objects that were
            output by PUDL, to be merged into a single deduplicated datapackage
            for loading into a database or other storage medium.
        out_path (path-like): Base directory for the newly created
            datapackage. The final path element will also be used as the name
            of the merged data package.
        clobber (bool): If the location of the output datapackage already
            exists, should it be overwritten? If True, yes. If False, no.

    Returns:
        dict: A report containing information about the validity of the
        merged datapackage.

    Raises:
        FileNotFoundError: If any of the input datapackage paths do not exist.
        FileExistsError: If the output directory exists and clobber is False.

    """
    # Create the output data directory, and intervening directories:
    pathlib.Path(out_path, "data").mkdir(parents=True, exist_ok=False)

    # Verify all packages have identical UUIDs, python package versions, etc.
    check_identical_vals(
        dps,
        required_vals=["datapkg-bundle-uuid",
                       "python-package-name",
                       "python-package-version",
                       "homepage", ],
        optional_vals=["datapkg-bundle-doi"])
    # Verify that the various data packages used identical ETL parameters for
    # each of the data sources.
    check_etl_params(dps)

    # Copy the CSV files over to the new output location:
    merge_data(dps, out_path=out_path)

    # generate a flattened dp metadata descriptor, using the last element of
    # the output datapackage path as the name of the newly merged datapackage.
    descriptor = merge_meta(dps, datapkg_name=out_path.name)

    # using the pkg_descriptor, validate and save the data package metadata
    report = pudl.load.metadata.validate_save_datapkg(
        descriptor, datapkg_dir=out_path)

    return report
