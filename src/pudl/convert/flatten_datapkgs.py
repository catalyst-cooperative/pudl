"""
This module takes a bundle of datapackages and flattens them.

Because we have enabled the generation of multiple data packages as a part of a
data package "bundle", we need to squish the multiple data packages together in
order to put all of the pudl data into one data package. This is especailly
useful for converting the data package to a SQLite database or any other format.

The module does two main things:
 - squish the csv's together
 - squish the metadata (datapackage.json) files together

The CSV squishing is pretty simple and is all being done in
`flatten_datapkg_bundle_csvs`. We are assuming and enforcing that if two data
packages include the same dataset, that dataset has the same ETL parameters
(years, tables, states, etc.). The metadata is slightly more complicated to
compile because each element of the metadata is structured differently. Most of
that work is being done in `flatten_datapkg_bundle_metadata`.
"""

import itertools
import json
import logging
import os
import pathlib
import shutil

import pudl

logger = logging.getLogger(__name__)


##############################################################################
# Flattening PUDL Data Packages
##############################################################################


def flatten_datapkg_bundle_csvs(datapkg_bundle_dir, datapkg_name='pudl-all'):
    """
    Copy the CSVs into a new data package directory.

    Args:
        datapkg_bundle_dir (path-like): the subdirectory where the bundle of
            data packages live
        datapkg_name (str): the name you choose for the flattened data package.

    """
    # set where the flattened datapackage is going to live
    all_dir = pathlib.Path(datapkg_bundle_dir, datapkg_name)
    # delete the subdirectory if it exists
    if os.path.exists(all_dir):
        shutil.rmtree(all_dir)
    # make the subdirectory..
    os.mkdir(all_dir)
    # we also need the sub-subdirectory for the data
    all_data_dir = pathlib.Path(datapkg_bundle_dir, datapkg_name, 'data')
    os.mkdir(all_data_dir)
    # for each of the package directories, copy over the csv's
    for datapkg_dir in datapkg_bundle_dir.iterdir():
        # copy all the csv's except not from all_dir - would make duplicates or
        # from the epacems package (because it has CEMS and EIA files).
        if datapkg_dir != all_dir:  # and 'epacems' in pkg_dir.name:
            for csv in pathlib.Path(datapkg_dir, 'data').iterdir():
                # if the csv already exists, shutil.copy will overrite. this is
                # fine because we've already checked if the parameters are the
                # same
                shutil.copy(csv, all_data_dir)


def compile_datapkg_bundle_metadata(datapkg_bundle_dir,
                                    datapkg_name='pudl-all'):
    """
    Grab the metadata from each of your dp's.

    Args:
        datapkg_bundle_dir (path-like): the subdirectory where the bundle of
            data packages live
        datapkg_name (str): the name you choose for the flattened data package.

    Returns:
        dict: datapkg_descriptor_elements

    """
    datapkg_descriptor_elements = {}
    for datapkg_dir in datapkg_bundle_dir.iterdir():
        if datapkg_dir.name != datapkg_name:
            with open(pathlib.Path(datapkg_dir, "datapackage.json")) as md:
                metadata = json.load(md)
            for thing in ['datapkg-bundle-uuid', 'licenses', 'homepage',
                          'profile', 'created', 'sources', 'contributors',
                          'resources', 'autoincrement', 'keywords',
                          'python-package-name', 'python-package-version',
                          'version']:
                try:
                    datapkg_descriptor_elements[thing].append(metadata[thing])
                except KeyError:
                    try:
                        datapkg_descriptor_elements[thing] = [metadata[thing]]
                    except KeyError:
                        # These are optional fields:
                        if thing not in ["version", "datapkg-bundle-doi"]:
                            raise
    return(datapkg_descriptor_elements)


def flatten_datapkg_bundle_metadata(datapkg_bundle_dir,
                                    datapkg_name='pudl-all'):
    """
    Convert a bundle of PUDL data package metadata into one file.

    Args:
        datapkg_bundle_dir (path-like): the subdirectory where the bundle of
            data packages live
        datapkg_name (str): the name you choose for the flattened data package.

    Returns:
        dict: datapkg_descriptor

    """
    # grab the peices of metadata from each of the data packages
    datapkg_descriptor_elements = compile_datapkg_bundle_metadata(
        datapkg_bundle_dir, datapkg_name=datapkg_name)
    # the beginning of the data package descriptor
    datapkg_descriptor = {
        'name': datapkg_name,
        'title': 'Flattened bundle of pudl data packages',
    }
    # the uuid for the individual data packages should be exactly the same
    if len(set(datapkg_descriptor_elements['datapkg-bundle-uuid'])) != 1:
        raise AssertionError(
            'too many ids found in data packages metadata')
    # for these pkg_descriptor items, they should all be the same, so we are
    # just going to grab the first item for the flattened metadata
    for item in ['datapkg-bundle-uuid', 'datapkg-bundle-doi', 'licenses',
                 'homepage', 'python-package-version', 'python-package-name']:
        try:
            datapkg_descriptor[item] = datapkg_descriptor_elements[item][0]
        except KeyError:
            if item == "datapkg-bundle-doi":
                pass

    # we're gonna grab the first 'created' timestap (each dp generates it's own
    # timestamp, which are slightly different)
    datapkg_descriptor['created'] = min(datapkg_descriptor_elements['created'])
    # these elements are dictionaries that have different items inside of them
    # we want to grab all of the elements and have no duplicates
    for item in ['sources', 'contributors', 'resources']:
        # flatten the list of dictionaries
        list_of_dicts = \
            [item for sublist in datapkg_descriptor_elements[item]
                for item in sublist]
        if item == 'contributors':
            # turn the dict elements into tuple/sets to de-dupliate it
            datapkg_descriptor[item] = [dict(y) for y in set(
                tuple(x.items()) for x in list_of_dicts)]
        elif item == 'sources' or 'resources':
            item_list = []
            # pull only one of each dataset creating a dictionary with the
            # title as the key (both source and resources have titles that
            # should never differ between data packages).
            for i in dict([(item_dict['title'], item_dict)
                           for item_dict in list_of_dicts]).values():
                item_list.append(i)
            datapkg_descriptor[item] = item_list
    # autoincrement is a dictionary that we want merged, so there is a little
    # function that helps for that.
    datapkg_descriptor['autoincrement'] = pudl.helpers.merge_dicts(
        datapkg_descriptor_elements['autoincrement'])
    # for lists we'd like to merge, we are flattening the list of lists, then
    # de-duplicating the elements by turning them into a set
    datapkg_descriptor['keywords'] = list(
        set(itertools.chain.from_iterable(
            datapkg_descriptor_elements['keywords'])))
    return(datapkg_descriptor)


def get_all_sources(datapkg_descriptor_elements):
    """Grab list of all of the datasets in a data package bundle."""
    source_codes = set()
    for sources in datapkg_descriptor_elements['sources']:
        for source in sources:
            source_codes.add(source['source_code'])
    return(source_codes)


def get_same_source_meta(datapkg_descriptor_elements, source_code):
    """Grab the source metadata of the same dataset from all data packages."""
    samezies = []
    for sources in datapkg_descriptor_elements['sources']:
        for source in sources:
            if source['source_code'] == source_code:
                samezies.append(source)
    return(samezies)


def check_for_matching_parameters(datapkg_bundle_dir, datapkg_name):
    """
    Check to see if the ETL parameters for datasets are the same across dp's.

    Args:
        pkg_bundle_dir (path-like): the subdirectory where the bundle of data
            packages live
        pkg_name (str): the name you choose for the flattened data package.

    """
    logger.info('Checking for matching ETL parameters across data packages')
    # grab all of the metadata components
    datapkg_descriptor_elements = compile_datapkg_bundle_metadata(
        datapkg_bundle_dir, datapkg_name=datapkg_name)
    # grab all of the sources code
    source_codes = get_all_sources(datapkg_descriptor_elements)
    # check if
    for src in source_codes:
        samezies = get_same_source_meta(datapkg_descriptor_elements, src)
        # for each of the source dictionaries, check if they are the same
        for source_dict in samezies:
            if not samezies[0] == source_dict:
                raise AssertionError(f'parameters do not match for {src}')


def flatten_datapkg_bundle(pudl_settings,
                           datapkg_bundle_name,
                           datapkg_name='pudl-all'):
    """
    Combines a collection of PUDL data packages into one.

    Args:
        datapkg_bundle_name (str): the name of the subdirectory where the
            bundle of data packages live. Normally, this name will have been
            generated in `generate_datapkg_bundle`.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        datapkg_name (str): the name you choose for the flattened data package.

    Returns:
        dict: a dictionary of the data package validation report.

    """
    # determine the subdirectory for the package bundles...
    datapkg_bundle_dir = pathlib.Path(pudl_settings['datapkg_dir'],
                                      datapkg_bundle_name)
    if not os.path.exists(datapkg_bundle_dir):
        raise AssertionError(
            "The datapackage bundle directory does not exist. ")

    # ensure data packages that have the same sources have the same parameters
    check_for_matching_parameters(datapkg_bundle_dir, datapkg_name)

    # copy the csv's into a new data package directory
    flatten_datapkg_bundle_csvs(datapkg_bundle_dir, datapkg_name=datapkg_name)
    # generate a flattened dp metadata descriptor
    datapkg_descriptor = flatten_datapkg_bundle_metadata(
        datapkg_bundle_dir, datapkg_name=datapkg_name)
    # using the pkg_descriptor, validate and save the data package metadata
    report = pudl.load.metadata.validate_save_datapkg(
        datapkg_descriptor,
        datapkg_dir=pathlib.Path(datapkg_bundle_dir, datapkg_name))
    return(report)
