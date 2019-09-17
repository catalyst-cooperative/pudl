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
`flatten_data_packages_csvs`. We are assuming and enforcing that if two data
packages include the same dataset, that dataset has the same ETL parameters
(years, tables, states, etc.). The metadata is slightly more complicated to
compile because each element of the metadata is structured differently. Most of
that work is being done in `flatten_data_package_metadata`.
"""

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


def flatten_data_packages_csvs(pkg_bundle_dir, pkg_name='pudl-all'):
    """
    Copy the CSVs into a new data package directory.

    Args:
        pkg_bundle_dir (path-like): the subdirectory where the bundle of data
            packages live
        pkg_name (str): the name you choose for the flattened data package.

    """
    # set where the flattened datapackage is going to live
    all_dir = pathlib.Path(pkg_bundle_dir, pkg_name)
    # delete the subdirectory if it exists
    if os.path.exists(all_dir):
        shutil.rmtree(all_dir)
    # make the subdirectory..
    os.mkdir(all_dir)
    # we also need the sub-subdirectory for the data
    all_data_dir = pathlib.Path(pkg_bundle_dir, pkg_name, 'data')
    os.mkdir(all_data_dir)
    # for each of the package directories, copy over the csv's
    for pkg_dir in pkg_bundle_dir.iterdir():
        # copy all the csv's except not from all_dir - would make duplicates or
        # from the epacems package (because it has CEMS and EIA files).
        if pkg_dir != all_dir:  # and 'epacems' in pkg_dir.name:
            for csv in pathlib.Path(pkg_dir, 'data').iterdir():
                # if the csv already exists, shutil.copy will overrite. this is
                # fine because we've already checked if the parameters are the
                # same
                shutil.copy(csv, all_data_dir)
        # for the CEMS pacakge, only pull the actual CEMS tables.
        # elif 'epacems' in pkg_dir.name:
        #    for csv in pathlib.Path(pkg_dir, 'data').iterdir():
        #        shutil.copy(csv, all_data_dir)
                # if 'hourly_emissions_epacems' in csv.name:
                #    shutil.copy(csv, all_data_dir)


def compile_data_packages_metadata(pkg_bundle_dir,
                                   pkg_name='pudl-all'):
    """
    Grab the metadata from each of your dp's.

    Args:
        pkg_bundle_dir (path-like): the subdirectory where the bundle of data
            packages live
        pkg_name (str): the name you choose for the flattened data package.

    Returns:
        dict: pkg_descriptor_elements

    """
    pkg_descriptor_elements = {}
    for pkg_dir in pkg_bundle_dir.iterdir():
        if pkg_dir.name != pkg_name:
            with open(pathlib.Path(pkg_dir, "datapackage.json")) as md:
                metadata = json.load(md)
            for thing in ['id', 'licenses', 'homepage', 'profile',
                          'created', 'sources', 'contributors', 'resources',
                          'autoincrement']:
                try:
                    pkg_descriptor_elements[thing].append(metadata[thing])
                except KeyError:
                    pkg_descriptor_elements[thing] = [metadata[thing]]
    return(pkg_descriptor_elements)


def flatten_data_package_metadata(pkg_bundle_dir,
                                  pkg_name='pudl-all'):
    """
    Convert a bundle of PULD data package metadata into one file.

    Args:
        pkg_bundle_dir (path-like): the subdirectory where the bundle of data
            packages live
        pkg_name (str): the name you choose for the flattened data package.

    Returns:
        dict: pkg_descriptor

    """
    # grab the peices of metadata from each of the data packages
    pkg_descriptor_elements = compile_data_packages_metadata(pkg_bundle_dir,
                                                             pkg_name=pkg_name)
    # the beginning of the data package descriptor
    pkg_descriptor = {
        'name': pkg_name,
        'title': 'flattened bundle of pudl data packages',
    }
    # the uuid for the individual data packages should be exactly the same
    if not len(set(pkg_descriptor_elements['id'])) == 1:
        raise AssertionError(
            'too many ids found in data packages metadata')
    # for these pkg_descriptor items, they should all be the same, so we are
    # just going to grab the first item for the flattened metadata
    for item in ['id', 'licenses', 'homepage']:
        pkg_descriptor[item] = pkg_descriptor_elements[item][0]
    # we're gonna grab the first 'created' timestap (each dp generates it's own
    # timestamp, which are slightly different)
    pkg_descriptor['created'] = min(pkg_descriptor_elements['created'])
    # these elements are dictionaries that have different items inside of them
    # we want to grab all of the elements and have no duplicates
    for item in ['sources', 'contributors', 'resources']:
        # flatten the list of dictionaries
        list_of_dicts = \
            [item for sublist in pkg_descriptor_elements[item]
                for item in sublist]
        if item == 'contributors':
            # turn the dict elements into tuple/sets to de-dupliate it
            pkg_descriptor[item] = [dict(y) for y in set(
                tuple(x.items()) for x in list_of_dicts)]
        elif item == 'sources' or 'resources':
            item_list = []
            # pull only one of each dataset creating a dictionary with the
            # dataset as the key
            for i in dict([(item_dict['title'], item_dict)
                           for item_dict in list_of_dicts]).values():
                item_list.append(i)
            pkg_descriptor[item] = item_list
    pkg_descriptor['autoincrement'] = pudl.helpers.merge_dicts(
        pkg_descriptor_elements['autoincrement'])
    return(pkg_descriptor)


def get_all_sources(pkg_descriptor_elements):
    """Grab list of all of the datasets in a data package bundle."""
    titles = set()
    for sources in pkg_descriptor_elements['sources']:
        for source in sources:
            titles.add(source['title'])
    return(titles)


def get_same_source_meta(pkg_descriptor_elements, title):
    """Grab the the source metadata of the same dataset from all datapackages."""
    samezies = []
    for sources in pkg_descriptor_elements['sources']:
        for source in sources:
            if source['title'] == title:
                samezies.append(source)
    return(samezies)


def check_for_matching_parameters(pkg_bundle_dir, pkg_name):
    """
    Check to see if the ETL parameters for datasets are the same across dp's.

    Args:
        pkg_bundle_dir (path-like): the subdirectory where the bundle of data
            packages live
        pkg_name (str): the name you choose for the flattened data package.

    """
    logger.info('Checking for matching ETL parameters across data packages')
    # grab all of the metadata components
    pkg_descriptor_elements = compile_data_packages_metadata(pkg_bundle_dir,
                                                             pkg_name=pkg_name)
    # grab all of the "titles" (read sources)
    titles = get_all_sources(pkg_descriptor_elements)
    # check if
    for title in titles:
        samezies = get_same_source_meta(pkg_descriptor_elements, title)
        # for each of the source dictionaries, check if they are the same
        for source_dict in samezies:
            if not samezies[0] == source_dict:
                raise AssertionError(f'parameters do not match for {title}')


def flatten_pudl_datapackages(pudl_settings,
                              pkg_bundle_name,
                              pkg_name='pudl-all'):
    """
    Combines a collection of PUDL data packages into one.

    Args:
        pkg_bundle_name (str): the name of the subdirectory where the bundle of
            data packages live. Normally, this name will have been generated in
            `generate_data_packages`.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        pkg_name (str): the name you choose for the flattened data package.

    Returns:
        dict: a dictionary of the data package validation report.

    """
    # determine the subdirectory for the package bundles...
    pkg_bundle_dir = pathlib.Path(pudl_settings['datapackage_dir'],
                                  pkg_bundle_name)
    if not os.path.exists(pkg_bundle_dir):
        raise AssertionError(
            "The datapackage bundle directory does not exist. ")

    # check that data packages that have the same sources have the same parameters
    check_for_matching_parameters(pkg_bundle_dir, pkg_name)

    # copy the csv's into a new data package directory
    flatten_data_packages_csvs(pkg_bundle_dir,
                               pkg_name=pkg_name)
    # generate a flattened dp metadata descriptor
    pkg_descriptor = flatten_data_package_metadata(pkg_bundle_dir,
                                                   pkg_name=pkg_name)
    # using the pkg_descriptor, validate and save the data package metadata
    report = pudl.load.metadata.validate_save_pkg(
        pkg_descriptor,
        pkg_dir=pathlib.Path(pkg_bundle_dir, pkg_name))
    return(report)
