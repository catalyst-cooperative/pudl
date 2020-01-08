"""
Make me metadata!!!.

Lists of dictionaries of dictionaries of lists, forever. This module enables the
generation and use of the metadata for tabular data packages. This module also
saves and validates the datapackage once the metadata is compiled. The intented
use of the module is to use it *after* generating the CSV's via `etl.py`.

On a basic level, based on the settings in the pkg_settings, tables and sources
associated with a data package, we are compiling information about the data
package. For the table metadata, we are pulling from the megadata
(`src/pudl/package_data/meta/datapkg/datapackage.json`). Most of the other
elements of the metadata is regenerated.

For most tables, this is a relatively straightforward process, but we are
attempting to enable partioning of tables (storing parts of a table in
individual CSVs). These partitioned tables are parts of a "group" which can be
read by frictionlessdata tools as one table. At each step the process, this
module needs to know whether to deal with the full partitioned table names or
the cononical table name.
"""

import datetime
import hashlib
import importlib
import json
import logging
import os
import pathlib
import re
import shutil
import uuid

import datapackage
import goodtables
import pkg_resources

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)

##############################################################################
# CREATING PACKAGES AND METADATA
##############################################################################


def hash_csv(csv_path):
    """Calculates a SHA-256 hash of the CSV file for data integrity checking.

    Args:
        csv_path (path-like) : Path the CSV file to hash.

    Returns:
        str: the hexdigest of the hash, with a 'sha256:' prefix.

    """
    # how big of a bit should I take?
    blocksize = 65536
    # sha256 is the fastest relatively secure hashing algorith.
    hasher = hashlib.sha256()
    # opening the file and eat it for lunch
    with open(csv_path, 'rb') as afile:
        buf = afile.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(blocksize)

    # returns the hash
    return f"sha256:{hasher.hexdigest()}"


def compile_partitions(datapkg_settings):
    """
    Pull out the partitions from data package settings.

    Args:
        datapkg_settings (dict): a dictionary containing package settings
            containing top level elements of the data package JSON descriptor
            specific to the data package

    Returns:
        dict:

    """
    partitions = {}
    for dataset in datapkg_settings['datasets']:
        for dataset_name in dataset:
            try:
                partitions.update(dataset[dataset_name]['partition'])
            except KeyError:
                pass
    return(partitions)


def get_unpartitioned_tables(tables, datapkg_settings):
    """
    Get the tables w/out the partitions.

    Because the partitioning key will always be the name of the table without
    whatever element the table is being partitioned by, we can assume the names
    of all of the un-partitioned tables to get a list of tables that is easier
    to work with.

    Args:
        tables (iterable): list of tables that are included in this datapackage.
        datapkg_settings (dictionary):

    Returns:
        iterable: tables_unpartitioned is a set of un-partitioned tables

    """
    partitions = compile_partitions(datapkg_settings)
    tables_unpartitioned = set()
    if partitions:
        for table in tables:
            for part in partitions.keys():
                if part in table:
                    tables_unpartitioned.add(part)
                else:
                    tables_unpartitioned.add(table)
        return tables_unpartitioned
    else:
        return tables


def package_files_from_table(table, datapkg_settings):
    """Determine which files should exist in a package cooresponding to a table.

    We want to convert the datapackage tables and any information about package
    partitioning into a list of expected files. For each table that is
    partitioned, we want to add the partitions to the end of the table name.

    """
    partitions = compile_partitions(datapkg_settings)
    files = []
    for dataset in datapkg_settings['datasets']:
        try:
            partitions[table]
        except KeyError:
            if table not in files:
                files.append(table)
            continue
        try:
            for dataset_name in dataset:
                if dataset[dataset_name]['partition']:
                    for part in dataset[dataset_name][partitions[table]]:
                        file = table + "_" + str(part)
                        files.append(file)
        except KeyError:
            pass
    return(files)


def get_repartitioned_tables(tables, partitions, datapkg_settings):
    """
    Get the re-partitioned tables.

    Args:
        tables (list): a list of tables that are included in this data package.
        partitions (dict)
        datapkg_settings (dict): a dictionary containing package settings
            containing top level elements of the data package JSON descriptor
            specific to the data package.

    Returns:
        list: list of tables including full groups of

    """
    flat_datapkg_settings = pudl.etl.get_flattened_etl_parameters(
        [datapkg_settings])
    tables_repartitioned = []
    for table in tables:
        if partitions:
            for part in partitions.keys():
                if part is table:
                    for part_separator in flat_datapkg_settings[partitions[part]]:
                        tables_repartitioned.append(
                            table + "_" + str(part_separator))
                else:
                    tables_repartitioned.append(table)
    return tables_repartitioned


def data_sources_from_tables(table_names):
    """
    Look up data sources based on a list of PUDL tables.

    Args:
        tables_names (iterable): a list of names of 'seed' tables, whose
            dependencies we are seeking to find.

    Returns:
        set: The set of data sources for the list of PUDL table names.

    """
    all_tables = get_dependent_tables_from_list(
        table_names)
    table_sources = set()
    # All tables get PUDL:
    table_sources.add('pudl')
    for t in all_tables:
        for src in pc.data_sources:
            if re.match(f".*_{src}$", t):
                table_sources.add(src)

    return table_sources


def get_foreign_key_relash_from_datapkg(datapkg_json):
    """Generate a dictionary of foreign key relationships from pkging metadata.

    This function helps us pull all of the foreign key relationships of all
    of the tables in the metadata.

    Args:
        datapkg_json (path-like): Path to the datapackage.json
            containing the schema from which the foreign key relationships
            will be read

    Returns:
        dict: list of foreign key tables

    """
    with open(datapkg_json) as md:
        metadata = json.load(md)

    fk_relash = {}
    for tbl in metadata['resources']:
        fk_relash[tbl['name']] = []
        if 'foreignKeys' in tbl['schema']:
            fk_tables = []
            for fk in tbl['schema']['foreignKeys']:
                fk_tables.append(fk['reference']['resource'])
            fk_relash[tbl['name']] = fk_tables
    return(fk_relash)


def get_dependent_tables_datapkg(table_name, fk_relash):
    """
    For a given table, get the list of all the other tables it depends on.

    Args:
        table_name (str): The table whose dependencies we are looking for.
        fk_relash ():

    Todo:
        Incomplete docstring.

    Returns:
        set: the set of all the tables the specified table depends upon.

    """
    # Add the initial table
    dependent_tables = set()
    dependent_tables.add(table_name)

    # Get the list of tables this table depends on:
    new_table_names = set()
    new_table_names.update(fk_relash[table_name])

    # Recursively call this function on the tables our initial
    # table depends on:
    for table_name in new_table_names:
        logger.debug(f"Finding dependent tables for {table_name}")
        dependent_tables.add(table_name)
        for t in get_dependent_tables_datapkg(table_name, fk_relash):
            dependent_tables.add(t)

    return dependent_tables


def get_dependent_tables_from_list(table_names):
    """Given a list of tables, find all the other tables they depend on.

    Iterate over a list of input tables, adding them and all of their dependent
    tables to a set, and return that set. Useful for determining which tables
    need to be exported together to yield a self-contained subset of the PUDL
    database.

    Args:
        table_names (iterable): a list of names of 'seed' tables, whose
            dependencies we are seeking to find.

    Returns:
        all_the_tables (set): The set of all the tables which any of the input
        tables depends on, via ForeignKey constraints.

    """
    with importlib.resources.path('pudl.package_data.meta.datapkg',
                                  'datapackage.json') as md:
        fk_relash = get_foreign_key_relash_from_datapkg(md)

        all_the_tables = set()
        for t in table_names:
            for x in get_dependent_tables_datapkg(t, fk_relash):
                all_the_tables.add(x)

    return all_the_tables


def test_file_consistency(tables, datapkg_settings, datapkg_dir):
    """
    Test the consistency of tables for packaging.

    The purpose of this function is to test that we have the correct list of
    tables. There are three different ways we could determine which tables are
    being dumped into packages: a list of the tables being generated through
    the ETL functions, the list of dependent tables and the list of CSVs in
    package directory.

    Currently, this function is supposed to be fed the ETL function tables
    which are tested against the CSVs present in the package directory.

    Args:
        datapkg_name (string): the name of the data package.
        tables (list): a list of table names to be tested.
        datapkg_dir (path-like): the directory in which to check the
            consistency of table files

    Raises:
        AssertionError: If the tables in the CSVs and the ETL tables are not
            exactly the same list of tables.

    Todo:
        Determine what to do with the dependent tables check.

    """
    datapkg_name = datapkg_settings['name']
    # remove the '.csv' or the '.csv.gz' from the file names
    file_tbls = [re.sub(r'(\.csv.*$)', '', x) for x in os.listdir(
        os.path.join(datapkg_dir, 'data'))]
    # given list of table names and partitions, generate list of expected files
    datapkg_files = tables

    dependent_tbls = list(get_dependent_tables_from_list(tables))
    etl_tbls = tables

    dependent_tbls.sort()
    file_tbls.sort()
    datapkg_files.sort()
    etl_tbls.sort()
    # TODO: determine what to do about the dependent_tbls... right now the
    # dependent tables include some glue tables for FERC in particular, but
    # we are imagining the glue tables will be in another data package...
    if (file_tbls == datapkg_files):  # & (dependent_tbls == etl_tbls)):
        logger.info(f"Tables are consistent for {datapkg_name} package")
    else:
        inconsistent_tbls = []
        for tbl in file_tbls:
            if tbl not in datapkg_files:
                inconsistent_tbls.extend(tbl)
                raise AssertionError(f"{tbl} from CSVs not in ETL tables")

        raise AssertionError(
            f"Tables are inconsistent. "
            f"Missing tables include: {inconsistent_tbls}")


def pull_resource_from_megadata(resource_name):
    """
    Read a single data resource from the PUDL metadata library.

    Args:
        resource_name (str): the name of the tabular data resource whose JSON
            descriptor we are reading.

    Returns:
        json: a Tabular Data Resource Descriptor, as a JSON object.

    Raises:
        ValueError: If table_name is not found exactly one time in the PUDL
            metadata library.

    """
    with importlib.resources.open_text('pudl.package_data.meta.datapkg',
                                       'datapackage.json') as md:
        metadata_mega = json.load(md)
    # bc we partition the CEMS output, the CEMS table name includes the state,
    # year or other partition.. therefor we need to assume for the sake of
    # grabing metadata that any table name that includes the table name is cems
    if "hourly_emissions_epacems" in resource_name:
        table_name_mega = "hourly_emissions_epacems"
    else:
        table_name_mega = resource_name
    table_resource = [
        x for x in metadata_mega['resources'] if x['name'] == table_name_mega
    ]

    if len(table_resource) == 0:
        raise ValueError(f"{resource_name} not found in stored metadata.")
    if len(table_resource) > 1:
        raise ValueError(f"{resource_name} found multiple times in metadata.")
    table_resource = table_resource[0]
    # rename the resource name to the og table name
    # this is important for the partitioned tables in particular
    table_resource['name'] = resource_name
    return(table_resource)


def get_date_from_sources(sources, date_to_grab):
    """
    Grab the years from a source's parameters and convert to a date.

    Args:
        sources (iterable): this is a list of source dictionaries. should be
            the result of the get_source_metadata() function.
        date_to_grab (string): the name of the date metadata to extract.
            Currently, this is only either 'start_date' or 'end_date'.

    Returns:
        string : date formatted as 'YYYY-MM-DD' or None

    """
    # if there are no sources, then we grab nothing
    if len(sources) == 0:
        return None
    # if there are two or more sources, then we won't know which source to grab
    # the date from so this will fail.
    if len(sources) != 1:
        raise AssertionError(
            "You should only be grabbing dates from one source at a time."
        )
    for key, param in sources[0]['parameters_pudl'].items():
        # grab the parameters that contains the years so we can assume
        # start/end dates
        if "years" in key:
            if date_to_grab == 'start_date':
                return str(min(param)) + "-01-01"
            elif date_to_grab == 'end_date':
                return str(max(param)) + "-21-31"


def spatial_coverage(resource_name):
    """
    Extract spatial coverage (country and state) for a given source.

    Args:
        resource_name (str): The name of the (potentially partitioned) resource
            for which we are enumerating the spatial coverage. Currently this
            is the only place we are able to access the partitioned spatial
            coverage after the ETL process has completed.

    Returns:
        dict: A dictionary containing country and potentially state level
        spatial coverage elements. Country keys are "country" for the full name
        of country, "iso_3166-1_alpha-2" for the 2-letter ISO code, and
        "iso_3166-1_alpha-3" for the 3-letter ISO code. State level elements
        are "state" (a two letter ISO code for sub-national jurisdiction) and
        "iso_3166-2" for the combined country-state code conforming to that
        standard.

    """
    coverage = {
        "country": "United States of America",
        # More generally... ISO 3166-1 2-letter country code:
        "iso_3166-1_alpha-2": "US",
        # More generally... ISO 3166-1 3-letter country code:
        "iso_3166-1_alpha-3": "USA",
    }
    if "hourly_emissions_epacems" in resource_name:
        us_state = resource_name.split("_")[4].upper()
        coverage["state"] = us_state
        # ISO3166-2:US code for the relevant state or outlying area:
        coverage["iso_3166-2"] = f"US-{us_state}"
    return coverage


def temporal_coverage(resource_name, datapkg_settings):
    """Extract start and end dates from ETL parameters for a given source.

    Args:
        resource_name (str): The name of the (potentially partitioned) resource
            for which we are enumerating the spatial coverage. Currently this
            is the only place we are able to access the partitioned spatial
            coverage after the ETL process has completed.
        datapkg_settings (dict): Python dictionary represeting the ETL
            parameters read in from the settings file, pertaining to the
            tabular datapackage this resource is part of.

    Returns:
        dict: A dictionary of two items, keys "start_date" and "end_date" with
        values in ISO 8601 YYYY-MM-DD format, indicating the extent of the
        time series data contained within the resource. If the resource does
        not contain time series data, the dates are null.

    """
    start_date = None
    end_date = None
    if "hourly_emissions_epacems" in resource_name:
        year = resource_name.split("_")[3]
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
    else:
        source_years = f"{resource_name.split('_')[-1]}_years"
        for dataset in datapkg_settings["datasets"]:
            etl_params = list(dataset.values())[0]
            try:
                start_date = f"{min(etl_params[source_years])}-01-01"
                end_date = f"{max(etl_params[source_years])}-12-31"
                break
            except KeyError:
                continue

    return {"start_date": start_date, "end_date": end_date}


def get_tabular_data_resource(resource_name, datapkg_dir,
                              datapkg_settings, partitions=False):
    """
    Create a Tabular Data Resource descriptor for a PUDL table.

    Based on the information in the database, and some additional metadata this
    function will generate a valid Tabular Data Resource descriptor, according
    to the Frictionless Data specification, which can be found here:
    https://frictionlessdata.io/specs/tabular-data-resource/

    Args:
        resource_name (string): name of the tabular data resource for which you
            want to generate a Tabular Data Resource descriptor. This is the
            resource name, rather than the database table name, because we
            partition large tables into resource groups consisting of many
            files.
        datapkg_dir (path-like): The location of the directory for this
            package. The data package directory will be a subdirectory in the
            `datapkg_dir` directory, with the name of the package as the name
            of the subdirectory.
        datapkg_settings (dict): Python dictionary represeting the ETL
            parameters read in from the settings file, pertaining to the
            tabular datapackage this resource is part of.
        partitions (dict): Fuck if I know what this is.

    Returns:
        dict: A Python dictionary representing a tabular data resource
        descriptor that complies with the Frictionless Data specification.

    """
    # Only some datasets have meaningful temporal coverage:
    # temporal_data = ["eia860", "eia923", "ferc1", "eia861", "epacems"]
    # every time we want to generate the cems table, we want it compressed
    if "hourly_emissions_epacems" in resource_name:
        abs_path = pathlib.Path(datapkg_dir, "data", f"{resource_name}.csv.gz")
    else:
        abs_path = pathlib.Path(datapkg_dir, "data", f"{resource_name}.csv")

    # pull the skeleton of the descriptor from the megadata file
    descriptor = pull_resource_from_megadata(resource_name)
    descriptor["path"] = str(abs_path.relative_to(abs_path.parent.parent))
    descriptor["bytes"] = abs_path.stat().st_size
    descriptor["hash"] = hash_csv(abs_path)
    descriptor["created"] = (
        datetime.datetime.utcnow()
        .replace(microsecond=0)
        .isoformat() + "Z"
    )
    unpartitioned_tables = get_unpartitioned_tables([resource_name],
                                                    datapkg_settings)
    data_sources = data_sources_from_tables(unpartitioned_tables)
    descriptor["sources"] = [pc.data_source_info[src] for src in data_sources]
    descriptor["coverage"] = {
        "temporal": temporal_coverage(resource_name, datapkg_settings),
        "spatial": spatial_coverage(resource_name),
    }

    if partitions:
        for part in partitions.keys():
            if part in resource_name:
                descriptor["group"] = part

    resource = datapackage.Resource(descriptor)

    if resource.valid:
        logger.debug(f"{resource_name} is a valid resource")
    else:
        logger.info(resource)
        raise ValueError(
            f"""
            Invalid tabular data resource descriptor: {resource.name}

            Errors:
            {resource.errors}
            """
        )

    return descriptor


def get_source_metadata(data_sources, datapkg_settings):
    """
    Lookup metadata for data sources included in a specified datapackage.

    Args:
        data_sources (iterable): data source codes
        datapkg_settings (dict):

    Returns:
        list: A list of dictionaries appropriate for populating the "sources"
        element of a tabular datapackage, including the "title" and "path" for
        each of the PUDL data sources mentioned in the input datapkg_settings.
        (e.g. eia923, ferc1).

    """
    sources = []
    for src in data_sources:
        if src in pc.data_sources:
            src_meta = pc.data_source_info[src].copy()
            for dataset_dict in datapkg_settings['datasets']:
                for dataset in dataset_dict:
                    # because we have defined eia as a dataset, but 860 and 923
                    # are separate sources, either the dataset must be or be in
                    # the source
                    if dataset in src or dataset == src:
                        src_meta['parameters_pudl'] = dataset_dict[dataset]
            sources.append(src_meta)
    return sources


def get_keywords_from_sources(data_sources):
    """Grab keywords for the metadata based on data sources."""
    keywords = set()
    for src in data_sources:
        keywords.update(pc.keywords_by_data_source[src])
    return list(keywords)


def get_autoincrement_columns(unpartitioned_tables):
    """Grab the autoincrement columns for pkg tables."""
    with importlib.resources.open_text('pudl.package_data.meta.datapkg',
                                       'datapackage.json') as md:
        metadata_mega = json.load(md)
    autoincrement = {}
    for table in unpartitioned_tables:
        try:
            autoincrement[table] = metadata_mega['autoincrement'][table]
        except KeyError:
            pass
    return autoincrement


def validate_save_datapkg(datapkg_descriptor, datapkg_dir):
    """
    Validate a data package descriptor and save it to a json file.

    Args:
        datapkg_descriptor (dict): A Python dictionary representation of a
            (hopefully valid) tabular datapackage descriptor.
        datapkg_dir (path-like): Directory into which the datapackage.json
            file containing the tabular datapackage descriptor should be
            written.

    Returns:
        dict: A dictionary containing the goodtables datapackage validation
        report.

    """
    # Use that descriptor to instantiate a Package object
    datapkg = datapackage.Package(datapkg_descriptor)

    # Validate the data package descriptor before we go to
    logger.info(
        f"Validating JSON descriptor for {datapkg.descriptor['name']} "
        f"tabular data package...")
    if not datapkg.valid:
        raise ValueError(
            f"Invalid tabular data package: {datapkg.descriptor['name']} "
            f"Errors: {datapkg.errors}")
    logger.info('JSON descriptor appears valid!')

    # pkg_json is the datapackage.json that we ultimately output:
    datapkg_json = pathlib.Path(datapkg_dir, "datapackage.json")
    datapkg.save(str(datapkg_json))
    logger.info(
        f"Validating a sample of data from {datapkg.descriptor['name']} "
        f"tabular data package using goodtables...")
    # Validate the data within the package using goodtables:
    report = goodtables.validate(
        datapkg_json,
        # TODO: check which checks are applied... and uncomment out the line
        # below when the checks are integrated
        # checks=['structure', 'schema', 'foreign-key'],
        # table_limit=100,
        row_limit=1000)
    if not report["valid"]:
        goodtables_errors = ""
        for table in report["tables"]:
            if not table["valid"]:
                goodtables_errors += str(table["source"])
                goodtables_errors += str(table["errors"])
        raise ValueError(
            f"Data package data validation failed with goodtables. "
            f"Errors: {goodtables_errors}"
        )
    logger.info('Congrats! You made a valid data package!')
    return report


def generate_metadata(datapkg_settings,
                      datapkg_resources,
                      datapkg_dir,
                      datapkg_bundle_uuid=None,
                      datapkg_bundle_doi=None):
    """
    Generate metadata for package tables and validate package.

    The metadata for this package is compiled from the pkg_settings and from
    the "megadata", which is a json file containing the schema for all of the
    possible pudl tables. Given a set of tables, this function compiles
    metadata and validates the metadata and the package. This function assumes
    datapackage CSVs have already been generated.

    See Frictionless Data for the tabular data package specification:
    http://frictionlessdata.io/specs/tabular-data-package/

    Args:
        datapkg_settings (dict): a dictionary containing package settings
            containing top level elements of the data package JSON descriptor
            specific to the data package including:
            * name: short package name e.g. pudl-eia923, ferc1-test
            * title: One line human readable description.
            * description: A paragraph long description.
            * version: the version of the data package being published.
            * keywords: For search purposes.
        datapkg_resources (list): The names of tabular data resources that are
            included in this data package.
        datapkg_dir (path-like): The location of the directory for this
            package. The data package directory will be a subdirectory in the
            `datapkg_dir` directory, with the name of the package as the
            name of the subdirectory.
        datapkg_bundle_uuid: A type 4 UUID identifying the ETL run which
            which generated the data package -- this indicates that the data
            packages are compatible with each other
        datapkg_bundle_doi: A digital object identifier (DOI) that will be used
            to archive the bundle of mutually compatible data packages. Needs
            to be provided by an archiving service like Zenodo. This field may
            also be added after the data package has been generated.

    Returns:
        dict: a Python dictionary representing a valid tabular data package
        descriptor.

    """
    # Create a tabular data resource for each of the input resources:
    resources = []
    partitions = compile_partitions(datapkg_settings)
    for resource in datapkg_resources:
        resources.append(get_tabular_data_resource(
            resource,
            datapkg_dir=datapkg_dir,
            datapkg_settings=datapkg_settings,
            partitions=partitions)
        )

    datapkg_tables = get_unpartitioned_tables(
        datapkg_resources, datapkg_settings)
    data_sources = data_sources_from_tables(datapkg_tables)

    contributors = set()
    for src in data_sources:
        for c in pc.contributors_by_source[src]:
            contributors.add(c)

    # Fields which we are requiring:
    datapkg_descriptor = {
        "name": datapkg_settings["name"],
        "id": str(uuid.uuid4()),
        "profile": "tabular-data-package",
        "title": datapkg_settings["title"],
        "description": datapkg_settings["description"],
        "keywords": get_keywords_from_sources(data_sources),
        "homepage": "https://catalyst.coop/pudl/",
        "created": (datetime.datetime.utcnow().
                    replace(microsecond=0).isoformat() + 'Z'),
        "contributors": [pc.contributors[c] for c in contributors],
        "sources": [pc.data_source_info[src] for src in data_sources],
        "etl-parameters-pudl": datapkg_settings["datasets"],
        "licenses": [pc.licenses["cc-by-4.0"]],
        "autoincrement": get_autoincrement_columns(datapkg_tables),
        "python-package-name": "catalystcoop.pudl",
        "python-package-version":
            pkg_resources.get_distribution('catalystcoop.pudl').version,
        "resources": resources,
    }

    # Optional fields:
    try:
        datapkg_descriptor["version"] = datapkg_settings["version"]
    except KeyError:
        pass

    # The datapackage bundle UUID indicates packages can be used together
    if datapkg_bundle_uuid is not None:
        # Check to make sure it's a valid Type 4 UUID.
        # If it's not the right kind of hex value or string, this will fail:
        val = uuid.UUID(datapkg_bundle_uuid, version=4)
        # If it's nominally a Type 4 UUID, but these come back different,
        # something is wrong:
        if uuid.UUID(val.hex, version=4) != uuid.UUID(str(val), version=4):
            raise ValueError(
                f"Got invalid type 4 UUID: {datapkg_bundle_uuid} "
                f"as bundle ID for data package {datapkg_settings['name']}."
            )
        # Guess it looks okay!
        datapkg_descriptor["datapkg-bundle-uuid"] = datapkg_bundle_uuid

    # Check the proffered DOI, if any, against this regex, taken from the
    # idutils python package:
    if datapkg_bundle_doi is not None:
        if not pudl.helpers.is_doi(datapkg_bundle_doi):
            raise ValueError(
                f"Got invalid DOI: {datapkg_bundle_doi} "
                f"as bundle DOI for data package {datapkg_settings['name']}."
            )
        datapkg_descriptor["datapkg-bundle-doi"] = datapkg_bundle_doi

    _ = validate_save_datapkg(datapkg_descriptor, datapkg_dir)
    return datapkg_descriptor


def prep_directory(dir_path, clobber=False):
    """
    Create (or delete and create) data package directory.

    Args:
        dir_path (path-like): path to the directory that you are trying to
            clean and prepare.
        clobber (bool): If True and dir_path exists, it will be removed and
            replaced with a new, empty directory.

    Raises:
        FileExistsError: if a file or directory already exists at dir_path.

    Returns:
        path-like: dir_path.

    """
    if os.path.exists(dir_path) and (clobber is False):
        raise FileExistsError(
            f'{dir_path} already exists and clobber is set to {clobber}')
    elif os.path.exists(dir_path) and (clobber is True):
        shutil.rmtree(dir_path)
    os.mkdir(dir_path)
    return(dir_path)
