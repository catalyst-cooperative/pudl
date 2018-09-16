#!/usr/bin/env python
"""
Script to create MSHA data package for PUDL.
"""

import sys
import os
import datapackage
import goodtables
import tableschema
import json
import hashlib
import datetime
import argparse
import urllib
import shutil
from pprint import pprint

import pandas as pd

sys.path.append(os.path.abspath(os.path.join('..', '..', '..', '..')))


def main(arguments):
    from pudl import init, datastore, settings
    from pudl.transform.pudl import fix_int_na
    import pudl.constants as pc

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '-d', '--download',
        help="Download fresh data directly from MSHA.",
        default=False, action='store_true')
    parser.add_argument(
        '-r', '--row_limit',
        help="Maximum number of rows to use in data validation.",
        default=10000, action='store', type=int)

    args = parser.parse_args(arguments)

    # Construct some paths we'll need later...
    input_dir = os.path.join(settings.PUDL_DIR, "scripts", "data_pkgs",
                             "msha", "pudl-msha")

    # Generate package output directories based on name of the data package
    output_dir = os.path.join(settings.PUDL_DIR, "results", "data_pkgs",
                              "msha", "pudl-msha")

    archive_dir = os.path.join(output_dir, "archive")
    os.makedirs(archive_dir, exist_ok=True)

    scripts_dir = os.path.join(output_dir, "scripts")
    os.makedirs(scripts_dir, exist_ok=True)

    data_dir = os.path.join(output_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # Dictionary with one element pertaining to each of the data resources
    # that are going to be part of the output data package. The initial data
    # and defs sub-dictionary elements will be joined by other useful items
    # that exist for each of the data resources as we go.
    #  - "data" the filename of the original data file from MSHA
    #  - "defs" the filename of the data definition file from MSHA
    #  - "data_df" pandas dataframe containing the MSHA data.
    #  - "defs_df" pandas dataframe containing the MSHA file definition.
    #  - "resource" is a datapackage.Resource() object.
    #  - "json" is a JSON data package descriptor
    resources = {
        "mines": {
            "data": "Mines.zip",
            "defs": "Mines_Definition_File.txt"
        },
        "controller-operator-history": {
            "data": "ControllerOperatorHistory.zip",
            "defs": "Controller_Operator_History_Definition_File.txt"
        },
        "employment-production-quarterly": {
            "data": "MinesProdQuarterly.zip",
            "defs": "MineSProdQuarterly_Definition_File.txt"
        }
        # "contractor-employment-production-quarterly": {
        #       "data": "ContractorProdQuarterly.zip",
        #       "defs": "ContractorProdQuarterly_Definition_File.txt"
        #   }
    }

    if args.download:
        # Get the data directly from MSHA
        data_path = pc.base_data_urls["mshamines"]
        for res in resources:
            for d in ["data", "defs"]:
                # Construct the full URL
                url_parts = urllib.parse.urlparse(
                    pc.base_data_urls['mshamines'])
                new_path = url_parts.path + '/' + resources[res][d]
                res_url = urllib.parse.urlunparse(
                    list(url_parts[0:2]) + [new_path, '', '', ''])

                # Download the data file to data_dir
                print(f"Downloading {res_url}")
                urllib.request.urlretrieve(
                    res_url,
                    filename=os.path.join(archive_dir, resources[res][d])
                )
    else:
        # Get the data from our local PUDL datastore.
        data_path = os.path.join(settings.DATA_DIR, "msha")
        for res in resources:
            for d in ["data", "defs"]:
                src_file = os.path.join(data_path, resources[res][d])
                dst_file = os.path.join(archive_dir, resources[res][d])
                shutil.copyfile(src_file, dst_file)

    for res in resources:
        # Create dataframes from input data & definition files (local or remote):
        for d in ['data', 'defs']:
            resources[res][f"{d}_df"] = \
                pd.read_csv(f"{archive_dir}/{resources[res][d]}",
                            delimiter="|",
                            encoding="iso-8859-1")
        # Read the input tabular data resource JSON file we've prepared
        resources[res]["json"] = json.load(
            open(os.path.join(input_dir, f"{res}.json")))

    # OMFG even the MSHA data is broken. *sigh*
    resources["employment-production-quarterly"]["data_df"].columns = \
        list(resources["employment-production-quarterly"]
             ["defs_df"]['COLUMN_NAME'])

    # Create a data package to contain our resources, based on the template
    # JSON file that we have already prepared as an input.
    pkg = datapackage.Package(os.path.join(input_dir, "datapackage.json"))

    for res in resources:
        # Convert the definitions to a dictionary of field descriptions
        field_desc = resources[res]["defs_df"].set_index(
            'COLUMN_NAME').to_dict()['FIELD_DESCRIPTION']

        # Set the description attribute of the fields in the schema using field descriptions.
        for field in resources[res]["json"]["schema"]["fields"]:
            field['description'] = field_desc[field['name']]
        resources[res]["resource"] = datapackage.Resource(
            descriptor=resources[res]["json"])

        # Make sure we didn't miss or re-name any fields accidentally
        json_fields = resources[res]["resource"].schema.field_names
        defs_fields = list(resources[res]["defs_df"]['COLUMN_NAME'])
        data_fields = list(resources[res]['data_df'].columns)
        assert json_fields == defs_fields, "json vs. defs missing field: {}".format(
            set(json_fields).symmetric_difference(set(defs_fields)))
        assert data_fields == defs_fields, "data vs. defs missing field: {}".format(
            set(data_fields).symmetric_difference(set(defs_fields)))
        resources[res]["resource"].infer()
        resources[res]["resource"].commit()

        # Need to clean up the integer NA values in the data before outputting:
        for field in resources[res]["resource"].schema.field_names:
            if resources[res]["resource"].schema.get_field(field).type == 'integer':
                resources[res]["data_df"][field] = fix_int_na(
                    resources[res]["data_df"][field])

        # Force boolean values to use canonical True/False values.
        for field in resources[res]["resource"].schema.field_names:
            if resources[res]["resource"].schema.get_field(field).type == 'boolean':
                resources[res]["data_df"][field] = resources[res]["data_df"][field].replace(
                    'Y', True)
                resources[res]["data_df"][field] = resources[res]["data_df"][field].replace(
                    'N', False)

        # the data itself goes in output -- this is what we're packaging up
        output_csv = os.path.join(data_dir, f"{res}.csv")
        resources[res]['data_df'].to_csv(output_csv, index=False)

        # calculate some useful information about the output file, and add it to the resource:
        # resource file size:
        resources[res]["resource"].descriptor["bytes"] = os.path.getsize(
            output_csv)

        # resource file hash:
        BLOCKSIZE = 65536
        hasher = hashlib.sha1()
        with open(output_csv, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)

        resources[res]["resource"].descriptor["hash"] = f"sha1:{hasher.hexdigest()}"

        # Check our work...
        print("Validating {} tabular data resource".format(
            resources[res]['resource'].descriptor['name']))
        if not resources[res]["resource"].valid:
            print(f"TABULAR DATA RESOURCE {res} IS NOT VALID.")
            return 1

        # Add the completed resource to the data package
        pkg.add_resource(
            descriptor=resources[res]["resource"].descriptor)

    # Automatically fill in some additional metadata
    pkg.infer()

    # Timestamp indicating when packaging occured
    pkg.descriptor['created'] = datetime.datetime.utcnow().replace(
        microsecond=0).isoformat() + 'Z'
    # Have to set this to 'data-package' rather than 'tabular-data-package' due to a DataHub.io bug
    pkg.descriptor['profile'] = 'data-package'
    pkg.commit()

    # save the datapackage
    print("Validating pudl-msha data package")
    if not pkg.valid:
        print("PUDL MSHA DATA PACKAGE IS NOT VALID.")
        return 1
    pkg.save(os.path.join(output_dir, 'datapackage.json'))

    # Validate some of the data...
    print("Validating pudl-msha data")
    report = goodtables.validate(os.path.join(
        output_dir, 'datapackage.json'), row_limit=args.row_limit)
    if not report['valid']:
        print("PUDL MSHA DATA TABLES FAILED TO VALIDATE")
        pprint(report)
        return 1

    shutil.copyfile(os.path.join(input_dir, "README.md"),
                    os.path.join(output_dir, "README.md"))
    shutil.copyfile(os.path.join(input_dir, sys.argv[0]),
                    os.path.join(output_dir, "scripts", sys.argv[0]))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
