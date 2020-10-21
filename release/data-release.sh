#!/bin/bash

# This script is intended to be run from within the docker
# container and to run the ETL pipeline with given configuration.

# Configuration file to use should be set as ENV variable PUDL_SETTINGS
# and defaults to /pudl/src/release/data-release.sh

pudl_setup --pudl_in /pudl/inputs --pudl_out /pudl/outputs
ferc1_to_sqlite --clobber --sandbox $PUDL_SETTINGS
pudl_etl --clobber --sandbox $PUDL_SETTINGS
