#!/bin/bash

# This script is intended to be run from within the docker
# container and to run the ETL pipeline with given configuration.

# Configuration file to use should be set as ENV variable PUDL_SETTINGS
# and defaults to /pudl/src/release/data-release.sh

set -e

# TODO(rousik): uhm, running within conda is fragile, maybe we can work around
# this somehow?

conda init bash
eval "$(conda shell.bash hook)"
source activate pudl-dev

pudl_setup --pudl_in /pudl/inputs --pudl_out /pudl/outputs
ferc1_to_sqlite --clobber $PUDL_SETTINGS
pudl_etl --clobber $PUDL_SETTINGS

# TODO: Build zenodo archives from the above. If the settings
# file is not for the standard release, then the tar commands
# may need to be different.
# cd /pudl/outputs
# mkdir zenodo-archive
# for dir in pudl-ferc1 pudl-eia860-eia923 pudl-eia860-eia923-epacems; do
#   tar -czf zenodo-archive/${dir}.tgz datapkg/pudl-data-release/${dir}
# done
#
# Additionally we copy bunch of files to repro this (this should not be
# necessary).
# load-pudl.sh is a conveniences script that unpacks the zenodo archive
# and spins up pudl environment to use with this.
