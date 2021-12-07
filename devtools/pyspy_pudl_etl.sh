#!/usr/bin/bash

# This script takes two positional arguments:
# * The first one is the path to the PUDL settings file

PROFILE_DIR=`git rev-parse --show-toplevel`/devtools/profiles
OUTFILE=pyspy_pudl_etl_`date +%Y%m%d%H%M%S`.json
mkdir -p $PROFILE_DIR

py-spy record -r 10 -f speedscope -o "$PROFILE_DIR/$OUTFILE" -- pudl_etl --clobber $1
