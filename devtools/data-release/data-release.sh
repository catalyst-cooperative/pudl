#!/bin/bash
# A script to generate a reproducible data release locally.
# Assumes:
#  - Ubuntu Linux OS
#  - libsnappy is installed
#  - miniconda is installed in ~/miniconda3
#  - Should be run from a fresh directory containing only:
#    - this script
#    - the pudl-data-release-env.yml conda environment file
#    - the pudl-data-release-etl.yml PUDL settings file

PUDL_VERSION=0.3.0
START_TIME=$(date --iso-8601="seconds")
EIA860_YEARS="--years 2011 2012 2013 2014 2015 2016 2017 2018"
EIA923_YEARS="--years 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018"
EPACEMS_YEARS="--years 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018"
EPACEMS_STATES=""
FERC1_YEARS="--years 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018"
###############################################################################
# libsnappy needs to be installed for data validation since tox uses pip
###############################################################################
# sudo apt install libsnappy-dev libsnappy1v5

###############################################################################
# Create, activate, and record the pudl-data-release conda environment
###############################################################################
#wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
#bash ~/miniconda.sh -b -p ~/miniconda

###############################################################################
# Create, activate, and record the pudl-data-release conda environment
###############################################################################
echo "======================================================================"
echo $START_TIME
echo "Creating and archiving PUDL conda environment"
echo "======================================================================"
~/miniconda3/bin/conda init bash
export PATH=~/miniconda3/bin:$PATH
eval "$(~/miniconda3/bin/conda shell.bash hook)"
source ~/miniconda3/etc/profile.d/conda.sh
~/miniconda3/bin/conda env remove --name pudl-data-release
~/miniconda3/bin/conda config --set channel_priority strict
~/miniconda3/bin/conda env create \
    --name pudl-data-release \
    --file data-release-env.yml # catalystcoop.pudl=$PUDL_VERSION
source activate pudl-data-release

ACTIVE_CONDA_ENV=$(conda env list | grep '\*' | awk '{print $1}')
echo "Active conda env: $ACTIVE_CONDA_ENV"

# Obtain and install the most recent PUDL commit (or the tagged release...):
git clone --depth 1 https://github.com/catalyst-cooperative/pudl.git # --branch $PUDL_VERSION
pip install --editable ./pudl
# Record exactly which software was installed for ETL:
~/miniconda3/bin/conda env export > export-environment.yml

echo "======================================================================"
date --iso-8601="seconds"
echo "Setting up PUDL data management environment."
echo "======================================================================"
pudl_setup --clobber ./

echo "======================================================================"
date --iso-8601="seconds"
echo "Downloading raw input data."
echo "======================================================================"
pudl_data --sources epacems $EPACEMS_STATES $EPACEMS_YEARS
pudl_data --sources eia860 $EIA860_YEARS
pudl_data --sources eia923 $EIA923_YEARS
pudl_data --sources ferc1 $FERC1_YEARS

echo "======================================================================"
date --iso-8601="seconds"
echo "Cloning FERC Form 1 into SQLite."
echo "======================================================================"
ferc1_to_sqlite data-release-etl.yml

echo "======================================================================"
date --iso-8601="seconds"
echo "Running PUDL ETL"
echo "======================================================================"
pudl_etl data-release-etl.yml

echo "======================================================================"
date --iso-8601="seconds"
echo "Loading FERC 1 & EIA 860/923 data into SQLite for validation."
echo "======================================================================"
# Load the FERC 1 and EIA datapackages into an SQLite DB:
datapkg_to_sqlite \
    datapkg/pudl-data-release/pudl-ferc1/datapackage.json \
    datapkg/pudl-data-release/pudl-eia860-eia923/datapackage.json \
    -o datapkg/pudl-data-release/pudl-merged/

# The CEMS is too large to usefully put into SQLite, so convert to Parquet.
# Note that we are not currently doing detailed validation of the CEMS data,
# but this conversion process will at least catch data type issues.
echo "======================================================================"
date --iso-8601="seconds"
echo "Converting EPA CEMS data to Apache Parquet for validation."
echo "======================================================================"
epacems_to_parquet \
    datapkg/pudl-data-release/pudl-eia860-eia923-epacems/datapackage.json

# Validate the data we've loaded
# Can't actually do this right now because it requires *all* the data
#tox -v -c pudl/tox.ini -e validate #> data-validation.log
# Instead just a a structural test, let's run the Travis tests...
echo "======================================================================"
date --iso-8601="seconds"
echo "Using Tox to validate PUDL data before release."
echo "======================================================================"
# Only want to do this when we're processing all the FERC 1 & EIA data...
tox -v -c pudl/tox.ini -e validate

# This is just for testing purposes... obviously we need a real README
echo "======================================================================"
date --iso-8601="seconds"
echo "Copying data release README files into each data package."
echo "======================================================================"
touch README.txt
cp README.txt datapkg/pudl-data-release/pudl-ferc1
cp README.txt datapkg/pudl-data-release/pudl-eia860-eia923/
cp README.txt datapkg/pudl-data-release/pudl-eia860-eia923-epacems/

echo "======================================================================"
date --iso-8601="seconds"
echo "Archiving raw input data for distribution."
echo "======================================================================"
mkdir zenodo-archive
tar -cvzf zenodo-archive/pudl-input-data.tgz data/

echo "======================================================================"
date --iso-8601="seconds"
echo "Archiving PUDL datapackages for distribution."
echo "======================================================================"
tar -cvzf zenodo-archive/pudl-ferc1.tgz datapkg/pudl-data-release/pudl-ferc1/
tar -cvzf zenodo-archive/pudl-eia860-eia923.tgz datapkg/pudl-data-release/pudl-eia860-eia923/
tar -cvzf zenodo-archive/pudl-eia860-eia923-epacems.tgz datapkg/pudl-data-release/pudl-eia860-eia923-epacems/
cp data-release.sh \
    data-release-etl.yml \
    data-release-env.yml \
    export-environment.yml \
    README.txt \
    zenodo-archive

echo "======================================================================"
END_TIME=$(date --iso-8601="seconds")
ARCHIVE_SIZE=$(du -sh zenodo-archive)
echo "PUDL data release creation and validation complete."
echo "START TIME:" $START_TIME
echo "END TIME:  " $END_TIME
echo "Archive Size:" $ARCHIVE_SIZE
echo "======================================================================"
cp data-release.log zenodo-archive
