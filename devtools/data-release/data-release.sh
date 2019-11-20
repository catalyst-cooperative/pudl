#!/bin/bash

PUDL_VERSION=0.2.1

###############################################################################
# libsnappy needs to be installed for data validation since tox uses pip
###############################################################################
sudo apt install libsnappy-dev libsnappy1v5

###############################################################################
# Create, activate, and record the pudl-data-release conda environment
###############################################################################
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p ~/miniconda
~/miniconda/bin/conda init bash
export PATH=~/miniconda/bin:$PATH
eval "$(~/miniconda/bin/conda shell.bash hook)"
source ~/miniconda/etc/profile.d/conda.sh
echo "conda path:"
echo `which conda`
~/miniconda/bin/conda config --set channel_priority strict
~/miniconda/bin/conda env create --file data-release-env.yml
#    catalystcoop.pudl=$PUDL_VERSION
source ~/miniconda/bin/activate pudl-data-release
git clone --depth 1 https://github.com/catalyst-cooperative/pudl.git # --branch $PUDL_VERSION

pip install --editable ./pudl
~/miniconda/bin/conda env export > export-environment.yml

# Create a new workspace and download the required data:
pudl_setup --clobber ./
pudl_data -s eia860 -y 2011 2012 2013 2014 2015 2016 2017 2018
pudl_data -s eia923 -y 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018
pudl_data -s ferc1 -y 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018
pudl_data -s epacems -t co

# Clone the FERC 1 DB:
ferc1_to_sqlite data-release-etl.yml
# Run the main ETL to generate data packages:
pudl_etl data-release-etl.yml

# Now we have the input files & data packages that we are going to archive,
# However, we still need to validate them, create .tgz files, and upload

# Load those data packages into an SQLite DB:
datapkg_to_sqlite --datapkg_bundle_name pudl-data-release

# Validate the data we've loaded
tox -v -c pudl/tox.ini -e validate #> data-validation.log

touch README.txt
cp README.txt datapkg/pudl-data-release/pudl-ferc1
cp README.txt datapkg/pudl-data-release/pudl-eia860-eia923/
cp README.txt datapkg/pudl-data-release/pudl-eia860-eia923-epacems/

tar -cvzf pudl-input-data.tgz data/
tar -cvzf pudl-ferc1.tgz datapkg/pudl-data-release/pudl-ferc1/
tar -cvzf pudl-eia860-eia923.tgz datapkg/pudl-data-release/pudl-eia860-eia923/
tar -cvzf pudl-eia860-eia923-epacems.tgz datapkg/pudl-data-release/pudl-eia860-eia923-epacems/
