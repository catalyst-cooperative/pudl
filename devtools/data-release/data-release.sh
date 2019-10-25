#!/bin/sh

###############################################################################
# Create, activate, and record the pudl-data-release conda environment
###############################################################################
PUDL_VERSION=0.2.1
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p ~/miniconda
export PATH=~/miniconda/bin:$PATH
eval "$(conda shell.bash hook)"
conda init bash
conda config --set channel_priority strict
conda env create --file data-release-env.yml
#    catalystcoop.pudl=$PUDL_VERSION
conda activate pudl-data-release
git clone --depth 1 https://github.com/catalyst-cooperative/pudl.git # --branch $PUDL_VERSION

pip install --editable ./pudl
conda env export > export-environment.yml

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

cd pudl
tox -v -e validate > ../data-validation.log
cd ..
#touch README.txt
# Make sure everyone has the right
#cp README.txt datapkg/pudl-data-release/pudl-ferc1
#cp README.txt datapkg/pudl-data-release/pudl-eia860-eia923/
#cp README.txt datapkg/pudl-data-release/pudl-eia860-eia923-epacems/
#
#tar -cvzf pudl-input-data.tgz data/
#tar -cvzf pudl-ferc1.tgz datapkg/pudl-data-release/pudl-ferc1/
#tar -cvzf pudl-eia860-eia923.tgz datapkg/pudl-data-release/pudl-eia860-eia923/
#tar -cvzf pudl-eia860-eia923-epacems.tgz datapkg/pudl-data-release/pudl-eia860-eia923-epacems/
