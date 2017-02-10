#!/bin/sh

# This script will re-download all of the EIA923 data directly from EIA.

EIA860_XLS_URL="https://www.eia.gov/electricity/data/eia860/xls"
START_YEAR=2001
END_YEAR=2015

for yr in `seq $START_YEAR $END_YEAR`
do
    base=eia860$yr
    curl $EIA860_XLS_URL/$base.zip -o $base.zip
    mkdir -p $base
    mv $base.zip $base
    (cd $base; unzip $base.zip)
done

# Make the data store read only for safety
chmod -R a-w *

# Not part of the data store:
chmod 0755 get_eia860.sh
chmod 0644 README.txt
chmod 0644 .gitignore
