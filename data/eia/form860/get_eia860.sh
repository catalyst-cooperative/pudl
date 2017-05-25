#!/bin/sh

# This script will re-download all of the EIA860 data directly from EIA.

EIA860_XLS_URL="https://www.eia.gov/electricity/data/eia860/xls"
START_YEAR=2001
END_YEAR=2015
DIR=`dirname $0`

for yr in `seq $START_YEAR $END_YEAR`
do (
    fn=eia860$yr
    base=$DIR/$fn
    echo "Downloading EIA 860 data for $yr"
    curl -s $EIA860_XLS_URL/$base.zip -o $base.zip
    echo "Downloaded EIA 860 data for $yr"
    mkdir -p $base
    mv $base.zip $base
    pushd $base
        unzip -q $fn.zip
    popd
    # Make the data store read only for safety
    chmod -R a-w $base ) &
done
wait
