#!/bin/sh

# This script will re-download all of the FERC Form 1 data from FERC.

FERC1_URL="ftp://eforms1.ferc.gov/f1allyears"

# Electronic FERC Form 1 data begins in 1994, and is typically finalized
# 9 months after the end of the preceding year.
START_YEAR=1994
END_YEAR=2015
DIR=`dirname $0`

for yr in `seq $START_YEAR $END_YEAR`
do (
    fn=f1_$yr
    base=$DIR/$fn
    echo "Downloading FERC Form 1 data for $yr"
    curl -s $FERC1_URL/$fn.zip -o $base.zip
    echo "Downloaded FERC Form 1 data for $yr"
    mkdir -p $base
    mv $base.zip $base

    pushd $base
        unzip -q $fn.zip


        for topdir in UPLOADERS FORMSADMIN
        do
            if [ -d $base/$topdir ]
            then
                mv $base/$topdir/FORM1/working/* $base
                rmdir $base/$topdir/FORM1/working
                rmdir $base/$topdir/FORM1
                rmdir $base/$topdir
            fi
        done
    popd

    # Make the data store read only for safety
    chmod -R a-w $base ) &
done
wait
