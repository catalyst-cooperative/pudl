#!/bin/sh

# This script will re-download all of the FERC Form 1 data from FERC.

FERC1_URL="ftp://eforms1.ferc.gov/f1allyears"

# Electronic FERC Form 1 data begins in 1994, and is typically finalized
# 9 months after the end of the preceding year.
START_YEAR=1994
END_YEAR=2015

for yr in `seq $START_YEAR $END_YEAR`
do
    base=f1_$yr
    echo "Downloading FERC Form 1 data for $yr"
    curl --progress-bar $FERC1_URL/$base.zip -o $base.zip
    mkdir -p $base
    mv $base.zip $base
    (cd $base; unzip -q $base.zip)

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

    # Make the data store read only for safety
    chmod -R a-w $base
done
