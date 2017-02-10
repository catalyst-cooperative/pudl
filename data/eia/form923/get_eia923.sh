#!/bin/sh

# This script will re-download all of the EIA923 data directly from EIA.

EIA923_XLS_URL="https://www.eia.gov/electricity/data/eia923/xls"

EIA923_ZIPS="f923_2016 \
             f923_2015 \
             f923_2014 \
             f923_2013 \
             f923_2012 \
             f923_2011 \
             f923_2010 \
             f923_2009 \
             f923_2008 \
             f906920_2007 \
             f906920_2006 \
             f906920_2005 \
             f906920_2004 \
             f906920_2003 \
             f906920_2002 \
             f906920_2001 "

for z in $EIA923_ZIPS
do
    curl $EIA923_XLS_URL/{$z}.zip -o '#1.zip'
done

for z in $EIA923_ZIPS
do
    mkdir -p $z
    mv $z.zip $z
    (cd $z; unzip $z)
done

# Make the data store read only for safety
chmod -R a-w *

# Not part of the data store:
chmod 0755 get_eia923.sh
chmod 0644 README.txt
chmod 0644 .gitignore
