#!/usr/bin/bash
mkdir -p devtools/profiles/

py-spy record -r 10 -f speedscope -o "devtools/profiles/$2" -- pudl_etl --clobber $1
