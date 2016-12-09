#!/bin/sh

db="ferc_f1"
yr="2015"
# We're only doing a subset of the files for now:
#  F1_1  => f1_respondent_id
#  F1_31 => f1_fuel
#  F1_33 => f1_gnrt_plant
#  F1_52 => f1_plant_in_srvce
#  F1_53 => f1_pumped_storage
#  F1_54 => f1_purchased_pwr
#  F1_70 => f1_work
#  F1_71 => f1_xmssn_adds
#  F1_79 => f1_col_lit_tbl
#  F1_84 => f1_row_lit_tbl
#  F1_86 => f1_hydro
#  F1_89 => f1_steam
#  F1_398_ANCL_PS => f1_398_ancl_ps
#  F1_S0_FILING_LOG => f1_s0_filing_log

dbfs="F1_1.DBF  \
      F1_31.DBF \
      F1_33.DBF \
      F1_52.DBF \
      F1_53.DBF \
      F1_54.DBF \
      F1_70.DBF \
      F1_71.DBF \
      F1_79.DBF \
      F1_84.DBF \
      F1_86.DBF \
      F1_89.DBF \
      F1_398_ANCL_PS.DBF \
      F1_S0_FILING_LOG.DBF"

for f in $dbfs
do
    pgdbf ../data/ferc/form1/$yr/*/FORM1/working/$f | iconv -c -f ASCII -t UTF8 | psql $db
done
