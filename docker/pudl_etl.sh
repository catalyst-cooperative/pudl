#!/usr/bin/bash

pudl_setup \
    --pudl_in $CONTAINER_PUDL_IN \
    --pudl_out $CONTAINER_PUDL_OUT
ferc1_to_sqlite \
    --clobber \
    --logfile $LOGFILE \
    $PUDL_SETTINGS_YML
pudl_etl \
    --clobber \
    --logfile $LOGFILE \
    $PUDL_SETTINGS_YML
