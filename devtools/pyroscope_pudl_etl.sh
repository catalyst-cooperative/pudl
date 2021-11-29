#!/usr/bin/bash

pyroscope exec --log-level info --spy-name pyspy \
    --application-name pudl ferc1_to_sqlite --clobber $1 && \
pyroscope exec --log-level info --spy-name pyspy \
    --application-name pudl pudl_etl --clobber $1
