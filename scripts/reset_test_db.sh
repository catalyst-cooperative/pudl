#!/bin/sh
psql -U catalyst -c "DROP DATABASE pudl_test;" pudl
psql -U catalyst -c "CREATE DATABASE pudl_test;" pudl
psql -U catalyst -c "DROP DATABASE ferc1_test;" ferc1
psql -U catalyst -c "CREATE DATABASE ferc1_test;" ferc1
