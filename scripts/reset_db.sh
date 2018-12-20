#!/bin/sh
psql -U catalyst -c "DROP DATABASE pudl;" pudl_test
psql -U catalyst -c "CREATE DATABASE pudl;" pudl_test
psql -U catalyst -c "DROP DATABASE ferc1;" ferc1_test
psql -U catalyst -c "CREATE DATABASE ferc1;" ferc1_test
