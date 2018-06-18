#!/bin/bash

echo "installing dbs"

psql -U postgres -c "CREATE ROLE catalyst WITH LOGIN;"
psql -U postgres -c "CREATE DATABASE pudl_test WITH OWNER=catalyst ENCODING='UTF8';"
psql -U postgres -c "CREATE DATABASE ferc1_test WITH OWNER=catalyst ENCODING='UTF8';"

echo "done"
exit 0
