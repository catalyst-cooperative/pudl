#!/usr/bin/bash
# This script runs the entire ferceqr ETL and validation tests in a docker container on a Google Compute Engine instance.
# This script won't work locally because it needs adequate GCP permissions.

function initialize_postgres() {
    echo "initializing postgres."
    # This is sort of a fiddly set of postgres admin tasks:
    #
    # 1. start the dagster cluster, which is set to be owned by mambauser in the Dockerfile
    # 2. create a db within this cluster so we can do things
    # 3. tell it to actually fail when we mess up, instead of continuing blithely
    # 4. create a *dagster* user, whose creds correspond with those in docker/dagster.yaml
    # 5. make a database for dagster, which is owned by the dagster user
    pg_ctlcluster "$PG_VERSION" dagster start &&
        createdb -h127.0.0.1 -p5433 &&
        psql -v "ON_ERROR_STOP=1" -h127.0.0.1 -p5433 &&
        psql -c "CREATE USER dagster WITH SUPERUSER PASSWORD 'dagster_password'" -h127.0.0.1 -p5433 &&
        psql -c "CREATE DATABASE dagster OWNER dagster" -h127.0.0.1 -p5433
}

function authenticate_gcp() {
    # Set the default gcloud project id so the gcloud storage operations know what project to bill
    echo "Authenticating to GCP"
    gcloud config set project "$GCP_BILLING_PROJECT"
}

function run_ferceqr_etl() {
    echo "Running FERC EQR ETL"
    initialize_postgres &&
        authenticate_gcp &&
        dagster dev &
    dagster job backfill -j ferceqr_etl
    inotifywait -e create -t 18000 --include 'SUCCESS|FAILURE' "$PUDL_OUTPUT"
    killall dagster
}

run_ferceqr_etl 2>&1

# This needs to happen regardless of the ETL outcome:
pg_ctlcluster "$PG_VERSION" dagster stop 2>&1
