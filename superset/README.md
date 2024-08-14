# PUDL Superset
This directory contains files required to build and deploy PUDL's Superset instance.

## Local deployment
To test out a local deployment build the images:

```
docker compose build
```

Before you start the service you'll need to set some environment variables

```
# These auth0 are required for authentication
# You can find these values in the the catalyst-cooperative-pudl GCP Secret Manager
export AUTH0_CLIENT_ID="auth0 client id"
export AUTH0_CLIENT_SECRET="auth0 client secret"
export AUTH0_DOMAIN="auth0 client domain"

# Set the connection details
export SUPERSET_DB_HOST=postgres
export SUPERSET_DB_PORT=5432
export SUPERSET_DB_USER=superset
export SUPERSET_DB_PASS=superset
export SUPERSET_DB_NAME=superset
```

Then you can start the services

```
docker compose up
```

If this is the first time running superset locally or you recently ran `docker compose down` you'll need to run the commands in `setup.sh`.

## Making changes to the production deployment
TODO: instructions on how to connect to Cloud SQL

## Deploy to Cloud Run
Once you've made changes the superset docker image, you can update the production deploying with this command:

```
gcloud builds submit --config cloudbuild.yaml .
```

This command will use Cloud Build to build the docker image, push it to the Google Cloud Artifcat Registry and redeploy the Cloud Run `pudl-superset` service with the new docker image.
