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
# For local development it's best that you create your own
# Auth0 project so we don't accidently muck with the production
# auth0 information.
# You can follow the instructions here: https://auth0.com/docs/get-started/auth0-overview/create-applications
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
docker compose --profile local up
```

If this is the first time running superset locally or you recently ran `docker compose down` you'll need to run the commands in `setup.sh`.

## Making changes to the production deployment
We'll need to occasionally make changes to the production deployment from our local machines. For example, we might need to debug Superset upgrades, apply migrations to the Superset database or update the predefined roles. We'll use the [Cloud Auth Proxy](https://cloud.google.com/sql/docs/postgres/connect-auth-proxy) to connect the local `superset` service to the production Cloud SQL database.

First, run this command to authenticate with GCP:

```
gcloud auth application-default login
```

The command should emit this message:
```
Credentials saved to file: [/path_to_gcloud_config/application_default_credentials.json]
```

Create an environment variable to store this path:

```
export GOOGLE_GHA_CREDS_PATH={/path_to_gcloud_config/application_default_credentials.json}
```

This credential file will be mounted to the `cloud_auth_proxy` docker service so it can authenticate with GCP.

Next you'll need to set a number of database connection environment variables. You can grab the
database password and Cloud SQL connection name using these commands:

```
gcloud secrets versions access latest --secret="superset-database-password"
gcloud secrets versions access latest --secret="superset-database-connection-name"
```

and then set the environment variables:

```
export SUPERSET_DB_HOST=cloud_auth_proxy
export SUPERSET_DB_PORT=5432
export SUPERSET_DB_USER=postgres
export SUPERSET_DB_PASS={Grab the password from GCP Secret Manager}
export SUPERSET_DB_NAME=postgres
export CLOUD_SQL_CONNECTION_NAME={Grab the database connection name from GCP Secret Manager}
```

Finally, run:

```
docker compose --profile prod up
```


## Deploy to Cloud Run
Once you've made changes to the superset docker image, you can update the production deployment with this command:

```
gcloud builds submit --config cloudbuild.yaml .
```

This command will use Cloud Build to build the docker image, push it to the Google Cloud Artifact Registry and redeploy the Cloud Run `pudl-superset` service with the new docker image.

## Update roles with the `superset` CLI
Superset admins can create, delete and edit roles via the Superset UI. They can also change them using the Superset CLI
which is helpful when adding a large number permissions to a new or existing role.

To do this, make sure you are running the docker services and are connected to the production database.
Then, drop into the superset container:

```
docker compose exec superset bash
```

Export the roles as a json file:

```
superset fab export-roles --path roles/roles.json
```

Edit the roles.json and then import the roles into the production database:

```
superset fab export-roles --path roles/roles.json
```

I've noticed the importing can sometimes take a few minutes.

## How to programmatically create PUDL table dashboards

We've designed a dashboard template for filtering and downloading PUDL tables. The `./automation/create_table_dashboards.py` script
programmatically creates one of these dashboards and all the charts it depends on.

To use this script you'll first need to assign the `superset-bot` credentials to env vars:

```
export SUPERSET_USERNAME=superset-bot
export SUPERSET_PASSWORD={grab password from Google Secrets}
```

Then, to create a dashboard, run:

```
python ./automation/create_table_dashboards.py [TABLE_NAMES]...
```

### Limitations / Open questions

Generally the script is good enough for managing a few dashboards while we do user testing.
It needs a lot of work if we want to use it in production:

- There is no "create all the tables option". We could import this information from our metadata
- This script creates the dataset, chart and dashboard from scratch so you'll have to delete everything if you want to update a dashboard.
- It's still unclear how we'll want to programmatically update these charts. I think the best way to do it is to have a canonical
  template chart that we edit in the UI, use the API to grab the configuration of the dashboard, recreate the `table_download_position.json` jinja template
  and recreate all the dashboard elements.
- The dashboard template does not add table descriptions to the Data Dictionary tab of the dashboard and it does not add filters
- The script does not automatically publish the dashboard
- For some reason, when the "Public" role has any permissions, the API authenticate as an anonymous user and throws an [error](https://github.com/apache/superset/discussions/18284) when sending POST requests.
