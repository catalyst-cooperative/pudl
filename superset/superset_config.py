"""PUDL's Superset configuration."""

import os

import sqlalchemy as sa
from flask import Flask
from flask_appbuilder.security.manager import (
    AUTH_OAUTH,
)

AUTH_TYPE = AUTH_OAUTH

AUTH0_CLIENT_ID = os.environ["AUTH0_CLIENT_ID"]
AUTH0_CLIENT_SECRET = os.environ["AUTH0_CLIENT_SECRET"]
AUTH0_DOMAIN = os.environ["AUTH0_DOMAIN"]

OAUTH_PROVIDERS = [
    {
        "name": "auth0",
        "token_key": "access_token",  # Name of the token in the response of access_token_url
        "icon": "fa-key",  # Icon for the provider
        "remote_app": {
            "client_id": AUTH0_CLIENT_ID,  # Client Id (Identify Superset application)
            "client_secret": AUTH0_CLIENT_SECRET,  # Secret for this Client Id (Identify Superset application)
            "client_kwargs": {"scope": "openid profile email groups"},
            "server_metadata_url": f"https://{AUTH0_DOMAIN}/.well-known/openid-configuration",
        },
    }
]

# Allow users to sign up
# Give them the "GammaSQLLab" role
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "GammaSQLLab"

# Allow users to view superset without registering
AUTH_ROLE_PUBLIC = "Public"
PUBLIC_ROLE_LIKE = "Gamma"

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,  # Allow for assigning roles to dashboards
    "TAGGING_SYSTEM": True,
    "HORIZONTAL_FILTER_BAR": True,  # Show filter bar on top of dashboard
}

# This limits the number of rows when requesting chart data
# I think it needs to be <= SQL_MAX_ROW because superset
# needs to query the database before it can make a chart
ROW_LIMIT = 100_000
# Maximum number of rows returned for any analytical database query
SQL_MAX_ROW = 100_000

# I'm ont sure how these two settings differ
# row limit for native filters
NATIVE_FILTER_DEFAULT_ROW_LIMIT = 100_000
# max rows retrieved by filter select auto complete
FILTER_SELECT_ROW_LIMIT = 100_000

APP_ICON = "/static/assets/images/catalyst_logo.png"
APP_NAME = "PUDL"
LOGO_RIGHT_TEXT = "PUDL"


def get_db_connection_string() -> str:
    """Get the database connection string."""
    drivername = "postgresql+psycopg2"
    host = os.environ.get("SUPERSET_DB_HOST")
    port = os.environ.get("SUPERSET_DB_PORT")
    username = os.environ["SUPERSET_DB_USER"]
    password = os.environ["SUPERSET_DB_PASS"]
    database = os.environ["SUPERSET_DB_NAME"]

    is_cloud_run = os.environ.get("IS_CLOUD_RUN", False)

    if is_cloud_run:
        cloud_sql_connection_name = os.environ.get("CLOUD_SQL_CONNECTION_NAME")
        # I couldn't figure out how to use unix sockets with the sa.engine.url.URL
        # class so I'm creating the connection string manually
        return f"postgresql+psycopg2://{username}:{password}@/{database}?host=/cloudsql/{cloud_sql_connection_name}"
    return str(
        sa.engine.url.URL.create(
            drivername=drivername,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
    )


SQLALCHEMY_DATABASE_URI = get_db_connection_string()


def FLASK_APP_MUTATOR(app: Flask) -> None:  # noqa: N802
    """Superset function that allows you to configure the Flask app.

    Args:
        app: The Flask app instance
    """
    app.config.update(
        PREFERRED_URL_SCHEME="https",
    )
