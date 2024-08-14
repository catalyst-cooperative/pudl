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

AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "GammaSQLLab"


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
