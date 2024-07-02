"""PUDL's Superset configuration."""

import logging
import os

from flask_appbuilder.security.manager import (
    AUTH_OAUTH,
)

LOG_LEVEL = logging.DEBUG

AUTH_TYPE = AUTH_OAUTH

AUTH0_CLIENT_ID = os.getenv("AUTH0_CLIENT_ID")
AUTH0_CLIENT_SECRET = os.getenv("AUTH0_CLIENT_SECRET")
AUTH0_SERVER_METADATA_URL = os.getenv("AUTH0_SERVER_METADATA_URL")

OAUTH_PROVIDERS = [
    {
        "name": "auth0",
        "token_key": "access_token",  # Name of the token in the response of access_token_url
        "icon": "fa-key",  # Icon for the provider
        "remote_app": {
            "client_id": AUTH0_CLIENT_ID,  # Client Id (Identify Superset application)
            "client_secret": AUTH0_CLIENT_SECRET,  # Secret for this Client Id (Identify Superset application)
            "client_kwargs": {"scope": "openid profile email"},
            "server_metadata_url": AUTH0_SERVER_METADATA_URL,
        },
    }
]

AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Admin"
