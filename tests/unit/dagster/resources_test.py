"""Unit tests for Dagster resources."""

import json

from dagster import build_init_resource_context
from requests import HTTPError

from pudl.dagster import resources as dagster_resources
from pudl.dagster.resources import ZulipNotificationResource


def test_zulip_notification_resource_sends_stream_message_successfully(mocker):
    """Zulip resource should post expected payload and return API response."""
    init_context = build_init_resource_context(
        config={
            "base_url": "https://zulip.example.com",
            "bot_email": "bot@example.com",
            "api_key": "test-key",  # pragma: allowlist secret
            "timeout_seconds": 9,
        }
    )
    resource = ZulipNotificationResource.from_resource_context(init_context)

    response = mocker.Mock()
    response.json.return_value = {"result": "success", "id": 123}
    post = mocker.patch.object(
        dagster_resources.requests,
        "post",
        return_value=response,
    )

    payload = resource.send_stream_message(
        stream="pudl-deployments",
        topic="FERC EQR Builds",
        content="hello",
    )

    assert payload == {"result": "success", "id": 123}
    post.assert_called_once_with(
        "https://zulip.example.com/api/v1/messages",
        auth=("bot@example.com", "test-key"),
        data={
            "type": "stream",
            "to": "pudl-deployments",
            "topic": "FERC EQR Builds",
            "content": "hello",
        },
        timeout=9,
    )
    response.raise_for_status.assert_called_once_with()


def test_zulip_notification_resource_returns_error_on_http_failure(mocker):
    """Zulip resource should return error dict on HTTP error, not raise."""
    init_context = build_init_resource_context(
        config={
            "base_url": "https://zulip.example.com",
            "bot_email": "bot@example.com",
            "api_key": "test-key",  # pragma: allowlist secret
        }
    )
    resource = ZulipNotificationResource.from_resource_context(init_context)

    response = mocker.Mock()
    response.raise_for_status.side_effect = HTTPError("bad gateway")
    mocker.patch.object(dagster_resources.requests, "post", return_value=response)

    payload = resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload["result"] == "error"
    assert "bad gateway" in payload["msg"]


def test_zulip_notification_resource_logs_on_zulip_error_payload(mocker):
    """Zulip resource should return error payload, not raise RuntimeError."""
    init_context = build_init_resource_context(
        config={
            "base_url": "https://zulip.example.com",
            "bot_email": "bot@example.com",
            "api_key": "test-key",  # pragma: allowlist secret
        }
    )
    resource = ZulipNotificationResource.from_resource_context(init_context)

    response = mocker.Mock()
    response.json.return_value = {"result": "error", "msg": "invalid stream"}
    mocker.patch.object(dagster_resources.requests, "post", return_value=response)

    payload = resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload == {"result": "error", "msg": "invalid stream"}


def test_zulip_notification_resource_handles_connection_error(mocker):
    """Zulip resource should return error dict on connection error, not raise."""
    init_context = build_init_resource_context(
        config={
            "base_url": "https://zulip.example.com",
            "bot_email": "bot@example.com",
            "api_key": "test-key",  # pragma: allowlist secret
        }
    )
    resource = ZulipNotificationResource.from_resource_context(init_context)

    mocker.patch.object(
        dagster_resources.requests,
        "post",
        side_effect=HTTPError("Connection refused"),
    )

    payload = resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload["result"] == "error"
    assert "Connection refused" in payload["msg"]


def test_zulip_notification_resource_handles_bad_json(mocker):
    """Zulip resource should handle non-JSON 200 response without crashing."""
    init_context = build_init_resource_context(
        config={
            "base_url": "https://zulip.example.com",
            "bot_email": "bot@example.com",
            "api_key": "test-key",  # pragma: allowlist secret
        }
    )
    resource = ZulipNotificationResource.from_resource_context(init_context)

    response = mocker.Mock()
    response.json.side_effect = json.JSONDecodeError("bad json", "", 0)
    mocker.patch.object(dagster_resources.requests, "post", return_value=response)

    payload = resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload["result"] == "error"
    assert "invalid JSON" in payload["msg"]
