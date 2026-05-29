"""Unit tests for Dagster resources."""

import pytest
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


def test_zulip_notification_resource_raises_on_http_error(mocker):
    """Zulip resource should bubble HTTP errors from requests."""
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

    with pytest.raises(HTTPError, match="bad gateway"):
        resource.send_stream_message(stream="stream", topic="topic", content="body")


def test_zulip_notification_resource_raises_on_zulip_error_payload(mocker):
    """Zulip resource should reject non-success Zulip API payloads."""
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

    with pytest.raises(RuntimeError, match="Zulip message failed"):
        resource.send_stream_message(stream="stream", topic="topic", content="body")
