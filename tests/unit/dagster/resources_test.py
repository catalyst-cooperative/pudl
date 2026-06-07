"""Unit tests for Dagster resources."""

import json

import pytest
from dagster import build_init_resource_context
from requests import HTTPError

from pudl.dagster import resources as dagster_resources
from pudl.dagster.resources import ZulipNotificationResource


@pytest.fixture
def zulip_resource() -> ZulipNotificationResource:
    return ZulipNotificationResource.from_resource_context(
        build_init_resource_context(
            config={
                "base_url": "https://zulip.example.com",
                "bot_email": "bot@example.com",
                "api_key": "test-key",  # pragma: allowlist secret
                "timeout_seconds": 9,  # non-standard timeout to test that it propagates
            }
        )
    )


def test_zulip_notification_resource_sends_stream_message_successfully(
    mocker, zulip_resource
):
    """Zulip resource should post expected payload and return API response."""
    response = mocker.Mock()
    response.json.return_value = {"result": "success", "id": 123}
    post = mocker.patch.object(
        dagster_resources.requests,
        "post",
        return_value=response,
    )

    payload = zulip_resource.send_stream_message(
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


def test_zulip_notification_resource_returns_error_on_http_failure(
    mocker, zulip_resource
):
    """Zulip resource should return error dict on HTTP error, not raise."""
    response = mocker.Mock()
    response.raise_for_status.side_effect = HTTPError("bad gateway")
    mocker.patch.object(dagster_resources.requests, "post", return_value=response)

    payload = zulip_resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload["result"] == "error"
    assert "bad gateway" in payload["msg"]


def test_zulip_notification_resource_logs_on_zulip_error_payload(
    mocker, zulip_resource
):
    """Zulip resource should return error payload, not raise RuntimeError."""
    response = mocker.Mock()
    response.json.return_value = {"result": "error", "msg": "invalid stream"}
    mocker.patch.object(dagster_resources.requests, "post", return_value=response)

    payload = zulip_resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload == {"result": "error", "msg": "invalid stream"}


def test_zulip_notification_resource_handles_connection_error(mocker, zulip_resource):
    """Zulip resource should return error dict on connection error, not raise."""
    mocker.patch.object(
        dagster_resources.requests,
        "post",
        side_effect=HTTPError("Connection refused"),
    )

    payload = zulip_resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload["result"] == "error"
    assert "Connection refused" in payload["msg"]


def test_zulip_notification_resource_handles_bad_json(mocker, zulip_resource):
    """Zulip resource should handle non-JSON 200 response without crashing."""
    response = mocker.Mock()
    response.json.side_effect = json.JSONDecodeError("bad json", "", 0)
    mocker.patch.object(dagster_resources.requests, "post", return_value=response)

    payload = zulip_resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload["result"] == "error"
    assert "invalid JSON" in payload["msg"]
    assert "response_text" in payload


def test_zulip_notification_resource_uploads_and_sends_message(
    mocker, tmp_path, zulip_resource
):
    """Zulip resource should upload file and append link to message content."""
    # Create a temp file to "upload"
    csv_file = tmp_path / "status.csv"
    csv_file.write_text("table,row_count\ncore_ferceqr__contracts,42\n")

    # Mock the upload POST
    upload_response = mocker.Mock()
    upload_response.json.return_value = {
        "result": "success",
        "url": "/user_uploads/1/abc/status.csv",
        "filename": "status.csv",
    }

    # Mock the message POST
    msg_response = mocker.Mock()
    msg_response.json.return_value = {"result": "success", "id": 456}

    mocked_post = mocker.patch.object(
        dagster_resources.requests,
        "post",
        side_effect=[upload_response, msg_response],
    )

    payload = zulip_resource.send_stream_message(
        stream="pudl-deployments",
        topic="FERC EQR Builds",
        content="Build failed!",
        file_path=str(csv_file),
    )

    assert payload == {"result": "success", "id": 456}

    # First call: upload
    assert (
        mocked_post.call_args_list[0]
        .args[0]
        .startswith("https://zulip.example.com/api/v1/user_uploads")
    )
    assert "file" in mocked_post.call_args_list[0].kwargs["files"]

    # Second call: message with appended download link
    second_call = mocked_post.call_args_list[1]
    assert second_call.args[0] == "https://zulip.example.com/api/v1/messages"
    msg_data = second_call.kwargs["data"]
    assert msg_data["content"].endswith(
        "[status.csv](https://zulip.example.com/user_uploads/1/abc/status.csv)"
    )
