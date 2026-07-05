"""Tests for the zenodo data release script."""

import os
from types import SimpleNamespace

import pytest
import requests

from pudl.scripts.zenodo_data_release import (
    PRODUCTION,
    RETRYABLE_STATUS_CODES,
    SANDBOX,
    EmptyDraft,
    ZenodoClient,
    build_zenodo_release_zulip_message,
)


@pytest.fixture(autouse=True)
def _no_sleep(mocker):
    """Disable time.sleep to keep retry tests fast."""

    mocker.patch("pudl.scripts.zenodo_data_release.time.sleep", autospec=True)


@pytest.fixture
def zenodo_client(mocker):
    """Return a ZenodoClient configured for sandbox uploads."""

    mocker.patch.dict(os.environ, {"ZENODO_SANDBOX_TOKEN_PUBLISH": "fake-token"})
    return ZenodoClient(SANDBOX)


def _fake_response(status_code: int, payload: dict | None = None) -> SimpleNamespace:
    """Lightweight stand-in for ``requests.Response``.

    Many tests only need deterministic ``status_code`` and ``json()`` accessors.  Using
    ``SimpleNamespace`` keeps the fixture simple and avoids the internal state a real
    ``requests.Response`` expects. When a genuine response object is required (e.g.,
    attaching one to ``requests.HTTPError``) the ``_requests_response`` helper above is
    used instead.
    """

    payload = payload or {}

    def _json():
        return payload

    return SimpleNamespace(status_code=status_code, text="ok", json=_json)


def _requests_response(status_code: int) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = b""  # noqa: SLF001
    response.url = "https://example.com"
    return response


@pytest.mark.parametrize("status_code", sorted(RETRYABLE_STATUS_CODES))
def test_retry_request_retries_retryable_status_codes(
    mocker, zenodo_client, status_code
):
    """Ensure retry_request retries transient HTTP errors before succeeding."""

    responses = [
        _fake_response(status_code),
        _fake_response(200, {"result": "success"}),
    ]
    mock_request = mocker.patch(
        "pudl.scripts.zenodo_data_release.requests.request",
        side_effect=responses,
    )

    resp = zenodo_client.retry_request(method="GET", url="https://example.com")

    assert resp.json()["result"] == "success"
    assert mock_request.call_count == 2


@pytest.mark.parametrize("status_code", [403, 404, 429])
def test_retry_request_raises_on_non_retryable_status_codes(
    mocker, zenodo_client, status_code
):
    """Ensure retry_request surfaces HTTP errors for non-retryable codes."""

    http_error = requests.HTTPError(
        f"{status_code}", response=_requests_response(status_code)
    )
    mock_request = mocker.patch(
        "pudl.scripts.zenodo_data_release.requests.request",
        side_effect=http_error,
    )

    with pytest.raises(requests.HTTPError):
        zenodo_client.retry_request(
            method="GET", url="https://example.com", max_tries=1
        )

    assert mock_request.call_count == 1


def test_retry_request_errors_after_max_retries(mocker, zenodo_client):
    """Confirm retry_request raises once repeated 502 responses hit max_tries.

    The ``ZenodoCode`` class we're testing here raises its own ``HTTPError`` when a
    non-success status is returned, so we can use ``return_value=_fake_response(502)``
    to keep the mock simpler. We don't need a ``side_effect`` or a real
    ``requests.Response`` object.
    """

    mock_request = mocker.patch(
        "pudl.scripts.zenodo_data_release.requests.request",
        return_value=_fake_response(502),
    )

    max_tries = 3
    with pytest.raises(requests.HTTPError):
        zenodo_client.retry_request(
            method="GET", url="https://example.com", max_tries=max_tries
        )

    assert mock_request.call_count == max_tries


def test_create_bucket_file_reopens_stream(mocker, zenodo_client, tmp_path):
    """Ensure create_bucket_file re-reads the payload for each retry.

    The ``calls`` list captures the bytes returned by ``data.read()`` inside the fake
    ``requests.request`` implementation; observing identical payloads for each entry
    proves the file handle was reopened (and therefore rewound) before every upload
    attempt.
    """

    data = b"hello-world"
    file_path = tmp_path / "test.bin"
    file_path.write_bytes(data)

    calls: list[bytes] = []

    def fake_request(*, method, url, headers, data, stream, timeout):  # noqa: ARG001
        assert method == "PUT"
        payload = data.read()
        calls.append(payload)
        if len(calls) == 1:
            raise requests.ConnectionError("boom")
        return _fake_response(200)

    mocker.patch(
        "pudl.scripts.zenodo_data_release.requests.request",
        side_effect=fake_request,
    )

    response = zenodo_client.create_bucket_file(
        bucket_url="https://sandbox.zenodo.org/api/files/123",
        file_path=file_path,
        max_tries=3,
    )

    assert len(calls) == 2
    assert all(payload == data for payload in calls)
    assert response.status_code == 200


def test_sync_directory_skips_top_level_directories_and_ignored_files(
    mocker, zenodo_client, tmp_path
):
    """Ensure only top-level files that survive ignore regexes are uploaded."""

    keep_file = tmp_path / "keep.txt"
    keep_file.write_text("keep", encoding="utf-8")
    ignored_file = tmp_path / "ignore.parquet"
    ignored_file.write_text("ignored", encoding="utf-8")
    nested_dir = tmp_path / "ferc1_xbrl"
    nested_dir.mkdir()
    (nested_dir / "nested.txt").write_text("nested", encoding="utf-8")

    zenodo_client.get_deposition = mocker.Mock(  # type: ignore[method-assign]
        return_value=SimpleNamespace(
            links=SimpleNamespace(bucket="https://sandbox.zenodo.org/api/files/123")
        )
    )
    zenodo_client.create_bucket_file = mocker.Mock(  # type: ignore[method-assign]
        return_value=_fake_response(200)
    )

    draft = EmptyDraft(record_id=123, zenodo_client=zenodo_client)
    draft.sync_directory(str(tmp_path), ignore=(r".*\.parquet$",))

    uploaded_paths = [
        call.kwargs["file_path"].name
        for call in zenodo_client.create_bucket_file.call_args_list
    ]
    assert uploaded_paths == ["keep.txt"]


def test_get_until_visible_returns_immediately_on_success(mocker, zenodo_client):
    """A 200 on the first try should return immediately, with no retries."""
    mock_retry_request = mocker.patch.object(
        zenodo_client, "retry_request", return_value=_fake_response(200, {"ok": True})
    )

    response = zenodo_client._get_until_visible(url="https://example.com/records/1")

    assert response.status_code == 200
    assert mock_retry_request.call_count == 1


def test_get_until_visible_retries_transient_404_then_succeeds(mocker, zenodo_client):
    """A record ID that isn't visible yet should be tolerated and retried.

    Simulates the eventual-consistency race between Zenodo's legacy and new APIs:
    a freshly created record 404s a couple of times before it becomes visible.
    """
    responses = [
        _fake_response(404),
        _fake_response(404),
        _fake_response(200, {"ok": True}),
    ]
    mock_retry_request = mocker.patch.object(
        zenodo_client, "retry_request", side_effect=responses
    )

    response = zenodo_client._get_until_visible(url="https://example.com/records/1")

    assert response.status_code == 200
    assert mock_retry_request.call_count == 3


def test_get_until_visible_raises_after_persistent_404(mocker, zenodo_client):
    """A 404 that never resolves is a genuine error and should raise."""
    mock_retry_request = mocker.patch.object(
        zenodo_client, "retry_request", return_value=_requests_response(404)
    )

    with pytest.raises(requests.HTTPError):
        zenodo_client._get_until_visible(
            url="https://example.com/records/1", max_tries=3
        )

    assert mock_retry_request.call_count == 3


def test_build_zenodo_release_zulip_message_success_publish():
    """A successful publish run should show PRODUCTION/publish and the live URL."""
    message = build_zenodo_release_zulip_message(
        env=PRODUCTION,
        publish=True,
        succeeded=True,
        record_url="https://zenodo.org/records/12345",
    )

    assert ":check: PUDL Zenodo Release Succeeded" in message
    assert "PRODUCTION" in message
    assert "publish" in message
    assert "Published record: https://zenodo.org/records/12345" in message


def test_build_zenodo_release_zulip_message_success_draft():
    """A successful no-publish run should show SANDBOX/draft and the draft URL."""
    message = build_zenodo_release_zulip_message(
        env=SANDBOX,
        publish=False,
        succeeded=True,
        record_url="https://sandbox.zenodo.org/records/6789",
    )

    assert ":check: PUDL Zenodo Release Succeeded" in message
    assert "SANDBOX" in message
    assert "draft, no-publish" in message
    assert "Draft record: https://sandbox.zenodo.org/records/6789" in message


def test_build_zenodo_release_zulip_message_failure_omits_record_link():
    """A failed run should be clearly marked as failed, with no record link."""
    message = build_zenodo_release_zulip_message(
        env=SANDBOX,
        publish=True,
        succeeded=False,
        record_url=None,
    )

    assert ":x: PUDL Zenodo Release Failed" in message
    assert "record" not in message.lower()
