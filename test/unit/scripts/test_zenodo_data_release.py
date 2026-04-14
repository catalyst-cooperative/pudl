"""Tests for the zenodo data release script."""

import os
from types import SimpleNamespace

import pytest
import requests

from pudl.scripts.zenodo_data_release import (
    RETRYABLE_STATUS_CODES,
    SANDBOX,
    ZenodoClient,
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
