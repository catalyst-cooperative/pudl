"""Tests for the zenodo data release script."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from pudl.scripts.zenodo_data_release import (
    SANDBOX,
    ZenodoClient,
)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Disable time.sleep to keep retry tests fast."""

    monkeypatch.setattr("pudl.scripts.zenodo_data_release.time.sleep", lambda *_: None)


@pytest.fixture
def zenodo_client(monkeypatch):
    """Return a ZenodoClient configured for sandbox uploads."""

    monkeypatch.setenv("ZENODO_SANDBOX_TOKEN_PUBLISH", "fake-token")
    return ZenodoClient(SANDBOX)


def _fake_response(status_code: int, payload: dict | None = None) -> SimpleNamespace:
    payload = payload or {}

    def _json():
        return payload

    return SimpleNamespace(status_code=status_code, text="ok", json=_json)


def test_retry_request_retries_on_502(monkeypatch, zenodo_client):
    """Ensure retry_request retries transient HTTP errors before succeeding."""

    responses = [
        _fake_response(502),
        _fake_response(200, {"result": "success"}),
    ]

    def fake_request(*, method, url, timeout, **kwargs):  # noqa: ARG001
        return responses.pop(0)

    monkeypatch.setattr(
        "pudl.scripts.zenodo_data_release.requests.request",
        fake_request,
    )

    resp = zenodo_client.retry_request(method="GET", url="https://example.com")
    assert resp.json()["result"] == "success"


def test_create_bucket_file_reopens_stream(monkeypatch, zenodo_client, tmp_path):
    """create_bucket_file should re-read the entire payload on each retry."""

    data = b"hello-world"
    file_path = tmp_path / "test.bin"
    file_path.write_bytes(data)

    calls: list[bytes] = []

    def fake_put(url, *, headers, data, stream, timeout):  # noqa: ARG001
        payload = data.read()
        calls.append(payload)
        if len(calls) == 1:
            raise requests.ConnectionError("boom")
        return _fake_response(200)

    monkeypatch.setattr(
        "pudl.scripts.zenodo_data_release.requests.put",
        fake_put,
    )

    response = zenodo_client.create_bucket_file(
        bucket_url="https://sandbox.zenodo.org/api/files/123",
        file_path=file_path,
        max_tries=3,
    )

    assert len(calls) == 2
    assert all(payload == data for payload in calls)
    assert response.status_code == 200
