from unittest.mock import patch

import pytest

from newssearch.config.settings import HTTPTransportSettings
from newssearch.infrastructure.transport.base import AbstractSyncHTTPTransport
from newssearch.infrastructure.transport.requests_transport import RequestsHTTPTransport


@pytest.fixture()
def transport() -> AbstractSyncHTTPTransport:
    settings = HTTPTransportSettings(chunk_size=3)
    return RequestsHTTPTransport(client_settings=settings)


@pytest.fixture()
def mock_session(request: pytest.FixtureRequest):
    if "returns" in request.param:
        with patch("requests.Session.send", return_value=request.param["returns"]):
            yield
    if "raises" in request.param:
        with patch("requests.Session.send", side_effect=request.param["raises"]):
            yield
