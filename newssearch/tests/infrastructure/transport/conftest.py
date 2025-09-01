from unittest.mock import patch

import pytest

from newssearch.infrastructure.transport.base import AbstractHTTPTransport
from newssearch.infrastructure.transport.requests_transport import BaseHTTPTransport


@pytest.fixture()
def transport() -> AbstractHTTPTransport:
    return BaseHTTPTransport()


@pytest.fixture()
def mock_session(request: pytest.FixtureRequest):
    if "returns" in request.param:
        with patch("requests.Session.send", return_value=request.param["returns"]):
            yield
    if "raises" in request.param:
        with patch("requests.Session.send", side_effect=request.param["raises"]):
            yield
