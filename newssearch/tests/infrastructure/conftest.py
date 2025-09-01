import pytest

from newssearch.infrastructure.transport.requests_transport import BaseHTTPTransport


@pytest.fixture()
def transport() -> BaseHTTPTransport:
    return BaseHTTPTransport()
