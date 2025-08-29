import pytest

from newssearch.infrastructure.transport.base_transport import BaseHTTPTransport


@pytest.fixture()
def transport() -> BaseHTTPTransport:
    return BaseHTTPTransport()
