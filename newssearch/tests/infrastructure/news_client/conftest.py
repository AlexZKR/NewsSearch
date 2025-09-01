from unittest.mock import MagicMock

import pytest

from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.transport.requests_transport import AbstractHTTPTransport


@pytest.fixture()
def transport(request: pytest.FixtureRequest) -> MagicMock:
    mock = MagicMock(spec=AbstractHTTPTransport)

    if "returns" in request.param:
        mock.request.return_value = request.param["returns"]
    if "raises" in request.param:
        mock.request.side_effect = request.param["raises"]
    return mock


@pytest.fixture()
def news_client(transport) -> NewsClient:
    return NewsClient(transport=transport)


@pytest.fixture()
def news_client_paths_mock(request: pytest.FixtureRequest) -> MagicMock:
    """Mock NewsClient.get_paths_file"""
    mock = MagicMock(spec=NewsClient)

    if "returns" in request.param:
        mock.get_paths_file.return_value = request.param["returns"]
    if "raises" in request.param:
        mock.get_paths_file.side_effect = request.param["raises"]
    return mock
