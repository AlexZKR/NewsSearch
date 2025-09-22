from unittest.mock import MagicMock

import pytest

from newssearch.infrastructure.clients.news.news_client_sync import NewsClientSync
from newssearch.infrastructure.transport.requests_transport import (
    AbstractSyncHTTPTransport,
)


@pytest.fixture()
def transport(request: pytest.FixtureRequest) -> MagicMock:
    mock = MagicMock(spec=AbstractSyncHTTPTransport)

    if "request_returns" in request.param:
        mock.request.return_value = request.param["request_returns"]
    if "request_raises" in request.param:
        mock.request.side_effect = request.param["request_raises"]

    if "stream_returns" in request.param:
        mock.stream.return_value = request.param["stream_returns"]
    if "stream_raises" in request.param:
        mock.stream.side_effect = request.param["stream_raises"]
    return mock


@pytest.fixture()
def news_client(transport) -> NewsClientSync:
    return NewsClientSync(transport=transport)


@pytest.fixture()
def news_client_paths_mock(request: pytest.FixtureRequest) -> MagicMock:
    """Mock NewsClient.get_paths_file"""
    mock = MagicMock(spec=NewsClientSync)

    if "returns" in request.param:
        mock.get_paths_file.return_value = request.param["returns"]
    if "raises" in request.param:
        mock.get_paths_file.side_effect = request.param["raises"]
    return mock
