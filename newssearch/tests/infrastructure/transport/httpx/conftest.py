from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import AsyncClient, Request, Response

from newssearch.config.settings import HTTPTransportSettings
from newssearch.infrastructure.transport.aiohttp_transport import (
    HttpxHTTPTransport,
)
from newssearch.infrastructure.transport.schemas import ContentTypeEnum


@pytest_asyncio.fixture()
async def transport_httpx(client):
    transport_settings = HTTPTransportSettings(chunk_size=3)
    return HttpxHTTPTransport(client, transport_settings)


@pytest_asyncio.fixture()
async def client(request: pytest.FixtureRequest):
    mock = AsyncMock(spec=AsyncClient)

    if "returns" in request.param:
        mock.send.return_value = request.param["returns"]
        yield mock
    if "raises" in request.param:
        mock.send.side_effect = request.param["raises"]
        yield mock


def get_exp_response(
    content_type: ContentTypeEnum | None = None,
    content: str | None = None,
    status_code: int = 200,
) -> Response:
    r = Response(status_code)
    r.request = Request(method="GET", url="test")
    r.status_code = status_code
    if content:
        data = content.encode()
        r._content = data
        r._content = data
        r.headers["content-length"] = str(len(data))
    if content_type:
        r.headers["content-type"] = content_type.value
    return r
