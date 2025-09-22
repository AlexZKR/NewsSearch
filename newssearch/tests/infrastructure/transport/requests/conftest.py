from io import BytesIO
from unittest.mock import patch

import pytest
from requests import Response

from newssearch.config.settings import HTTPTransportSettings
from newssearch.infrastructure.transport.base import AbstractSyncHTTPTransport
from newssearch.infrastructure.transport.requests_transport import RequestsHTTPTransport
from newssearch.infrastructure.transport.schemas import ContentTypeEnum


@pytest.fixture()
def transport_requests() -> AbstractSyncHTTPTransport:
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


def get_exp_response(
    content_type: ContentTypeEnum | None = None,
    content: str | None = None,
    status_code: int = 200,
) -> Response:
    r = Response()
    r.status_code = status_code
    if content:
        data = content.encode()
        r._content = data
        r.raw = BytesIO(data)
        r.headers["content-length"] = str(len(data))
    if content_type:
        r.headers["content-type"] = content_type.value
    return r
