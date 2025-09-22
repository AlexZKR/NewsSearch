from contextlib import asynccontextmanager
from http import HTTPStatus
from logging import getLogger
from typing import Any

from httpx import AsyncClient, HTTPStatusError, Request, Response

from newssearch.config.settings import HTTPTransportSettings
from newssearch.infrastructure.transport.base import AbstractAsyncHTTPTransport
from newssearch.infrastructure.transport.exceptions import (
    ClientError,
    ConnectionTransportError,
    ServerError,
)
from newssearch.infrastructure.transport.schemas import (
    ContentTypeEnum,
    HTTPRequestData,
    ResponseContent,
)

logger = getLogger(__name__)


class HttpxHTTPTransport(AbstractAsyncHTTPTransport):
    """Based on httpx, for async calls. Retry and backoff support with backoff lib."""

    def __init__(
        self,
        client: AsyncClient,
        settings: HTTPTransportSettings = HTTPTransportSettings(),
    ) -> None:
        self.client = client
        self.settings = settings

    async def request(self, data: HTTPRequestData) -> ResponseContent:
        response = await self._make_request(data)
        return self._handle_response(response)

    async def stream(self, data: HTTPRequestData) -> Response:
        """Returns a httpx.Response created with stream=True"""
        response = await self._make_request(data, stream=True)

        try:
            return response
        finally:
            await response.aclose()

    async def _make_request(self, data: HTTPRequestData, **kwargs) -> Response:
        try:
            request = self._prepare_request(data)
            return await self.client.send(request, **kwargs)
        except Exception as exc:
            raise ConnectionTransportError(message=str(exc))

    def _prepare_request(self, data: HTTPRequestData) -> Request:
        return self.client.build_request(
            method=data.method,
            url=data.url,
            headers=data.headers,
            params=data.params,
        )

    def _handle_response(self, response: Response) -> ResponseContent:
        content = self._parse_content(response)
        try:
            response.raise_for_status()
        except HTTPStatusError as exc:
            raise self._handle_response_error(exc, content)
        return content

    def _handle_response_error(
        self, exc: HTTPStatusError, content: ResponseContent
    ) -> ServerError | ClientError:
        status = exc.response.status_code
        exception_class = (
            ServerError if status >= HTTPStatus.INTERNAL_SERVER_ERROR else ClientError
        )
        raise exception_class(status_code=status, response=content, message=str(exc))

    def _parse_content(self, response: Response) -> str | Any:
        match response.headers.get("content-type"):
            case ContentTypeEnum.text_html:
                return response.text
            case ContentTypeEnum.json:
                return response.json()
            case ContentTypeEnum.binary_octet_stream:
                return response.content
        return response.text


def get_client(  # pragma: no cover
    settings: HTTPTransportSettings = HTTPTransportSettings(),
) -> AsyncClient:
    return AsyncClient(
        headers=settings.common_headers,
        timeout=settings.default_timeout,
    )


@asynccontextmanager
async def init_transport(  # pragma: no cover
    settings: HTTPTransportSettings = HTTPTransportSettings(),
):
    client = get_client(settings)
    transport = HttpxHTTPTransport(client, settings)
    try:
        yield transport
    finally:
        await client.aclose()
