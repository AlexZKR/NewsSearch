from collections.abc import Iterator
from http import HTTPStatus
from logging import getLogger
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from newssearch.config.settings import HTTPTransportSettings
from newssearch.infrastructure.transport.base import AbstractHTTPTransport
from newssearch.infrastructure.transport.exceptions import (
    BaseTransportException,
    ClientError,
    ServerError,
)
from newssearch.infrastructure.transport.schemas import (
    ContentTypeEnum,
    HTTPRequestData,
    ResponseContent,
)

logger = getLogger(__name__)


class BaseHTTPTransport(AbstractHTTPTransport):
    """Based on requests, for sync calls. Has retry, back-off support"""

    def __init__(
        self, settings: HTTPTransportSettings = HTTPTransportSettings()
    ) -> None:
        self.settings = settings
        self.session: requests.Session | None = None

    def stream(self, data: HTTPRequestData) -> tuple[int, Iterator[bytes]]:
        try:
            with self._session as s:
                request = self._prepare_request(data).prepare()
                response = s.send(request, stream=True)
                iterator = response.iter_content(chunk_size=self.settings.chunk_size)
                content_len = int(response.headers.get("content-length", 0))
                return content_len, iterator
        except requests.RequestException as exc:
            raise self._handle_requests_exception(exc)

    def request(self, data: HTTPRequestData) -> ResponseContent:
        try:
            with self._session as s:
                request = self._prepare_request(data).prepare()
                response = s.send(request)

                return self._handle_response(response)
        except requests.RequestException as exc:
            raise self._handle_requests_exception(exc)

    def _prepare_request(self, data: HTTPRequestData) -> requests.Request:
        common_headers = {"user-agent": self.settings.user_agent}

        return requests.Request(
            method=data.method,
            url=data.url,
            headers=common_headers.update(data.headers)
            if data.headers
            else common_headers,
            params=data.params,
        )

    def _handle_response(self, response: requests.Response) -> ResponseContent:
        content = self._parse_content(response)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise self._handle_HTTP_error(exc, content)
        return content

    def _handle_requests_exception(
        self, exc: requests.RequestException
    ) -> BaseTransportException:
        """Handle retry exception, raised in request or stream func"""
        if exc.response is not None:
            content = self._parse_content(exc.response)
            return ClientError(
                status_code=exc.response.status_code,
                response=content,
                message=exc.response.reason,
            )
        else:
            return ClientError(message=str(exc))

    def _handle_HTTP_error(
        self, exc: requests.HTTPError, content: ResponseContent
    ) -> ServerError | ClientError:
        status = exc.response.status_code
        exception_class = (
            ServerError if status >= HTTPStatus.INTERNAL_SERVER_ERROR else ClientError
        )
        return exception_class(status_code=status, response=content)

    def _parse_content(self, response: requests.Response) -> str | Any:
        match response.headers.get("content-type"):
            case ContentTypeEnum.text_html:
                return response.text
            case ContentTypeEnum.json:
                return response.json()
            case ContentTypeEnum.binary_octet_stream:
                return response.content
        return response.text

    @property
    def _session(self) -> requests.Session:
        if self.session:  # pragma: nocover
            return self.session

        retry_obj: Retry = Retry(
            total=self.settings.max_retries,
            allowed_methods=self.settings.allowed_methods,
            status_forcelist=self.settings.status_forcelist,
            backoff_factor=self.settings.backoff_factor,
            backoff_max=self.settings.max_backoff,
            raise_on_redirect=True,
            raise_on_status=True,
        )
        adapter = HTTPAdapter(max_retries=retry_obj)
        s = requests.Session()
        s.mount("https://", adapter)

        self.session = s
        return self.session
