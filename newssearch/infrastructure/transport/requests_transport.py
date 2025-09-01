from http import HTTPStatus
from logging import getLogger
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from newssearch.config.settings import HTTPTransportSettings
from newssearch.infrastructure.transport.base import AbstractHTTPTransport
from newssearch.infrastructure.transport.exceptions import (
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

    def stream(self, data: HTTPRequestData) -> None:
        try:
            with self._session as s:
                request = self._prepare_request(data).prepare()
                response = s.send(request, stream=True)

                with open("warc.warc", "wb") as fd:
                    for chunk in response.iter_content(
                        chunk_size=self.settings.default_chunk_size
                    ):
                        fd.write(chunk)
            return
        except requests.RequestException as exc:
            self._handle_requests_exception(exc)
            raise

    def request(self, data: HTTPRequestData) -> ResponseContent:
        try:
            with self._session as s:
                request = self._prepare_request(data).prepare()
                response = s.send(request)

                return self._handle_response(response)
        except requests.RequestException as exc:
            self._handle_requests_exception(exc)
            raise

    def _prepare_request(self, data: HTTPRequestData) -> requests.Request:
        return requests.Request(
            method=data.method, url=data.url, headers=data.headers, params=data.params
        )

    def _handle_response(self, response: requests.Response) -> ResponseContent:
        content = self._parse_content(response)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            self._handle_HTTP_error(exc, content)
        return content

    def _handle_requests_exception(self, exc: requests.RequestException):
        """Handle retry exception, raised in request or stream func"""
        if exc.response:
            content = self._parse_content(exc.response)
            raise ClientError(status=exc.response.status_code, response=content)
        else:
            raise ClientError(message=str(exc))

    def _handle_HTTP_error(self, exc: requests.HTTPError, content: ResponseContent):
        status = exc.response.status_code
        exception_class = (
            ServerError if status >= HTTPStatus.INTERNAL_SERVER_ERROR else ClientError
        )
        raise exception_class(status=status, response=content)

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
        if self.session:
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
