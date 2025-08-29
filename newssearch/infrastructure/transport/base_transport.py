from abc import ABC, abstractmethod
from http import HTTPStatus
from logging import getLogger
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from newssearch.config.settings import HTTPTransportSettings
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


class AbstractHTTPTransport(ABC):
    @abstractmethod
    def request(self, data: HTTPRequestData) -> ResponseContent: ...

    @abstractmethod
    def stream(self, data: HTTPRequestData) -> None: ...


class BaseHTTPTransport(AbstractHTTPTransport):
    """Based on requests, for sync calls. Has retry, back-off support"""

    def __init__(
        self, settings: HTTPTransportSettings = HTTPTransportSettings()
    ) -> None:
        self.settings = settings
        self.session: requests.Session | None = None

    # TODO: make use of prepare_request to reuse request logic in request and stream!
    def stream(self, data: HTTPRequestData) -> None:
        try:
            with self._session as s:
                response = s.request(
                    method=data.method,
                    url=data.url,
                    params=data.params,
                    headers=data.headers,
                    allow_redirects=data.allow_redirects,
                    stream=True,
                )
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
                response = s.request(
                    method=data.method,
                    url=data.url,
                    params=data.params,
                    headers=data.headers,
                    allow_redirects=data.allow_redirects,
                )
                return self._handle_response(response)
        except requests.RequestException as exc:
            self._handle_requests_exception(exc)
            raise

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

    # def get(self, url, **kwargs) -> bytes:
    #     """Base method for HTTP GET"""
    #     with self._session as s:
    #         response = s.get(url, **kwargs)
    #         if response.status_code == http.HTTPStatus.NOT_FOUND:
    #             raise NotFoundException(url)
    #         return response.content

    # def get_streaming(self, url: str, **kwargs):
    #     """Base method for HTTP GET streaming

    #     Uses stream=True for iterating over large responses.
    #     """
    #     with self._session as s:
    #         logger.info(f"Performing GET for {url}")
    #         with s.get(
    #             url, timeout=self.settings.default_timeout, stream=True, **kwargs
    #         ) as r:
    #             logger.info(f"content-length: {r.headers.get('content-length')}")
    #             yield r.iter_content(self.settings.default_chunk_size)
