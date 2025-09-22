import gzip
from collections.abc import Iterator
from datetime import date
from http import HTTPMethod, HTTPStatus
from logging import getLogger
from typing import cast

from newssearch.config.settings import NewsClientSettings
from newssearch.infrastructure.clients.news.exceptions import (
    FileNotFound,
    NewsClientError,
)
from newssearch.infrastructure.clients.news.schemas import WarcFileSchema, WarcPathsFile
from newssearch.infrastructure.transport.base import AbstractSyncHTTPTransport
from newssearch.infrastructure.transport.exceptions import BaseTransportException
from newssearch.infrastructure.transport.requests_transport import HTTPRequestData
from newssearch.infrastructure.transport.schemas import ResponseContent
from newssearch.tasks.news_etl.utils.utils import get_tqdm

logger = getLogger(__name__)


class NewsClientSync:
    """Gets data from CC-News dataset

    More info: https://data.commoncrawl.org/crawl-data/CC-NEWS/index.html

    Short:
        1) Separate dataset file names are stored in 'paths' file. So, get it
        and store it near the client.
        2) If 'paths' file is in place, scan it for the desired date.
        3) If date is found then download the file.
    """

    def __init__(
        self,
        transport: AbstractSyncHTTPTransport,
        settings: NewsClientSettings = NewsClientSettings(),
    ):
        self.transport = transport
        self.settings = settings

    def get_warc_file(self, file: WarcFileSchema, pos: int = 0) -> Iterator[bytes]:
        """Download one WARC file."""
        url = self.__get_warc_file_url(file)
        content_len, iterator = self._try_stream_request(
            data=HTTPRequestData(method=HTTPMethod.GET, url=url)
        )

        with get_tqdm(msg=f"Downloading {file.id}", total=content_len, pos=pos) as pbar:
            for chunk in iterator:
                yield chunk
                pbar.update(len(chunk))

    def get_paths_file(self, year_month: date) -> WarcPathsFile:
        """Download file which contains WARC filepaths"""
        url = self.__get_paths_file_url(year_month)
        schema = WarcPathsFile.parse_warc_paths_url(url)

        response = self._try_make_request(
            data=HTTPRequestData(method=HTTPMethod.GET, url=url)
        )
        data = gzip.decompress(cast(bytes, response)).decode()

        schema.parse_warc_filepaths(data.splitlines())
        logger.info(f"Got {len(schema.filepaths)} paths")

        return schema

    def __get_paths_file_url(self, year_month: date) -> str:
        return self.settings.paths_url.format(
            yyyy=year_month.strftime("%Y"), mm=year_month.strftime("%m")
        )

    def __get_warc_file_url(self, file: WarcFileSchema) -> str:
        return self.settings.file_url.format(
            yyyy=file.year,
            mm=file.month,
            timestamp=file.timestamp,
            id=file.id,
        )

    def _try_make_request(self, data: HTTPRequestData) -> ResponseContent:
        try:
            return self.transport.request(data)
        except BaseTransportException as exc:
            if exc.status_code == HTTPStatus.NOT_FOUND:
                raise FileNotFound
            raise NewsClientError(exc) from exc

    def _try_stream_request(self, data: HTTPRequestData) -> tuple[int, Iterator[bytes]]:
        try:
            return self.transport.stream(data)
        except BaseTransportException as exc:
            if exc.status_code == HTTPStatus.NOT_FOUND:
                raise FileNotFound
            raise NewsClientError(exc) from exc
