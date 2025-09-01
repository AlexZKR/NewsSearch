import gzip
from datetime import date
from http import HTTPMethod, HTTPStatus
from logging import getLogger
from typing import cast

from newssearch.config.settings import NewsClientSettings
from newssearch.infrastructure.clients.news.exceptions import (
    FileNotFound,
    IDDoesntExistInPaths,
    NewsClientError,
)
from newssearch.infrastructure.clients.news.schemas import WarcPathSchema, WarcPathsFile
from newssearch.infrastructure.transport.exceptions import BaseTransportException
from newssearch.infrastructure.transport.requests_transport import (
    BaseHTTPTransport,
    HTTPRequestData,
)
from newssearch.infrastructure.transport.schemas import ResponseContent

logger = getLogger(__name__)


class NewsClient:
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
        transport: BaseHTTPTransport,
        settings: NewsClientSettings = NewsClientSettings(),
    ):
        self.transport = transport
        self.settings = settings

    def download_warc(self, year_month: date, start_id: str, range: int = 0):
        """Download one or multiple WARC files.

        Args:
            year_month (date): year and month of paths file.
            start_id (str): ID of first WARC to donwload.
            end_id (str | None, optional): ID of last WARC to download. If not specifed, then download only start_id. Defaults to None.
        """
        paths_file = self.get_paths_file(year_month)

        start_index = None
        for fp in paths_file.filepaths:
            if fp.id == start_id:
                start_index = paths_file.filepaths.index(fp)

        if start_index is None:
            raise IDDoesntExistInPaths(
                f"ID: {start_id} doesn't exist in path range {paths_file.id_range}"
            )

        # handle range that is larger than paths file length
        if start_index + range > len(paths_file.filepaths):
            range = len(paths_file.filepaths) - 1

        # handle range < 0
        range = max(range, 0)

        logger.info(f"Downloading {start_index + range} WARC files.")
        url = self.__get_file_url(paths_file.filepaths[start_index])
        self.transport.stream(data=HTTPRequestData(method=HTTPMethod.GET, url=url))

    def get_paths_file(self, year_month: date) -> WarcPathsFile:
        """Download file which contains WARC filepaths"""
        url = self.__get_paths_url(year_month)
        schema = WarcPathsFile.parse_warc_paths_url(url)

        response = self._try_make_request(
            data=HTTPRequestData(method=HTTPMethod.GET, url=url)
        )
        data = gzip.decompress(cast(bytes, response)).decode()

        schema.parse_warc_filepaths(data.splitlines())
        logger.info(f"Got {len(schema.filepaths)} paths")

        return schema

    def __get_paths_url(self, year_month: date) -> str:
        return self.settings.paths_url.format(
            yyyy=year_month.strftime("%Y"), mm=year_month.strftime("%m")
        )

    def __get_file_url(self, file: WarcPathSchema) -> str:
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

    def _try_stream_request(self, data: HTTPRequestData):
        try:
            self.transport.stream(data)
        except BaseTransportException as exc:
            raise NewsClientError(exc) from exc
