from datetime import date
from logging import getLogger

from newssearch.infrastructure.clients.news.exceptions import FileNotFound
from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.clients.news.schemas import WarcPathsFile
from newssearch.tasks.news_etl.utils import format_year_month

logger = getLogger(__name__)


class NewsETL:
    def __init__(self, news_client: NewsClient) -> None:
        self.client = news_client
        self.__paths_files: dict[str, WarcPathsFile] = {}

    def get_paths_file(self, year_month: date) -> WarcPathsFile | None:
        ym = format_year_month(year_month)
        if file := self.__paths_files.get(ym):
            return file
        try:
            file = self.client.get_paths_file(year_month)
            self.__paths_files[ym] = file
            return file
        except FileNotFound:
            logger.warning(f"Paths file for {ym} not found!")
            return None
