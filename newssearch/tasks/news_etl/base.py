from abc import ABC, abstractmethod

from newssearch.infrastructure.clients.news.schemas import WarcFileSchema


class BaseNewsETL(ABC):
    @abstractmethod
    def run(self, files: list[WarcFileSchema]) -> None: ...
