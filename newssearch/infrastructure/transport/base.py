from abc import ABC, abstractmethod
from binascii import Incomplete
from collections.abc import Generator

from newssearch.infrastructure.transport.schemas import HTTPRequestData, ResponseContent


class AbstractHTTPTransport(ABC):
    @abstractmethod
    def request(self, data: HTTPRequestData) -> ResponseContent: ...

    @abstractmethod
    def stream(self, data: HTTPRequestData) -> Generator[Incomplete]: ...
