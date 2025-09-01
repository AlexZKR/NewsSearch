from abc import ABC, abstractmethod
from collections.abc import Iterator

from newssearch.infrastructure.transport.schemas import HTTPRequestData, ResponseContent


class AbstractHTTPTransport(ABC):
    @abstractmethod
    def request(self, data: HTTPRequestData) -> ResponseContent: ...

    @abstractmethod
    def stream(self, data: HTTPRequestData) -> Iterator[bytes]: ...
