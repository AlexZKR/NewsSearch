from abc import ABC, abstractmethod
from collections.abc import Iterator

from httpx import Response

from newssearch.infrastructure.transport.schemas import HTTPRequestData, ResponseContent


class AbstractSyncHTTPTransport(ABC):
    @abstractmethod
    def request(self, data: HTTPRequestData) -> ResponseContent: ...

    @abstractmethod
    def stream(self, data: HTTPRequestData) -> tuple[int, Iterator[bytes]]:
        """Streaming request

        Returns:
            tuple[int, Iterator[bytes]]: content-length, iterator
        """


class AbstractAsyncHTTPTransport(ABC):
    @abstractmethod
    async def request(self, data: HTTPRequestData) -> ResponseContent: ...

    @abstractmethod
    async def stream(self, data: HTTPRequestData) -> Response:
        """Streaming request

        Returns:
            tuple[int, Iterator[bytes]]: content-length, iterator
        """
