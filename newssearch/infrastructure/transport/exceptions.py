from newssearch.infrastructure.transport.schemas import ResponseContent


class BaseTransportException(Exception):
    def __init__(
        self,
        status_code: int | None = None,
        response: ResponseContent | None = None,
        message: str | None = None,
    ):
        self.status_code = status_code
        self.response = response
        self.message = message

    def __str__(self) -> str:
        return f"HTTP Exception. Code: {self.status_code}; Response: {self.response}; Message: {self.message}"

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}; HTTP Exception. Code: {self.status_code}; Response: {self.response}; Message: {self.message}>"


class ConnectionTransportError(BaseTransportException):
    """All connection errors. We do not have any response from server"""


class ClientError(BaseTransportException):
    """HTTP errors with 400 <= status < 500."""


class ServerError(BaseTransportException):
    """HTTP errors with status >= 500."""
