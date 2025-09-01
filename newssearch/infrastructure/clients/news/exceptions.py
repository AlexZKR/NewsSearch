class NewsClientError(Exception): ...


class FileNotFound(NewsClientError): ...


class IDDoesntExistInPaths(NewsClientError):
    """Raised if specified `start_id` doesn't exist in downloaded path's file."""

    ...
