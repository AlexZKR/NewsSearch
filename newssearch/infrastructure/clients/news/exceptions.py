class NewsClientError(Exception): ...


class IDDoesntExistInPaths(NewsClientError):
    """Raised if specified `start_id` doesn't exist in downloaded path's file."""

    ...
