from enum import StrEnum


class ETLType(StrEnum):
    ONE_THREADED = "one-threaded"
    THREADED_OPTIMIZED = "threaded-optimized"
    ASYNC = "async"
