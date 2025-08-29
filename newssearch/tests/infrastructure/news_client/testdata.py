from newssearch.infrastructure.clients.news.enums import CCDataSet
from newssearch.infrastructure.clients.news.schemas import WarcPathSchema, WarcPathsFile

VALID_WARC_FILENAME = "crawl-data/CC-NEWS/2025/07/CC-NEWS-20250701004326-02814.warc.gz"
INVALID_WARC_FILENAME = "rawl-dataa-CCf-NEWS/2025/01as/CC-fsadfNEWS-20250101020153"

VALID_WARC_PATHS_URL = (
    "https://data.commoncrawl.org/crawl-data/CC-NEWS/2025/07/warc.paths.gz"
)
INVLAID_WARC_PATHS_URL = (
    "https://data.commoncrawl.org/crawl-data/wrong/nono/yesyes/warc.paths.gz"
)


def get_expected_warc_filename_schema() -> WarcPathSchema:
    return WarcPathSchema(
        filepath=VALID_WARC_FILENAME,
        dataset=CCDataSet.CCNEWS,
        year="2025",
        month="07",
        timestamp="20250701004326",
        id="02814",
    )


def get_valid_warc_paths_schema() -> WarcPathsFile:
    return WarcPathsFile(
        url=VALID_WARC_PATHS_URL,
        dataset=CCDataSet.CCNEWS,
        year="2025",
        month="07",
        filepaths=[get_expected_warc_filename_schema()],
    )
