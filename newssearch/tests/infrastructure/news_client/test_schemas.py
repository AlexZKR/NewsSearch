from newssearch.infrastructure.clients.news.enums import CCDataSet
from newssearch.infrastructure.clients.news.schemas import WarcPathSchema, WarcPathsFile

VALID_WARC_FILENAME = "crawl-data/CC-NEWS/2025/01/CC-NEWS-20250101020153-00156.warc.gz"
INVALID_WARC_FILENAME = "rawl-dataa-CCf-NEWS/2025/01as/CC-fsadfNEWS-20250101020153"

VALID_WARC_URL = "https://data.commoncrawl.org/crawl-data/CC-NEWS/2024/01/warc.paths.gz"
INVLAID_WARC_URL = (
    "https://data.commoncrawl.org/crawl-data/wrong/nono/yesyes/warc.paths.gz"
)


def get_expected_warc_filename_schema() -> WarcPathSchema:
    return WarcPathSchema(
        filepath=VALID_WARC_FILENAME,
        dataset=CCDataSet.CCNEWS,
        year="2025",
        month="01",
        timestamp="20250101020153",
        id="00156",
    )


def get_expected_paths_schema() -> WarcPathsFile:
    return WarcPathsFile(
        url=VALID_WARC_URL, dataset=CCDataSet.CCNEWS, year="2024", month="01"
    )


def test_warc_file_ok():
    assert (
        WarcPathSchema.parse_warc_path(filepath=VALID_WARC_FILENAME)
        == get_expected_warc_filename_schema()
    )


def test_warc_file_fail():
    assert WarcPathSchema.parse_warc_path(filepath=INVALID_WARC_FILENAME) is None


def test_paths_file_ok():
    paths_file = WarcPathsFile.parse_warc_paths_url(url=VALID_WARC_URL)
    assert paths_file is not None
    assert paths_file == get_expected_paths_schema()

    paths_file.parse_warc_filepaths([VALID_WARC_FILENAME, INVALID_WARC_FILENAME])
    assert paths_file.filepaths
    assert len(paths_file.filepaths) == 1
