from newssearch.infrastructure.clients.news.enums import CCDataSet
from newssearch.infrastructure.clients.news.schemas import WarcPathSchema, WarcPathsFile
from newssearch.tests.infrastructure.news_client.testdata import (
    INVALID_WARC_FILENAME,
    VALID_WARC_FILENAME,
    VALID_WARC_PATHS_URL,
    get_expected_warc_filename_schema,
)


def get_expected_paths_schema() -> WarcPathsFile:
    return WarcPathsFile(
        url=VALID_WARC_PATHS_URL, dataset=CCDataSet.CCNEWS, year="2025", month="07"
    )


def test_warc_file_ok():
    file = WarcPathSchema.parse_warc_path(filepath=VALID_WARC_FILENAME)
    assert file == get_expected_warc_filename_schema()
    assert str(file) == "2025/07 - 20250701004326 - 02814"


def test_warc_file_fail():
    assert WarcPathSchema.parse_warc_path(filepath=INVALID_WARC_FILENAME) is None


def test_paths_file_ok():
    paths_file = WarcPathsFile.parse_warc_paths_url(url=VALID_WARC_PATHS_URL)
    assert paths_file is not None
    assert paths_file == get_expected_paths_schema()

    paths_file.parse_warc_filepaths([VALID_WARC_FILENAME, INVALID_WARC_FILENAME])
    assert paths_file.filepaths
    assert len(paths_file.filepaths) == 1
