import gzip
from datetime import date

import pytest

from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.transport.requests_transport import BaseHTTPTransport
from newssearch.tests.infrastructure.news_client.testdata import (
    INVALID_WARC_FILENAME,
    VALID_WARC_FILENAME,
)


@pytest.mark.parametrize(
    ("transport", "expected_length"),
    [
        pytest.param(
            {
                "returns": gzip.compress(
                    "\n".join([VALID_WARC_FILENAME, INVALID_WARC_FILENAME]).encode()
                )
            },
            1,
            id="one valid one invalid",
        )
    ],
    indirect=["transport"],
)
def test_paths_file_ok(
    news_client: NewsClient, transport: BaseHTTPTransport, expected_length: int
):
    paths_file = news_client.get_paths_file(year_month=date(year=2025, month=7, day=1))
    assert paths_file.filepaths is not None
    assert len(paths_file.filepaths) == expected_length


# @pytest.mark.parametrize(
#     ("transport", "expected_length"),
#     [
#         pytest.param(
#             {"returns": get_valid_warc_paths_schema()},
#             1,
#             id="valid paths file with one filepath",
#         )
#     ],
#     indirect=["transport"],
# )
def test_download_warc_file_ok(news_client: NewsClient, transport: BaseHTTPTransport):
    news_client.download_warc(
        year_month=date(year=2025, month=7, day=1), start_id="02814"
    )
