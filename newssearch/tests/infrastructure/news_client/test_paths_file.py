import gzip
from datetime import date
from http import HTTPStatus

import pytest

from newssearch.infrastructure.clients.news.exceptions import (
    FileNotFound,
    NewsClientError,
)
from newssearch.infrastructure.clients.news.news_client_sync import NewsClientSync
from newssearch.infrastructure.transport.exceptions import ClientError
from newssearch.infrastructure.transport.requests_transport import RequestsHTTPTransport
from newssearch.tests.infrastructure.news_client.testdata import (
    INVALID_WARC_FILENAME,
    VALID_WARC_FILENAME,
)


@pytest.mark.parametrize(
    ("transport", "expected_length"),
    [
        pytest.param(
            {
                "request_returns": gzip.compress(
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
    news_client: NewsClientSync, transport: RequestsHTTPTransport, expected_length: int
):
    paths_file = news_client.get_paths_file(year_month=date(year=2025, month=7, day=1))
    assert paths_file.filepaths is not None
    assert len(paths_file.filepaths) == expected_length


@pytest.mark.parametrize(
    ("transport", "exp_exception"),
    [
        pytest.param(
            {
                "request_raises": ClientError(
                    status_code=HTTPStatus.NOT_FOUND, response="not found"
                )
            },
            FileNotFound,
            id="404",
        ),
        pytest.param(
            {
                "request_raises": ClientError(
                    status_code=HTTPStatus.BAD_REQUEST, response="bad request"
                )
            },
            NewsClientError,
            id="400",
        ),
    ],
    indirect=["transport"],
)
def test_paths_file_fail(
    news_client: NewsClientSync,
    transport: RequestsHTTPTransport,
    exp_exception: type[NewsClientError],
):
    with pytest.raises(exp_exception):
        news_client.get_paths_file(year_month=date(year=2025, month=7, day=1))
