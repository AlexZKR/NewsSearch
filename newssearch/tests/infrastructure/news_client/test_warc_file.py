from http import HTTPStatus

import pytest

from newssearch.infrastructure.clients.news.exceptions import (
    FileNotFound,
    NewsClientError,
)
from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.clients.news.schemas import WarcFileSchema
from newssearch.infrastructure.transport.exceptions import ClientError
from newssearch.infrastructure.transport.requests_transport import BaseHTTPTransport
from newssearch.tests.infrastructure.news_client.testdata import (
    get_expected_warc_filename_schema,
)


@pytest.mark.parametrize(
    ("transport", "file"),
    [
        pytest.param(
            {"stream_returns": (2, iter([b"123"]))},
            get_expected_warc_filename_schema(),
            id="ok",
        )
    ],
    indirect=["transport"],
)
def test_warc_file_ok(
    news_client: NewsClient, transport: BaseHTTPTransport, file: WarcFileSchema
):
    content_iterator = news_client.get_warc_file(file=file)
    assert [chunk for chunk in content_iterator] == [b"123"]


@pytest.mark.parametrize(
    ("transport", "exp_exception", "file"),
    [
        pytest.param(
            {
                "stream_raises": ClientError(
                    status_code=HTTPStatus.NOT_FOUND, response="not found"
                )
            },
            FileNotFound,
            get_expected_warc_filename_schema(),
            id="404",
        ),
        pytest.param(
            {
                "stream_raises": ClientError(
                    status_code=HTTPStatus.BAD_REQUEST, response="bad request"
                )
            },
            NewsClientError,
            get_expected_warc_filename_schema(),
            id="400",
        ),
    ],
    indirect=["transport"],
)
def test_warc_file_fail(
    news_client: NewsClient,
    transport: BaseHTTPTransport,
    exp_exception: type[NewsClientError],
    file: WarcFileSchema,
):
    content_iterator = news_client.get_warc_file(file=file)
    with pytest.raises(exp_exception):
        next(content_iterator)
