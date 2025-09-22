import pytest
import requests

from newssearch.infrastructure.transport.exceptions import ClientError
from newssearch.infrastructure.transport.requests_transport import RequestsHTTPTransport
from newssearch.infrastructure.transport.schemas import ContentTypeEnum, HTTPRequestData
from newssearch.tests.infrastructure.transport.conftest import (
    EXP_RESPONSE_TXT,
    get_exp_response,
    get_request_data_text,
)


@pytest.mark.parametrize(
    ("mock_session", "request_data", "exp_response"),
    [
        pytest.param(
            {
                "returns": get_exp_response(
                    content_type=ContentTypeEnum.binary_octet_stream,
                    content=EXP_RESPONSE_TXT,
                )
            },
            get_request_data_text(),
            EXP_RESPONSE_TXT.encode(),
            id="binary, 200 OK",
        ),
    ],
    indirect=["mock_session"],
)
def test_stream_ok(
    transport: RequestsHTTPTransport,
    mock_session,
    request_data: HTTPRequestData,
    exp_response,
) -> None:
    resp = b""
    size, iterator = transport.stream(request_data)
    for r in iterator:
        resp += r
    assert resp == exp_response
    assert size == len(resp)


@pytest.mark.parametrize(
    ("mock_session", "request_data", "exp_exception", "exp_msg"),
    [
        pytest.param(
            {"raises": requests.exceptions.RetryError},
            get_request_data_text(),
            ClientError,
            "No msg",
            id="retry error, without resp",
        ),
        pytest.param(
            {
                "raises": requests.exceptions.RetryError(
                    response=get_exp_response(
                        content_type=ContentTypeEnum.binary_octet_stream,
                        content="test",
                        status_code=500,
                    )  # type: ignore
                )
            },
            get_request_data_text(),
            ClientError,
            b"test",
            id="retry error, with resp",
        ),
    ],
    indirect=["mock_session"],
)
def test_stream_retry_exc(
    transport: RequestsHTTPTransport,
    mock_session,
    request_data: HTTPRequestData,
    exp_exception,
    exp_msg: str,
) -> None:
    with pytest.raises(exp_exception) as exc:
        result = b""
        size, iterator = transport.stream(request_data)
        for r in iterator:
            result += r
    if resp := exc.value.response:
        assert resp == exp_msg
