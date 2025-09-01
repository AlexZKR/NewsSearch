import json
from http import HTTPMethod

import pytest
import requests
from requests import Response

from newssearch.infrastructure.transport.base import AbstractHTTPTransport
from newssearch.infrastructure.transport.exceptions import ClientError, ServerError
from newssearch.infrastructure.transport.schemas import ContentTypeEnum, HTTPRequestData

EXP_RESPONSE_TXT = "test_text"
EXP_RESPONSE_JSON = json.dumps({"txt": EXP_RESPONSE_TXT})


def get_request_data_text() -> HTTPRequestData:
    return HTTPRequestData(method=HTTPMethod.GET, url="https://test.com")


def get_exp_response(
    content_type: ContentTypeEnum | None = None,
    content: str | None = None,
    status_code: int = 200,
) -> Response:
    r = Response()
    r.status_code = status_code
    if content:
        r._content = content.encode()
    if content_type:
        r.headers["content-type"] = content_type.value
    return r


@pytest.mark.parametrize(
    ("mock_session", "request_data", "exp_response"),
    [
        pytest.param(
            {"returns": get_exp_response(content=EXP_RESPONSE_TXT)},
            get_request_data_text(),
            EXP_RESPONSE_TXT,
            id="content_type not specifed, 200 OK",
        ),
        pytest.param(
            {
                "returns": get_exp_response(
                    content_type=ContentTypeEnum.text_html, content=EXP_RESPONSE_TXT
                )
            },
            get_request_data_text(),
            EXP_RESPONSE_TXT,
            id="text/html, 200 OK",
        ),
        pytest.param(
            {
                "returns": get_exp_response(
                    content_type=ContentTypeEnum.json, content=EXP_RESPONSE_JSON
                )
            },
            get_request_data_text(),
            {"txt": EXP_RESPONSE_TXT},
            id="json, 200 OK",
        ),
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
def test_request_ok(
    transport: AbstractHTTPTransport,
    mock_session,
    request_data: HTTPRequestData,
    exp_response,
) -> None:
    r = transport.request(request_data)
    assert r == exp_response


@pytest.mark.parametrize(
    ("mock_session", "request_data", "exp_exception", "exp_msg"),
    [
        pytest.param(
            {
                "returns": get_exp_response(
                    content_type=ContentTypeEnum.text_html,
                    content="test",
                    status_code=400,
                )
            },
            get_request_data_text(),
            ClientError,
            "HTTP Exception. Code: 400; Response: test; Message: None",
            id="400, ClientError",
        ),
        pytest.param(
            {
                "returns": get_exp_response(
                    content_type=ContentTypeEnum.text_html,
                    content="test",
                    status_code=500,
                )
            },
            get_request_data_text(),
            ServerError,
            "HTTP Exception. Code: 500; Response: test; Message: None",
            id="500, ServerError",
        ),
    ],
    indirect=["mock_session"],
)
def test_request_raise_for_status_exc(
    transport: AbstractHTTPTransport,
    mock_session,
    request_data: HTTPRequestData,
    exp_exception,
    exp_msg: str,
) -> None:
    with pytest.raises(exp_exception) as exc:
        transport.request(request_data)
    assert str(exc.value) == exp_msg


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
                        content_type=ContentTypeEnum.text_html,
                        content="test",
                        status_code=500,
                    )
                )
            },
            get_request_data_text(),
            ClientError,
            "test",
            id="retry error, with resp",
        ),
    ],
    indirect=["mock_session"],
)
def test_request_retry_exc(
    transport: AbstractHTTPTransport,
    mock_session,
    request_data: HTTPRequestData,
    exp_exception,
    exp_msg: str,
) -> None:
    with pytest.raises(exp_exception) as exc:
        transport.request(request_data)
    if resp := exc.value.response:
        assert resp == exp_msg
