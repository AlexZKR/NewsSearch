import pytest

from newssearch.infrastructure.transport.aiohttp_transport import HttpxHTTPTransport
from newssearch.infrastructure.transport.exceptions import ClientError, ServerError
from newssearch.infrastructure.transport.schemas import ContentTypeEnum, HTTPRequestData
from newssearch.tests.infrastructure.transport.conftest import (
    EXP_RESPONSE_JSON,
    EXP_RESPONSE_TXT,
    get_request_data_text,
)
from newssearch.tests.infrastructure.transport.httpx.conftest import get_exp_response

pytestmark = [pytest.mark.asyncio]


@pytest.mark.parametrize(
    ("client", "request_data", "exp_response"),
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
    indirect=["client"],
)
async def test_request_httpx_ok(
    transport_httpx: HttpxHTTPTransport,
    client,
    request_data: HTTPRequestData,
    exp_response,
) -> None:
    r = await transport_httpx.request(request_data)
    assert r == exp_response


@pytest.mark.parametrize(
    ("client", "request_data", "exp_exception"),
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
            id="500, ServerError",
        ),
    ],
    indirect=["client"],
)
async def test_request_raise_for_status_httpx_exc(
    transport_httpx: HttpxHTTPTransport,
    client,
    request_data: HTTPRequestData,
    exp_exception,
) -> None:
    with pytest.raises(exp_exception):
        await transport_httpx.request(request_data)


# @pytest.mark.parametrize(
#     ("mock_session", "request_data", "exp_exception", "exp_msg"),
#     [
#         pytest.param(
#             {"raises": requests.exceptions.RetryError},
#             get_request_data_text(),
#             ClientError,
#             "No msg",
#             id="retry error, without resp",
#         ),
#         pytest.param(
#             {
#                 "raises": requests.exceptions.RetryError(
#                     response=get_exp_response(
#                         content_type=ContentTypeEnum.text_html,
#                         content="test",
#                         status_code=500,
#                     )
#                 )
#             },
#             get_request_data_text(),
#             ClientError,
#             "test",
#             id="retry error, with resp",
#         ),
#     ],
#     indirect=["mock_session"],
# )
# async def test_request_retry_exc(
#     transport: HttpxHTTPTransport,
#     mock_session,
#     request_data: HTTPRequestData,
#     exp_exception,
#     exp_msg: str,
# ) -> None:
#     with pytest.raises(exp_exception) as exc:
#         await transport.request(request_data)
#     if resp := exc.value.response:
#         assert resp == exp_msg
