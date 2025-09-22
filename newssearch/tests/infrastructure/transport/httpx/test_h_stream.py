import pytest

from newssearch.infrastructure.transport.aiohttp_transport import HttpxHTTPTransport
from newssearch.infrastructure.transport.schemas import ContentTypeEnum, HTTPRequestData
from newssearch.tests.infrastructure.transport.conftest import (
    EXP_RESPONSE_TXT,
    get_request_data_text,
)
from newssearch.tests.infrastructure.transport.httpx.conftest import get_exp_response

pytestmark = [pytest.mark.asyncio]


@pytest.mark.parametrize(
    ("client", "request_data", "exp_response"),
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
    indirect=["client"],
)
async def test_stream_httpx_ok(
    transport_httpx: HttpxHTTPTransport,
    client,
    request_data: HTTPRequestData,
    exp_response,
) -> None:
    resp = b""
    response = await transport_httpx.stream(request_data)
    async for r in response.aiter_bytes(3):
        resp += r
    assert resp == exp_response


# @pytest.mark.parametrize(
#     ("client", "request_data", "exp_exception", "exp_msg"),
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
#                         content_type=ContentTypeEnum.binary_octet_stream,
#                         content="test",
#                         status_code=500,
#                     )
#                 )
#             },
#             get_request_data_text(),
#             ClientError,
#             b"test",
#             id="retry error, with resp",
#         ),
#     ],
#     indirect=["client"],
# )
# async def test_stream_retry_exc(
#     transport: HttpxHTTPTransport,
#     client,
#     request_data: HTTPRequestData,
#     exp_exception,
#     exp_msg: str,
# ) -> None:
#     with pytest.raises(exp_exception) as exc:
#         result = b""
#         size, iterator = await transport.stream(request_data)
#         async for r in iterator:
#             result += r
#     if resp := exc.value.response:
#         assert resp == exp_msg
