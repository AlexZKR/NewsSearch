import json
from http import HTTPMethod
from io import BytesIO

from requests import Response

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
        data = content.encode()
        r._content = data
        r.raw = BytesIO(data)
        r.headers["content-length"] = str(len(data))
    if content_type:
        r.headers["content-type"] = content_type.value
    return r
