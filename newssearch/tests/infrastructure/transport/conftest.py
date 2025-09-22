import json
from http import HTTPMethod

from newssearch.infrastructure.transport.schemas import HTTPRequestData

EXP_RESPONSE_TXT = "test_text"
EXP_RESPONSE_JSON = json.dumps({"txt": EXP_RESPONSE_TXT})


def get_request_data_text() -> HTTPRequestData:
    return HTTPRequestData(method=HTTPMethod.GET, url="https://test.com")
