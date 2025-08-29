from enum import StrEnum
from http import HTTPMethod
from typing import Any

from pydantic import BaseModel

ResponseContent = dict[str, Any] | str


class ContentTypeEnum(StrEnum):
    binary_octet_stream = "binary/octet-stream"
    json = "application/json"
    text_html = "text/html"


class HTTPRequestData(BaseModel):
    method: HTTPMethod
    url: str
    params: dict[str, Any] | None = None
    headers: dict[str, Any] | None = None
    allow_redirects: bool = True
