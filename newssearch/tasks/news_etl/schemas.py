from datetime import datetime
from enum import StrEnum, auto

from pydantic import BaseModel


class ETLStageEnum(StrEnum):
    extract = auto()
    transform = auto()
    load = auto()


class RecordContentSchema(BaseModel):
    title: str | None = None
    excerpt: str | None = None
    hostname: str | None = None

    tags: str | None = None
    categories: str | None = None

    text: str | None = None

    date: datetime | None = None
    date_crawled: datetime | None = None


class WARCRecordSchema(BaseModel):
    id: str
    url: str | None = None
    date: str
    content_length: str
    mime_type: str | None = None
    content: RecordContentSchema | None = None
