import json
from logging import getLogger
from urllib.parse import urlparse

import trafilatura
from warcio.recordloader import ArcWarcRecord

from newssearch.tasks.news_etl.schemas import RecordContentSchema, WARCRecordSchema

logger = getLogger(__name__)


def process_record(record: ArcWarcRecord):
    return WARCRecordSchema(
        id=record.rec_headers.get_header("WARC-Record-ID"),
        url=extract_top_level_domain(record.rec_headers.get_header("WARC-Target-URI")),
        date=record.rec_headers.get_header("WARC-Date"),
        content_length=record.rec_headers.get_header("Content-Length"),
        mime_type=record.http_headers.get_header("Content-Type")
        if record.http_headers
        else None,
        content=extract_text_content(record.content_stream().read()),
    )


def is_record_valid(record: ArcWarcRecord):
    if (
        record.rec_type == "response"
        and record.http_headers.get_header("Content-Type") == "text/html"
    ):
        return True
    return False


def extract_text_content(content: bytes | None) -> RecordContentSchema | None:
    if not content:
        return None
    extracted = trafilatura.extract(
        content,
        include_comments=False,
        deduplicate=True,
        output_format="json",
        with_metadata=True,
    )
    if extracted:
        root = json.loads(extracted)
        return RecordContentSchema(
            title=root.get("title"),
            excerpt=root.get("excerpt"),
            hostname=root.get("hostname"),
            tags=root.get("tags"),
            categories=root.get("categories"),
            text=root.get("raw_text"),
            date=root.get("date"),
            date_crawled=root.get("filedate"),
        )
    return None


def extract_top_level_domain(url: str):
    """Extract the top-level domain (TLD) from a URL."""
    try:
        parsed_url = urlparse(url)
        domain_parts = parsed_url.netloc.split(".")
        if len(domain_parts) > 1:
            return "." + domain_parts[-1]
        return domain_parts[0]
    except Exception as e:
        logger.warning(f"Error extracting TLD from {url}: {e}")
        return None
