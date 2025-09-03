import tempfile
from collections.abc import Iterator
from datetime import date, datetime
from logging import getLogger
from urllib.parse import urlparse

from tqdm import tqdm
from warcio.recordloader import ArcWarcRecord

from newssearch.config import settings

logger = getLogger(__name__)


def write_tmp_file(
    content_iterator: Iterator[bytes],
    temp_file: tempfile._TemporaryFileWrapper,
) -> int:
    file_size = 0
    for chunk in content_iterator:
        temp_file.write(chunk)
        file_size += len(chunk)
    return file_size


def is_record_valid(record: ArcWarcRecord):
    if (
        record.rec_type == "response"
        and record.http_headers.get_header("Content-Type") == "text/html"
    ):
        return True
    return False


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


def get_tqdm(msg: str, total: int, pos: int = 0, unit: str = "B") -> tqdm:
    return tqdm(total=total, desc=msg, position=pos, unit="B", unit_scale=True)


def format_year_month(date: datetime | date) -> str:
    if isinstance(date, datetime):
        return date.date().strftime(settings.NEWS_ETL_SETTINGS.date_format)
    return date.strftime(settings.NEWS_ETL_SETTINGS.date_format)
