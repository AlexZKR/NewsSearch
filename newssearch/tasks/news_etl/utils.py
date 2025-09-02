import re
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


def parse_id_range(raw: str, available_ids: set[str]) -> tuple[str, str]:
    raw = raw.strip()
    if not raw:
        raise ValueError("empty input")

    # allow hyphen variants with optional spaces
    parts = re.split(r"\s*[-–—]\s*", raw)
    if len(parts) == 1:
        start_s = end_s = parts[0]
    elif len(parts) == 2:  # noqa: PLR2004
        start_s, end_s = parts
        if not start_s:
            raise ValueError("range must have a start id")
        if not end_s:
            # "03802-" treat as single id (same as start)
            end_s = start_s
    else:
        raise ValueError("too many separators")

    if not (start_s.isdigit() and end_s.isdigit()):
        raise ValueError("ids must be numeric")

    # numeric validation using available ids
    width = max(len(i) for i in available_ids)  # preserve zero-padding

    available_ints = sorted(int(i) for i in available_ids)
    min_id, max_id = available_ints[0], available_ints[-1]

    start_i, end_i = int(start_s), int(end_s)
    if start_i > end_i:
        raise ValueError("start id must be <= end id")
    if not (min_id <= start_i <= max_id and min_id <= end_i <= max_id):
        raise ValueError(
            f"ids out of available range: {min_id:0{width}}-{max_id:0{width}}"
        )

    # return zero-padded strings matching existing ids format
    return str(start_i).zfill(width), str(end_i).zfill(width)


def format_year_month(date: datetime | date) -> str:
    if isinstance(date, datetime):
        return date.date().strftime(settings.NEWS_ETL_SETTINGS.date_format)
    return date.strftime(settings.NEWS_ETL_SETTINGS.date_format)
