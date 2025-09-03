import tempfile
from collections.abc import Iterator
from datetime import date, datetime
from logging import getLogger

from tqdm import tqdm

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


def get_tqdm(msg: str, total: int, pos: int = 0, unit: str = "B") -> tqdm:
    return tqdm(total=total, desc=msg, position=pos, unit="B", unit_scale=True)


def format_year_month(date: datetime | date) -> str:
    if isinstance(date, datetime):
        return date.date().strftime(settings.NEWS_ETL_SETTINGS.date_format)
    return date.strftime(settings.NEWS_ETL_SETTINGS.date_format)
