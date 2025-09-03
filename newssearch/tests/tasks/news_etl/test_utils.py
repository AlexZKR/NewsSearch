import tempfile
from datetime import date, datetime

from tqdm import tqdm as tqdm_class

from newssearch.config import settings
from newssearch.tasks.news_etl.utils.utils import (
    format_year_month,
    get_tqdm,
    write_tmp_file,
)


def test_write_tmp_file_writes_and_returns_size():
    chunks = [b"hello", b"_world", b"!"]
    total_size = sum(len(c) for c in chunks)
    iterator = iter(chunks)

    with tempfile.NamedTemporaryFile() as tmp:
        size = write_tmp_file(iterator, tmp)
        assert size == total_size
        tmp.seek(0)
        assert tmp.read() == b"".join(chunks)


def test_get_tqdm_returns_tqdm_with_expected_attrs():
    t = get_tqdm("msg", total=42, pos=3, unit="B")
    try:
        assert isinstance(t, tqdm_class)
        assert t.total == 42  # noqa: PLR2004
        assert t.desc == "msg"
        assert getattr(t, "unit", None) == "B"
    finally:
        t.close()


def test_format_year_month_works_for_date_and_datetime(monkeypatch):
    monkeypatch.setattr(
        settings.NEWS_ETL_SETTINGS, "date_format", "%Y/%m", raising=False
    )

    d = date(2025, 9, 1)
    assert format_year_month(d) == "2025/09"

    dt = datetime(2025, 9, 1, 12, 0)
    assert format_year_month(dt) == "2025/09"
