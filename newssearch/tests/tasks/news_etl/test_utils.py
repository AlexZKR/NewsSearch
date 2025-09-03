import tempfile
from datetime import date, datetime
from typing import cast

import pytest
from tqdm import tqdm as tqdm_class
from warcio.recordloader import ArcWarcRecord

from newssearch.tasks.news_etl import utils


def test_write_tmp_file_writes_and_returns_size():
    chunks = [b"hello", b"_world", b"!"]
    total_size = sum(len(c) for c in chunks)
    iterator = iter(chunks)

    with tempfile.NamedTemporaryFile() as tmp:
        size = utils.write_tmp_file(iterator, tmp)
        assert size == total_size
        tmp.seek(0)
        assert tmp.read() == b"".join(chunks)


def test_is_record_valid_true_and_false():
    class DummyHeaders:
        def __init__(self, ct: str) -> None:
            self._ct = ct

        def get_header(self, name: str) -> str:
            return self._ct

    class DummyRecord:
        def __init__(self, rec_type: str, content_type: str) -> None:
            self.rec_type = rec_type
            self.http_headers = DummyHeaders(content_type)

    r_ok = cast(ArcWarcRecord, DummyRecord("response", "text/html"))
    assert utils.is_record_valid(r_ok) is True

    r_not_response = cast(ArcWarcRecord, DummyRecord("request", "text/html"))
    assert utils.is_record_valid(r_not_response) is False

    r_not_html = cast(ArcWarcRecord, DummyRecord("response", "application/json"))
    assert utils.is_record_valid(r_not_html) is False


@pytest.mark.parametrize(
    "url, expected",
    [
        ("http://sub.example.co.uk/path", ".uk"),
        ("https://example.com/", ".com"),
        ("http://localhost", "localhost"),
    ],
)
def test_extract_top_level_domain(url, expected):
    assert utils.extract_top_level_domain(url) == expected


def test_extract_top_level_domain_with_invalid_input_returns_none():
    # passing non-str should be handled and return None
    assert utils.extract_top_level_domain(123) is None  # type: ignore


def test_get_tqdm_returns_tqdm_with_expected_attrs():
    t = utils.get_tqdm("msg", total=42, pos=3, unit="B")
    try:
        assert isinstance(t, tqdm_class)
        assert t.total == 42  # noqa: PLR2004
        assert t.desc == "msg"
        assert getattr(t, "unit", None) == "B"
    finally:
        t.close()


def test_format_year_month_works_for_date_and_datetime(monkeypatch):
    monkeypatch.setattr(
        utils.settings.NEWS_ETL_SETTINGS, "date_format", "%Y/%m", raising=False
    )

    d = date(2025, 9, 1)
    assert utils.format_year_month(d) == "2025/09"

    dt = datetime(2025, 9, 1, 12, 0)
    assert utils.format_year_month(dt) == "2025/09"
