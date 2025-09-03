from newssearch.infrastructure.clients.news.enums import CCDataSet
from newssearch.infrastructure.clients.news.schemas import WarcFileSchema
from newssearch.tests.tasks.news_etl.dummy_helpers import DummyRecord


def get_dummy_records():
    """Create three dummy records: r1 valid, r2 invalid, r3 valid"""
    r1 = DummyRecord(
        "response",
        {
            "WARC-Record-ID": "id1",
            "WARC-Target-URI": "http://example.com/a",
            "WARC-Date": "d1",
            "Content-Length": "10",
        },
        {"Content-Type": "text/html"},
        b"<html>r1</html>",
    )
    r2 = DummyRecord(
        "request",
        {
            "WARC-Record-ID": "id2",
            "WARC-Target-URI": "http://example.com/b",
            "WARC-Date": "d2",
            "Content-Length": "20",
        },
        {"Content-Type": "text/html"},
        b"<html>r2</html>",
    )
    r3 = DummyRecord(
        "response",
        {
            "WARC-Record-ID": "id3",
            "WARC-Target-URI": "http://example.com/c",
            "WARC-Date": "d3",
            "Content-Length": "30",
        },
        {"Content-Type": "text/html"},
        b"<html>r3</html>",
    )

    return [r1, r2, r3]


def make_warc_file(id_: str) -> WarcFileSchema:
    return WarcFileSchema(
        filepath=f"crawl-data/CC-NEWS/2025/09/CC-NEWS-20250901{id_}.warc.gz",
        dataset=CCDataSet.CCNEWS,
        year="2025",
        month="09",
        timestamp=f"20250901{id_}",
        id=id_,
    )
