import json
from datetime import UTC, datetime
from typing import cast
from unittest.mock import Mock, patch

import pytest
from warcio.recordloader import ArcWarcRecord

from newssearch.tasks.news_etl.schemas import RecordContentSchema, WARCRecordSchema
from newssearch.tasks.news_etl.utils.record_factory import (
    extract_text_content,
    extract_top_level_domain,
    is_record_valid,
    process_record,
)


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
    assert is_record_valid(r_ok) is True

    r_not_response = cast(ArcWarcRecord, DummyRecord("request", "text/html"))
    assert is_record_valid(r_not_response) is False

    r_not_html = cast(ArcWarcRecord, DummyRecord("response", "application/json"))
    assert is_record_valid(r_not_html) is False


@pytest.mark.parametrize(
    "url, expected",
    [
        ("http://sub.example.co.uk/path", ".uk"),
        ("https://example.com/", ".com"),
        ("http://localhost", "localhost"),
    ],
)
def test_extract_top_level_domain(url, expected):
    assert extract_top_level_domain(url) == expected


def test_extract_top_level_domain_with_invalid_input_returns_none():
    assert extract_top_level_domain(123) is None  # type: ignore


def test_process_record_with_valid_data():
    """Test process_record with complete record data"""
    mock_record = Mock(spec=ArcWarcRecord)

    mock_rec_headers = Mock()
    mock_rec_headers.get_header.side_effect = lambda x: {
        "WARC-Record-ID": "record-123",
        "WARC-Target-URI": "https://example.com/article",
        "WARC-Date": "2023-01-01T12:00:00Z",
        "Content-Length": "1024",
    }[x]

    mock_http_headers = Mock()
    mock_http_headers.get_header.return_value = "text/html"

    mock_content_stream = Mock()
    mock_content_stream.read.return_value = b"<html>content</html>"

    mock_record.rec_headers = mock_rec_headers
    mock_record.http_headers = mock_http_headers
    mock_record.content_stream.return_value = mock_content_stream

    with (
        patch(
            "newssearch.tasks.news_etl.utils.record_factory.extract_top_level_domain"
        ) as mock_tld,
        patch(
            "newssearch.tasks.news_etl.utils.record_factory.extract_text_content"
        ) as mock_extract,
    ):
        mock_tld.return_value = ".com"
        mock_extract.return_value = RecordContentSchema(
            title="Test Article",
            excerpt="This is a test excerpt",
            hostname="example.com",
            text="This is the full article text",
        )

        result = process_record(mock_record)

        assert isinstance(result, WARCRecordSchema)
        assert result.id == "record-123"
        assert result.url == ".com"
        assert result.date == "2023-01-01T12:00:00Z"
        assert result.content_length == "1024"
        assert result.mime_type == "text/html"

        if result.content:
            assert result.content.title == "Test Article"

        mock_tld.assert_called_once_with("https://example.com/article")
        mock_extract.assert_called_once_with(b"<html>content</html>")


def test_process_record_with_missing_http_headers():
    """Test process_record when HTTP headers are missing"""
    mock_record = Mock(spec=ArcWarcRecord)

    mock_rec_headers = Mock()
    mock_rec_headers.get_header.side_effect = lambda x: {
        "WARC-Record-ID": "record-123",
        "WARC-Target-URI": "https://example.com/article",
        "WARC-Date": "2023-01-01T12:00:00Z",
        "Content-Length": "1024",
    }[x]

    mock_content_stream = Mock()
    mock_content_stream.read.return_value = b"<html>content</html>"

    mock_record.rec_headers = mock_rec_headers
    mock_record.http_headers = None
    mock_record.content_stream.return_value = mock_content_stream

    with (
        patch(
            "newssearch.tasks.news_etl.utils.record_factory.extract_top_level_domain"
        ) as mock_tld,
        patch(
            "newssearch.tasks.news_etl.utils.record_factory.extract_text_content"
        ) as mock_extract,
    ):
        mock_tld.return_value = ".com"
        mock_extract.return_value = RecordContentSchema(
            title="Test Article", text="This is the full article text"
        )

        result = process_record(mock_record)

        assert result.mime_type is None


def test_extract_text_content_with_valid_html():
    """Test extract_text_content with valid HTML content"""
    mock_extracted_data = {
        "title": "Test Article",
        "excerpt": "This is a test excerpt",
        "hostname": "example.com",
        "tags": "news, test",
        "categories": "technology",
        "raw_text": "This is the full article text",
        "date": "2023-01-01T12:00:00Z",
        "filedate": "2023-01-02T10:00:00Z",
    }

    with patch(
        "newssearch.tasks.news_etl.utils.record_factory.trafilatura.extract"
    ) as mock_extract:
        mock_extract.return_value = json.dumps(mock_extracted_data)

        result = extract_text_content(b"<html>content</html>")

        assert isinstance(result, RecordContentSchema)
        assert result.title == "Test Article"
        assert result.excerpt == "This is a test excerpt"
        assert result.hostname == "example.com"
        assert result.tags == "news, test"
        assert result.categories == "technology"
        assert result.text == "This is the full article text"

        expected_date = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        expected_date_crawled = datetime(2023, 1, 2, 10, 0, 0, tzinfo=UTC)

        assert result.date == expected_date
        assert result.date_crawled == expected_date_crawled


def test_extract_text_content_with_empty_content():
    """Test extract_text_content with None content"""
    result = extract_text_content(None)
    assert result is None


def test_extract_text_content_with_trafilatura_failure():
    """Test extract_text_content when trafilatura returns None"""
    with patch(
        "newssearch.tasks.news_etl.utils.record_factory.trafilatura.extract"
    ) as mock_extract:
        mock_extract.return_value = None
        result = extract_text_content(b"<html>content</html>")
        assert result is None


def test_extract_text_content_with_partial_data():
    """Test extract_text_content with partial data from trafilatura"""
    mock_extracted_data = {
        "title": "Test Article",
        "raw_text": "This is the full article text",
    }

    with patch(
        "newssearch.tasks.news_etl.utils.record_factory.trafilatura.extract"
    ) as mock_extract:
        mock_extract.return_value = json.dumps(mock_extracted_data)

        result = extract_text_content(b"<html>content</html>")

        assert isinstance(result, RecordContentSchema)
        assert result.title == "Test Article"
        assert result.text == "This is the full article text"
        assert result.excerpt is None
        assert result.hostname is None
        assert result.tags is None
        assert result.categories is None
        assert result.date is None
        assert result.date_crawled is None


def test_extract_text_content_with_malformed_content():
    """Test extract_text_content with malformed HTML content"""
    with patch(
        "newssearch.tasks.news_etl.utils.record_factory.trafilatura.extract"
    ) as mock_extract:
        mock_extract.return_value = None
        result = extract_text_content(b"<html><broken><content</html>")

        assert result is None
