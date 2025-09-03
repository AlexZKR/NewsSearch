from typing import Any

import newssearch.tasks.news_etl.news_etl as ne_mod
from newssearch.tasks.news_etl.news_etl import NewsETL
from newssearch.tests.tasks.news_etl.dummy_helpers import DummyRecord
from newssearch.tests.tasks.news_etl.testdata import make_warc_file


def test_transform_warc_processes_only_valid_records(
    monkeypatch,
    tmp_file_path,
    mock_tqdm,
    mock_archive_iterator: list[DummyRecord],
    etl: NewsETL,
):
    def fake_process(rec: Any):
        return f"processed-{rec.rec_headers.get_header('WARC-Record-ID')}"

    monkeypatch.setattr(ne_mod, "process_record", fake_process)

    file_schema = make_warc_file("00001")
    # file_size can be arbitrary; transform_warc will open the tmp file but ArchiveIterator won't read from it
    result = etl.transform_warc(tmp_file_path, file_schema, pos=0, file_size=123)

    assert result == ["processed-id1", "processed-id3"]


def test_transform_warc_handles_processing_exceptions(  # noqa: PLR0913
    monkeypatch,
    tmp_file_path,
    caplog,
    mock_tqdm,
    mock_archive_iterator: list[DummyRecord],
    etl: NewsETL,
):
    # make process_record raise
    def raise_on_process(rec):
        raise RuntimeError("boom")

    monkeypatch.setattr(ne_mod, "process_record", raise_on_process)

    caplog.clear()
    caplog.set_level("WARNING")

    file_schema = make_warc_file("00002")
    result = etl.transform_warc(tmp_file_path, file_schema, pos=0, file_size=10)

    # exception should be caught and result should be empty
    assert result == []
    assert any("Error processing record" in rec.message for rec in caplog.records)
