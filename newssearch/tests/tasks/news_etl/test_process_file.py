import os
import tempfile

from newssearch.tests.tasks.news_etl.testdata import make_warc_file


def test_process_file_runs(monkeypatch, etl):
    # fake iterator
    fake_iter = iter([b"chunk1", b"chunk2"])
    monkeypatch.setattr(
        etl, "client", type("C", (), {"get_warc_file": lambda self, f, p: fake_iter})()
    )

    # fake write_tmp_file
    monkeypatch.setattr(
        "newssearch.tasks.news_etl.news_etl_sync.write_tmp_file", lambda it, f: 123
    )

    # fake transform_warc
    monkeypatch.setattr(etl, "transform_warc", lambda *a, **k: ["rec1", "rec2"])

    # run (should not raise)
    etl.process_file(make_warc_file("00001"), 0)


def test_process_file_cleans_tempfile(monkeypatch, etl):
    fake_iter = iter([b"x"])
    monkeypatch.setattr(
        etl, "client", type("C", (), {"get_warc_file": lambda self, f, p: fake_iter})()
    )
    monkeypatch.setattr(
        "newssearch.tasks.news_etl.news_etl_sync.write_tmp_file", lambda it, f: 1
    )
    monkeypatch.setattr(etl, "transform_warc", lambda *a, **k: [])

    # Spy on os.unlink
    called = {}

    def fake_unlink(path):
        called["path"] = path

    monkeypatch.setattr(os, "unlink", fake_unlink)

    etl.process_file(make_warc_file("00001"), 0)

    assert "path" in called
    # ensure tempfile was deleted
    assert called["path"].startswith(tempfile.gettempdir())
