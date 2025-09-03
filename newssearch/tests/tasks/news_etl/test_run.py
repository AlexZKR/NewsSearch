import concurrent.futures as cf

from pytest import LogCaptureFixture, MonkeyPatch

from newssearch.infrastructure.clients.news.schemas import WarcFileSchema
from newssearch.tasks.news_etl.news_etl import NewsETL
from newssearch.tests.tasks.news_etl.testdata import make_warc_file


class DummyExecutor:
    def __init__(self, max_workers: int = 1) -> None:
        self._submitted: list[cf.Future] = []

    def __enter__(self) -> "DummyExecutor":
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, *args, **kwargs) -> cf.Future:
        fut = cf.Future()  # type: ignore [var-annotated]
        self._submitted.append(fut)
        try:
            res = fn(*args, **kwargs)
        except BaseException as e:  # capture and set exception on future
            fut.set_exception(e)
        else:
            fut.set_result(res)
        return fut


def _as_completed(iterable_of_futures):
    yield from iterable_of_futures


def test_run_calls_process_file_and_logs_success(
    monkeypatch: MonkeyPatch,
    caplog: LogCaptureFixture,
    etl: NewsETL,
):
    ne_mod = __import__("newssearch.tasks.news_etl.news_etl", fromlist=["*"])
    monkeypatch.setattr(ne_mod.concurrent.futures, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(ne_mod.concurrent.futures, "as_completed", _as_completed)

    called: list[tuple[str, int]] = []

    def fake_process_file(self: NewsETL, file: WarcFileSchema, position: int) -> None:
        called.append((file.id, position))

    monkeypatch.setattr(NewsETL, "process_file", fake_process_file)

    files = [make_warc_file("00001"), make_warc_file("00002")]

    caplog.clear()
    caplog.set_level("INFO")

    etl.run(files)

    assert called == [("00001", 0), ("00002", 1)]
    # two success messages should be in logs
    assert any("Successfully processed 00001" in r.message for r in caplog.records)
    assert any("Successfully processed 00002" in r.message for r in caplog.records)


def test_run_logs_error_when_process_file_raises(monkeypatch, caplog, etl):
    ne_mod = __import__("newssearch.tasks.news_etl.news_etl", fromlist=["*"])
    monkeypatch.setattr(ne_mod.concurrent.futures, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(ne_mod.concurrent.futures, "as_completed", _as_completed)

    def fake_process_file(self: NewsETL, file: WarcFileSchema, position: int) -> None:
        if file.id == "00002":
            raise RuntimeError("boom")

    monkeypatch.setattr(NewsETL, "process_file", fake_process_file)

    files = [make_warc_file("00001"), make_warc_file("00002"), make_warc_file("00003")]

    caplog.clear()
    caplog.set_level("ERROR")

    etl.run(files)

    # file 00001 and 00003 processed successfully -> info messages not checked here
    # file 00002 should have an error log
    assert any("Error processing 00002" in r.message for r in caplog.records)
