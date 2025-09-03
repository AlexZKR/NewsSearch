import pytest
import typer

import newssearch.cli.news_cli.utils
from newssearch.cli.news_cli.utils import get_files_for_download
from newssearch.infrastructure.clients.news.enums import CCDataSet
from newssearch.infrastructure.clients.news.schemas import WarcFileSchema


def make_warc_files(count: int) -> list[WarcFileSchema]:
    files: list[WarcFileSchema] = []
    for i in range(1, count + 1):
        idx = str(i).zfill(5)
        files.append(
            WarcFileSchema(
                filepath=f"crawl-data/CC-NEWS/2025/09/CC-NEWS-20250901{idx}.warc.gz",
                dataset=CCDataSet.CCNEWS,
                year="2025",
                month="09",
                timestamp=f"20250901{idx}",
                id=idx,
            )
        )
    return files


@pytest.fixture
def warc_files() -> list[WarcFileSchema]:
    return make_warc_files(5)


def make_input_func(inputs):
    it = iter(inputs)

    def _fn():
        return next(it)

    return _fn


def test_single_id_returns_single_file(monkeypatch: pytest.MonkeyPatch, warc_files):
    monkeypatch.setattr(
        newssearch.cli.news_cli.utils,
        "process_user_input",
        make_input_func(["00003"]),
    )
    result = get_files_for_download(warc_files)
    assert result == [warc_files[2]]


def test_range_returns_slice(monkeypatch, warc_files):
    monkeypatch.setattr(
        newssearch.cli.news_cli.utils,
        "process_user_input",
        make_input_func(["00002-00004"]),
    )
    result = get_files_for_download(warc_files)
    assert result == warc_files[1:4]


def test_invalid_then_valid_input_loops_until_valid(monkeypatch, warc_files):
    monkeypatch.setattr(
        newssearch.cli.news_cli.utils,
        "process_user_input",
        make_input_func(["not-a-number", "00001-00002"]),
    )
    result = get_files_for_download(warc_files)
    assert result == warc_files[0:2]


def test_quit_raises_exit(monkeypatch, warc_files):
    def raise_exit():
        raise typer.Exit(code=0)

    monkeypatch.setattr(newssearch.cli.news_cli.utils, "process_user_input", raise_exit)
    with pytest.raises(typer.Exit):
        get_files_for_download(warc_files)
