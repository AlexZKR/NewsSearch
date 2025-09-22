import os
import tempfile

import pytest

import newssearch.tasks.news_etl.news_etl_sync as ne_mod
from newssearch.tests.tasks.news_etl.dummy_helpers import DummyPbar
from newssearch.tests.tasks.news_etl.testdata import get_dummy_records


@pytest.fixture
def tmp_file_path():
    tf = tempfile.NamedTemporaryFile(delete=False)
    try:
        path = tf.name
    finally:
        tf.close()
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture()
def etl():
    return ne_mod.NewsETLOneThreaded(news_client=None)  # type: ignore # client not used in transform_warc


@pytest.fixture()
def mock_tqdm(monkeypatch):
    """Patch get_tqdm so transform_warc doesn't create real progress bars"""
    monkeypatch.setattr(ne_mod, "get_tqdm", lambda msg, total, pos: DummyPbar())
    yield


@pytest.fixture()
def mock_archive_iterator(monkeypatch):
    records = get_dummy_records()
    monkeypatch.setattr(ne_mod, "ArchiveIterator", lambda f: iter(records))
    yield records
