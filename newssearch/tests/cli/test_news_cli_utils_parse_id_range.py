import pytest

from newssearch.cli.news_cli.utils import parse_id_range


@pytest.fixture
def available_ids() -> set[str]:
    # five zero-padded ids, width = 5
    return {f"{i:05d}" for i in range(1, 6)}


def call(raw: str, avail: set[str]):
    return parse_id_range(raw, avail)


def test_single_id_returns_same_start_and_end(available_ids):
    assert call("00003", available_ids) == ("00003", "00003")


def test_range_returns_start_and_end(available_ids):
    assert call("00002-00004", available_ids) == ("00002", "00004")


def test_range_with_spaces_and_unicode_dashes(available_ids):
    assert call("00002 - 00004", available_ids) == ("00002", "00004")
    # en-dash
    assert call("00002–00004", available_ids) == ("00002", "00004")
    # em-dash
    assert call("00002—00004", available_ids) == ("00002", "00004")


def test_dash_suffix_treated_as_single(available_ids):
    assert call("00003-", available_ids) == ("00003", "00003")


def test_numeric_short_forms_are_zero_padded(available_ids):
    # using '2' and '3' should yield zero-padded result with width from available_ids
    assert call("2-3", available_ids) == ("00002", "00003")
    assert call("4", available_ids) == ("00004", "00004")


@pytest.mark.parametrize("raw", ["", "   "])
def test_empty_input_raises(raw, available_ids):
    with pytest.raises(ValueError, match="empty input"):
        call(raw, available_ids)


def test_too_many_separators_raises(available_ids):
    with pytest.raises(ValueError, match="too many separators"):
        call("00001-00002-00003", available_ids)


def test_missing_start_raises(available_ids):
    with pytest.raises(ValueError, match="range must have a start id"):
        call("-00002", available_ids)


@pytest.mark.parametrize(
    "raw",
    [
        "abc",  # non-digit
        "0001-00a2",  # one side non-digit
    ],
)
def test_non_numeric_raises(raw, available_ids):
    with pytest.raises(ValueError, match="ids must be numeric"):
        call(raw, available_ids)


def test_start_greater_than_end_raises(available_ids):
    with pytest.raises(ValueError, match="start id must be <= end id"):
        call("00004-00002", available_ids)


def test_out_of_available_range_raises(available_ids):
    # below minimum
    with pytest.raises(ValueError, match=r"ids out of available range: 00001-00005"):
        call("00000-00002", available_ids)
    # above maximum
    with pytest.raises(ValueError, match=r"ids out of available range: 00001-00005"):
        call("00003-00099", available_ids)
