import pytest

from ohmqtt.topic_filter import validate_topic_filter, match_topic_filter


def test_topic_filter_empty_filter():
    filter = ""
    with pytest.raises(ValueError):
        validate_topic_filter(filter)

def test_topic_filter_empty_topic():
    filter = "sport/tennis/player1"
    with pytest.raises(ValueError):
        match_topic_filter(filter, "")

def test_topic_filter_null_character_filter():
    with pytest.raises(ValueError):
        validate_topic_filter("sport/tennis/\u0000player1")

def test_topic_filter_null_character_topic():
    filter = "sport/tennis/player1"
    with pytest.raises(ValueError):
        match_topic_filter(filter, "sport/tennis/\u0000player1")

def test_topic_filter_long_filter():
    filter = "a" * 65536
    with pytest.raises(ValueError):
        validate_topic_filter(filter)

def test_topic_filter_long_topic():
    filter = "sport/tennis/player1"
    long_topic = "a" * 65536
    with pytest.raises(ValueError):
        match_topic_filter(filter, long_topic)

def test_topic_filter_exact_match():
    filter = "sport/tennis/player1"
    assert match_topic_filter(filter, "sport/tennis/player1")
    assert not match_topic_filter(filter, "sport/tennis/player2")
    assert not match_topic_filter(filter, "sport/tennis")
    assert not match_topic_filter(filter, "sport")

def test_topic_filter_multi_level_wildcard_match():
    filter = "sport/tennis/player1/#"
    assert match_topic_filter(filter, "sport/tennis/player1")
    assert match_topic_filter(filter, "sport/tennis/player1/ranking")
    assert match_topic_filter(filter, "sport/tennis/player1/score/wimbledon")
    assert not match_topic_filter(filter, "sport/tennis/player2")
    assert not match_topic_filter(filter, "sport/tennis")
    assert not match_topic_filter(filter, "sport")

def test_topic_filter_multi_level_wildcard_match_hidden():
    filter = "$SYS/#"
    assert match_topic_filter(filter, "$SYS/monitor/Clients")

def test_topic_filter_multi_level_wildcard_match_all():
    filter = "#"
    assert match_topic_filter(filter, "sport/tennis/player1")
    assert match_topic_filter(filter, "sport/tennis/player1/ranking")
    assert match_topic_filter(filter, "sport/tennis/player1/score/wimbledon")
    assert match_topic_filter(filter, "sport/tennis/player2")
    assert match_topic_filter(filter, "sport/tennis")
    assert match_topic_filter(filter, "sport")
    assert not match_topic_filter(filter, "$SYS/monitor/Clients")

def test_topic_filter_multi_level_wildcard_invalid_filter():
    with pytest.raises(ValueError):
        validate_topic_filter("sport/tennis#")
    with pytest.raises(ValueError):
        validate_topic_filter("sport/tennis/#/ranking")

def test_topic_filter_multi_level_wildcard_invalid_topic():
    filter = "sport/tennis/player1/#"
    with pytest.raises(ValueError):
        match_topic_filter(filter, "sport/tennis/player1/#")

def test_topic_filter_single_level_wildcard_match():
    filter = "sport/tennis/+/ranking"
    assert match_topic_filter(filter, "sport/tennis/player1/ranking")
    assert match_topic_filter(filter, "sport/tennis/player2/ranking")
    assert not match_topic_filter(filter, "sport/tennis/player1")
    assert not match_topic_filter(filter, "sport/tennis/player1/score")
    assert not match_topic_filter(filter, "sport/tennis/player1/score/wimbledon")
    assert not match_topic_filter(filter, "sport/tennis/ranking")

def test_topic_filter_single_level_wildcard_match_hidden():
    filter = "$SYS/+/Clients"
    assert match_topic_filter(filter, "$SYS/monitor/Clients")

    filter = "+/monitor/Clients"
    assert not match_topic_filter(filter, "$SYS/monitor/Clients")
