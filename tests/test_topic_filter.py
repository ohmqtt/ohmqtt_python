import pytest

from ohmqtt.topic_filter import validate_topic_filter, validate_share_name, match_topic_filter, join_share


def test_topic_filter_empty_filter() -> None:
    f = ""
    with pytest.raises(ValueError):
        validate_topic_filter(f)

def test_topic_filter_empty_topic() -> None:
    f = "sport/tennis/player1"
    with pytest.raises(ValueError):
        match_topic_filter(f, "")

def test_topic_filter_null_character_filter() -> None:
    with pytest.raises(ValueError):
        validate_topic_filter("sport/tennis/\u0000player1")

def test_topic_filter_null_character_topic() -> None:
    f = "sport/tennis/player1"
    with pytest.raises(ValueError):
        match_topic_filter(f, "sport/tennis/\u0000player1")

def test_topic_filter_long_filter() -> None:
    f = "a" * 65536
    with pytest.raises(ValueError):
        validate_topic_filter(f)

def test_topic_filter_long_topic() -> None:
    f = "sport/tennis/player1"
    long_topic = "a" * 65536
    with pytest.raises(ValueError):
        match_topic_filter(f, long_topic)

def test_topic_filter_exact_match() -> None:
    f = "sport/tennis/player1"
    assert match_topic_filter(f, "sport/tennis/player1")
    assert not match_topic_filter(f, "sport/tennis/player2")
    assert not match_topic_filter(f, "sport/tennis")
    assert not match_topic_filter(f, "sport")

def test_topic_filter_multi_level_wildcard_match() -> None:
    f = "sport/tennis/player1/#"
    assert match_topic_filter(f, "sport/tennis/player1")
    assert match_topic_filter(f, "sport/tennis/player1/ranking")
    assert match_topic_filter(f, "sport/tennis/player1/score/wimbledon")
    assert not match_topic_filter(f, "sport/tennis/player2")
    assert not match_topic_filter(f, "sport/tennis")
    assert not match_topic_filter(f, "sport")

def test_topic_filter_multi_level_wildcard_match_hidden() -> None:
    f = "$SYS/#"
    assert match_topic_filter(f, "$SYS/monitor/Clients")

def test_topic_filter_multi_level_wildcard_match_all() -> None:
    f = "#"
    assert match_topic_filter(f, "sport/tennis/player1")
    assert match_topic_filter(f, "sport/tennis/player1/ranking")
    assert match_topic_filter(f, "sport/tennis/player1/score/wimbledon")
    assert match_topic_filter(f, "sport/tennis/player2")
    assert match_topic_filter(f, "sport/tennis")
    assert match_topic_filter(f, "sport")
    assert not match_topic_filter(f, "$SYS/monitor/Clients")

def test_topic_filter_multi_level_wildcard_invalid_filter() -> None:
    with pytest.raises(ValueError):
        validate_topic_filter("sport/tennis#")
    with pytest.raises(ValueError):
        validate_topic_filter("sport/tennis/#/ranking")

def test_topic_filter_multi_level_wildcard_invalid_topic() -> None:
    f = "sport/tennis/player1/#"
    with pytest.raises(ValueError):
        match_topic_filter(f, "sport/tennis/player1/#")

def test_topic_filter_single_level_wildcard_match() -> None:
    f = "sport/tennis/+/ranking"
    assert match_topic_filter(f, "sport/tennis/player1/ranking")
    assert match_topic_filter(f, "sport/tennis/player2/ranking")
    assert not match_topic_filter(f, "sport/tennis/player1")
    assert not match_topic_filter(f, "sport/tennis/player1/score")
    assert not match_topic_filter(f, "sport/tennis/player1/score/wimbledon")
    assert not match_topic_filter(f, "sport/tennis/ranking")

def test_topic_filter_single_level_wildcard_match_hidden() -> None:
    f = "$SYS/+/Clients"
    assert match_topic_filter(f, "$SYS/monitor/Clients")

    f = "+/monitor/Clients"
    assert not match_topic_filter(f, "$SYS/monitor/Clients")

def test_topic_filter_validate_share_name() -> None:
    for share_name in ["", "a/b", "a" * 65536, "\u0000", "#", "+"]:
        try:
            validate_share_name(share_name)
        except ValueError:
            pass
        else:
            pytest.fail(f"Expected ValueError for share name: {share_name}")
    for share_name in ["(foo)", "B A R", "baz-23"]:
        validate_share_name(share_name)

def test_topic_filter_join_share() -> None:
    assert join_share("sport/tennis/player1", "foo") == "$share/foo/sport/tennis/player1"
    assert join_share("sport/tennis/player1", None) == "sport/tennis/player1"
    assert join_share("$SYS/monitor/Clients", "foo") == "$share/foo/$SYS/monitor/Clients"
    assert join_share("$SYS/monitor/Clients", None) == "$SYS/monitor/Clients"
