import pytest

from ohmqtt.topic_filter import MQTTTopicFilter


def test_topic_filter_properties():
    filter = MQTTTopicFilter("sport/tennis/player1")
    assert filter.topic_filter == "sport/tennis/player1"

def test_topic_filter_empty_filter():
    with pytest.raises(ValueError):
        MQTTTopicFilter("")

def test_topic_filter_empty_topic():
    filter = MQTTTopicFilter("sport/tennis/player1")
    with pytest.raises(ValueError):
        filter.match("")

def test_topic_filter_null_character_filter():
    with pytest.raises(ValueError):
        MQTTTopicFilter("sport/tennis/\u0000player1")

def test_topic_filter_null_character_topic():
    filter = MQTTTopicFilter("sport/tennis/player1")
    with pytest.raises(ValueError):
        filter.match("sport/tennis/\u0000player1")

def test_topic_filter_long_filter():
    long_filter = "a" * 65536
    with pytest.raises(ValueError):
        MQTTTopicFilter(long_filter)

def test_topic_filter_long_topic():
    filter = MQTTTopicFilter("sport/tennis/player1")
    long_topic = "a" * 65536
    with pytest.raises(ValueError):
        filter.match(long_topic)

def test_topic_filter_exact_match():
    filter = MQTTTopicFilter("sport/tennis/player1")
    assert filter.match("sport/tennis/player1")
    assert not filter.match("sport/tennis/player2")
    assert not filter.match("sport/tennis")
    assert not filter.match("sport")

def test_topic_filter_multi_level_wildcard_match():
    filter = MQTTTopicFilter("sport/tennis/player1/#")
    assert filter.match("sport/tennis/player1")
    assert filter.match("sport/tennis/player1/ranking")
    assert filter.match("sport/tennis/player1/score/wimbledon")
    assert not filter.match("sport/tennis/player2")
    assert not filter.match("sport/tennis")
    assert not filter.match("sport")

def test_topic_filter_multi_level_wildcard_match_hidden():
    filter = MQTTTopicFilter("$SYS/#")
    assert filter.match("$SYS/monitor/Clients")

def test_topic_filter_multi_level_wildcard_match_all():
    filter = MQTTTopicFilter("#")
    assert filter.match("sport/tennis/player1")
    assert filter.match("sport/tennis/player1/ranking")
    assert filter.match("sport/tennis/player1/score/wimbledon")
    assert filter.match("sport/tennis/player2")
    assert filter.match("sport/tennis")
    assert filter.match("sport")
    assert not filter.match("$SYS/monitor/Clients")

def test_topic_filter_multi_level_wildcard_invalid_filter():
    with pytest.raises(ValueError):
        MQTTTopicFilter("sport/tennis#")
    with pytest.raises(ValueError):
        MQTTTopicFilter("sport/tennis/#/ranking")

def test_topic_filter_multi_level_wildcard_invalid_topic():
    filter = MQTTTopicFilter("sport/tennis/player1/#")
    with pytest.raises(ValueError):
        filter.match("sport/tennis/player1/#")

def test_topic_filter_single_level_wildcard_match():
    filter = MQTTTopicFilter("sport/tennis/+/ranking")
    assert filter.match("sport/tennis/player1/ranking")
    assert filter.match("sport/tennis/player2/ranking")
    assert not filter.match("sport/tennis/player1")
    assert not filter.match("sport/tennis/player1/score")
    assert not filter.match("sport/tennis/player1/score/wimbledon")
    assert not filter.match("sport/tennis/ranking")

def test_topic_filter_single_level_wildcard_match_hidden():
    filter = MQTTTopicFilter("$SYS/+/Clients")
    assert filter.match("$SYS/monitor/Clients")

    filter = MQTTTopicFilter("+/monitor/Clients")
    assert not filter.match("$SYS/monitor/Clients")

def test_topic_filter_hash():
    # The hash of a TopicFilter must be the hash of the topic filter string.
    filter = MQTTTopicFilter("sport/tennis/player1")
    assert hash(filter) == hash("sport/tennis/player1")

def test_topic_filter_equality():
    filter1 = MQTTTopicFilter("sport/tennis/player1")
    filter2 = MQTTTopicFilter("sport/tennis/player1")
    filter3 = MQTTTopicFilter("sport/tennis/player2")
    assert filter1 == filter2
    assert filter1 != filter3
    assert filter1 == "sport/tennis/player1"
    assert not filter1 == "not_the_filter"
    assert not filter1 == 12345
