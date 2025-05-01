import pytest

from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.topic_alias import AliasPolicy, MaxOutboundAliasError, TopicAlias


def test_topic_alias_outbound():
    """Test the outbound topic alias lookup."""
    topic_alias = TopicAlias()
    topic_alias.max_out_alias = 10

    # Test with policy NEVER
    result = topic_alias.lookup_outbound("test/topic", AliasPolicy.NEVER)
    assert result == (0, False)

    # Test with policy TRY
    result = topic_alias.lookup_outbound("test/topic", AliasPolicy.TRY)
    assert result == (1, False)

    # Test with policy ALWAYS
    result = topic_alias.lookup_outbound("test/topic2", AliasPolicy.ALWAYS)
    assert result == (2, False)

    # Test with existing alias
    result = topic_alias.lookup_outbound("test/topic2", AliasPolicy.ALWAYS)
    assert result == (2, True)

    # Test with out of aliases
    topic_alias.max_out_alias = 2

    result = topic_alias.lookup_outbound("test/topic3", AliasPolicy.TRY)
    assert result == (0, False)

    with pytest.raises(MaxOutboundAliasError):
        topic_alias.lookup_outbound("test/topic4", AliasPolicy.ALWAYS)

    # Test reset
    topic_alias.reset()
    assert topic_alias.out_aliases == {}
    assert topic_alias.next_out_alias == 1
    assert topic_alias.max_out_alias == 2  # Should not reset


def test_topic_alias_inbound():
    """Test the inbound topic alias store."""
    topic_alias = TopicAlias()

    # Test with max alias = 0
    try:
        topic_alias.lookup_inbound(1)
    except MQTTError as e:
        assert e.reason_code == MQTTReasonCode.TopicAliasInvalid
    else:
        pytest.fail("Expected MQTTError")

    try:
        topic_alias.store_inbound(1, "test/topic")
    except MQTTError as e:
        assert e.reason_code == MQTTReasonCode.TopicAliasInvalid
    else:
        pytest.fail("Expected MQTTError")

    # Test with max alias = 1
    topic_alias.max_in_alias = 1
    topic_alias.store_inbound(1, "test/topic")
    result = topic_alias.lookup_inbound(1)
    assert result == "test/topic"

    # Trying to overwrite the same alias with a different topic
    try:
        topic_alias.store_inbound(1, "test/topic2")
    except MQTTError as e:
        assert e.reason_code == MQTTReasonCode.TopicAliasInvalid
    else:
        pytest.fail("Expected MQTTError")

    # Trying to exceed the max alias
    try:
        topic_alias.store_inbound(2, "test/topic2")
    except MQTTError as e:
        assert e.reason_code == MQTTReasonCode.TopicAliasInvalid
    else:
        pytest.fail("Expected MQTTError")

    # Lookup non-existing alias
    topic_alias.max_in_alias = 2
    try:
        topic_alias.lookup_inbound(2)
    except MQTTError as e:
        assert e.reason_code == MQTTReasonCode.TopicAliasInvalid
    else:
        pytest.fail("Expected MQTTError")

    # Test reset
    topic_alias.reset()
    assert topic_alias.in_aliases == {}
    assert topic_alias.max_in_alias == 0
