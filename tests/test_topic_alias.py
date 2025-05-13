import pytest

from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import MQTTPublishPacket
from ohmqtt.topic_alias import AliasPolicy, MaxOutboundAliasError, InboundTopicAlias, OutboundTopicAlias


def make_packet(topic: str = "", alias: int | None = None) -> MQTTPublishPacket:
    """Create a MQTTPublishPacket with the given topic and alias."""
    packet = MQTTPublishPacket(topic=topic)
    if alias is not None:
        packet.properties.TopicAlias = alias
    return packet


def test_topic_alias_outbound() -> None:
    """Test the outbound topic alias lookup."""
    topic_alias = OutboundTopicAlias()
    topic_alias.max_alias = 10

    # Test with policy NEVER
    result = topic_alias.lookup("test/topic", AliasPolicy.NEVER)
    assert result == (0, False)

    # Test with policy TRY
    result = topic_alias.lookup("test/topic", AliasPolicy.TRY)
    assert result == (1, False)

    # Test with policy ALWAYS
    result = topic_alias.lookup("test/topic2", AliasPolicy.ALWAYS)
    assert result == (2, False)

    # Test with existing alias
    result = topic_alias.lookup("test/topic2", AliasPolicy.ALWAYS)
    assert result == (2, True)

    # Test with out of aliases
    topic_alias.max_alias = 2

    result = topic_alias.lookup("test/topic3", AliasPolicy.TRY)
    assert result == (0, False)

    with pytest.raises(MaxOutboundAliasError):
        topic_alias.lookup("test/topic4", AliasPolicy.ALWAYS)

    # Remove an alias
    topic_alias.pop()
    result = topic_alias.lookup("test/topic4", AliasPolicy.ALWAYS)
    assert result == (2, False)

    # Test reset
    topic_alias.reset()
    assert topic_alias.aliases == {}
    assert topic_alias.next_alias == 1
    assert topic_alias.max_alias == 2  # Should not reset


def test_topic_alias_inbound() -> None:
    """Test the inbound topic alias store."""
    topic_alias = InboundTopicAlias()

    # Unaliased, noop
    packet = make_packet("test/topic")
    topic_alias.handle(packet)
    assert packet == make_packet("test/topic")

    # No topic or alias, explode
    with pytest.raises(MQTTError) as exc:
        topic_alias.handle(make_packet())
    assert exc.value.reason_code == MQTTReasonCode.ProtocolError

    # Test with max alias = 0
    with pytest.raises(MQTTError) as exc:
        topic_alias.handle(make_packet(alias=1))
    assert exc.value.reason_code == MQTTReasonCode.TopicAliasInvalid

    with pytest.raises(MQTTError) as exc:
        topic_alias.handle(make_packet("x", 1))
    assert exc.value.reason_code == MQTTReasonCode.TopicAliasInvalid

    # Test with max alias = 1
    topic_alias.max_alias = 1
    topic_alias.handle(make_packet("test/topic", 1))
    packet = make_packet(alias=1)
    topic_alias.handle(packet)
    assert packet.topic == "test/topic"

    # Trying to overwrite the same alias with a different topic
    with pytest.raises(MQTTError) as exc:
        topic_alias.handle(make_packet("test/topic2", 1))
    assert exc.value.reason_code == MQTTReasonCode.TopicAliasInvalid

    # Trying to exceed the max alias
    with pytest.raises(MQTTError) as exc:
        topic_alias.handle(make_packet("test/topic3", 2))
    assert exc.value.reason_code == MQTTReasonCode.TopicAliasInvalid

    # Lookup non-existing alias
    topic_alias.max_alias = 3
    with pytest.raises(MQTTError) as exc:
        topic_alias.handle(make_packet(alias=3))
    assert exc.value.reason_code == MQTTReasonCode.TopicAliasInvalid

    # Test reset
    topic_alias.reset()
    assert topic_alias.aliases == {}
    assert topic_alias.max_alias == 0
