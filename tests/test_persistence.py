import pytest

from ohmqtt.mqtt_spec import MQTTPacketType, MQTTReasonCode
from ohmqtt.packet import (
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
    MQTTSubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubscribePacket,
    MQTTUnsubAckPacket,
)
from ohmqtt.persistence import InMemorySessionPersistence


def test_persistence_in_memory_opers():
    persistence = InMemorySessionPersistence()

    assert not persistence.has("test_client")
    assert not persistence.get("test_client", set(), 1)

    pub_packets = []
    for n in range(20):
        packet = MQTTPublishPacket(
            topic="test",
            payload=f"test {n}".encode("utf-8"),
            packet_id=persistence.next_packet_id("test_client", MQTTPacketType.PUBLISH),
            qos=1,
        )
        pub_packets.append(packet)
        persistence.put("test_client", packet)

    assert persistence.has("test_client")
    persistence.clear("test_client")
    assert not persistence.has("test_client")
    for pub_packet in pub_packets:
        persistence.put("test_client", pub_packet)
    assert persistence.next_packet_id("test_client", MQTTPacketType.PUBLISH) == 21

    gotten = persistence.get("test_client", set(), 20)
    assert gotten == pub_packets[:]

    gotten = persistence.get("test_client", set(), 10)
    assert gotten == pub_packets[:10]

    gotten = persistence.get("test_client", set(pub_packets[:10]), 10)
    assert gotten == pub_packets[10:]

    persistence.mark_dup("test_client", pub_packets[0].packet_id)
    gotten = persistence.get("test_client", set(), 20)
    assert gotten[1:] == pub_packets[1:]
    assert gotten[0].dup

    ack_packets = [
        MQTTPubAckPacket(
            packet_id=pub_packet.packet_id,
        ) for pub_packet in pub_packets
    ]
    for ack_packet in ack_packets:
        persistence.ack("test_client", ack_packet)

    gotten = persistence.get("test_client", set(), 10)
    assert not gotten

    assert not persistence.has("test_client")


def test_persistence_in_memory_qos2():
    persistence = InMemorySessionPersistence()

    pub_packet = MQTTPublishPacket(
        topic="test",
        payload=b"test",
        packet_id=persistence.next_packet_id("test_client", MQTTPacketType.PUBLISH),
        qos=2,
    )
    persistence.put("test_client", pub_packet)

    rec_packet = MQTTPubRecPacket(packet_id=pub_packet.packet_id)
    persistence.ack("test_client", rec_packet)
    gotten = persistence.get("test_client", set(), 10)
    assert not gotten

    rel_packet = MQTTPubRelPacket(packet_id=pub_packet.packet_id)
    persistence.put("test_client", rel_packet)

    comp_packet = MQTTPubCompPacket(packet_id=pub_packet.packet_id)
    persistence.ack("test_client", comp_packet)
    gotten = persistence.get("test_client", set(), 10)
    assert not gotten


def test_persistence_in_memory_sub_unsub():
    persistence = InMemorySessionPersistence()

    sub_packet = MQTTSubscribePacket(
        topics=[("test", 1)],
        packet_id=persistence.next_packet_id("test_client", MQTTPacketType.SUBSCRIBE),
    )
    persistence.put("test_client", sub_packet)

    suback_packet = MQTTSubAckPacket(packet_id=sub_packet.packet_id, reason_codes=(MQTTReasonCode.GrantedQoS1,))
    persistence.ack("test_client", suback_packet)
    gotten = persistence.get("test_client", set(), 10)
    assert not gotten

    unsub_packet = MQTTUnsubscribePacket(
        topics=["test"],
        packet_id=persistence.next_packet_id("test_client", MQTTPacketType.UNSUBSCRIBE),
    )
    persistence.put("test_client", unsub_packet)

    unsuback_packet = MQTTUnsubAckPacket(packet_id=unsub_packet.packet_id, reason_codes=(MQTTReasonCode.Success,))
    persistence.ack("test_client", unsuback_packet)
    gotten = persistence.get("test_client", set(), 10)
    assert not gotten


def test_persistence_in_memory_errors():
    persistence = InMemorySessionPersistence()
    pub_packet = MQTTPublishPacket(
        topic="test",
        payload=b"test",
        packet_id=1,
        qos=1,
    )
    ack_packet = MQTTPubAckPacket(packet_id=2)
    with pytest.raises(KeyError):
        # Client does not exist.
        persistence.mark_dup("test_client", 1)
    with pytest.raises(KeyError):
        # Client does not exist.
        persistence.ack("test_client", ack_packet)
    with pytest.raises(ValueError):
        # Bad packet type.
        persistence.ack("test_client", pub_packet)

    # Create a valid session.
    persistence.put("test_client", pub_packet)

    with pytest.raises(KeyError):
        # Packet does not exist.
        persistence.mark_dup("test_client", 2)
    with pytest.raises(KeyError):
        # Packet does not exist.
        persistence.ack("test_client", ack_packet)

    persistence.clear("test_client")

    # Fill up the session with publish packet IDs.
    for n in range(1, 65536):
        p = MQTTPublishPacket(
            topic="test",
            payload=b"test",
            packet_id=n,
            qos=1,
        )
        persistence.put("test_client", p)
    with pytest.raises(ValueError):
        # No free packet IDs.
        persistence.next_packet_id("test_client", MQTTPacketType.PUBLISH)
