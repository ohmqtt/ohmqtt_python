import pytest

from ohmqtt.mqtt_spec import MQTTPacketType
from ohmqtt.packet import MQTTPublishPacket, MQTTPubAckPacket
from ohmqtt.persistence import InMemorySessionPersistence


def test_persistence_in_memory_qos1():
    persistence = InMemorySessionPersistence()

    assert not persistence.has("test_client")
    assert not persistence.get("test_client", set(), 1)

    pub_packets = []
    for n in range(20):
        packet = MQTTPublishPacket(
            topic="test",
            payload=f"test {n}".encode(),
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
