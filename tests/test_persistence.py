from functools import partial
from pathlib import Path
import tempfile
from typing import Generator

import pytest

from ohmqtt.mqtt_spec import MQTTQoS
from ohmqtt.packet import MQTTPublishPacket, MQTTPubRelPacket
from ohmqtt.persistence.base import Persistence
from ohmqtt.persistence.in_memory import InMemoryPersistence
from ohmqtt.persistence.sqlite import SQLitePersistence
from ohmqtt.property import MQTTPublishProps
from ohmqtt.topic_alias import AliasPolicy


SQLiteInMemory = partial(SQLitePersistence, ":memory:")


@pytest.fixture
def tempdbpath() -> Generator[str, None, None]:
    """Fixture to create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tempdir:
        yield str(Path(tempdir) / "test.db")


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_open(persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()

    persistence.open("test_client")
    persistence.add(
        "test/topic",
        b"test payload",
        qos=MQTTQoS.Q1,
        retain=False,
        properties=MQTTPublishProps(ResponseTopic="response/topic"),
        alias_policy=AliasPolicy.TRY,
    )
    assert len(persistence) == 1
    mids = persistence.get(1)
    rendered = persistence.render(mids[0])
    assert isinstance(rendered.packet, MQTTPublishPacket)
    assert not rendered.packet.dup

    persistence.open("test_client")
    assert len(persistence) == 1
    rendered.packet.dup = True
    mids = persistence.get(1)
    assert rendered.packet == persistence.render(mids[0]).packet

    persistence.open("test_client2")
    assert len(persistence) == 0


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_happy_path_qos1(persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()
    persistence.open("test_client")
    assert len(persistence) == 0

    # Add a message to the store.
    handle = persistence.add(
        "test/topic",
        b"test payload",
        qos=MQTTQoS.Q1,
        retain=False,
        properties=MQTTPublishProps(ResponseTopic="response/topic"),
        alias_policy=AliasPolicy.TRY,
    )
    assert len(persistence) == 1

    # Retrieve the PUBLISH from the store.
    message_ids = persistence.get(10)
    assert len(message_ids) == 1
    expected_packet = MQTTPublishPacket(
        packet_id=1,
        topic="test/topic",
        payload=b"test payload",
        qos=MQTTQoS.Q1,
        retain=False,
        properties=MQTTPublishProps(ResponseTopic="response/topic"),
    )

    # Render the message, marking it as inflight.
    rendered = persistence.render(message_ids[0])
    assert rendered.packet == expected_packet
    assert rendered.alias_policy == AliasPolicy.TRY
    assert len(persistence) == 1

    # We should not be able to retrieve the message again.
    assert len(persistence.get(10)) == 0

    # Acknowledge the message.
    persistence.ack(1)
    assert len(persistence) == 0
    assert handle.is_acked()


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_happy_path_qos2(persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()
    persistence.open("test_client")
    assert len(persistence) == 0

    # Add a message to the store.
    handle = persistence.add(
        "test/topic",
        b"test payload",
        qos=MQTTQoS.Q2,
        retain=False,
        properties=MQTTPublishProps(ResponseTopic="response/topic"),
        alias_policy=AliasPolicy.TRY,
    )
    assert len(persistence) == 1

    # Retrieve the PUBLISH from the store.
    message_ids = persistence.get(10)
    assert len(message_ids) == 1
    expected_publish = MQTTPublishPacket(
        packet_id=1,
        topic="test/topic",
        payload=b"test payload",
        qos=MQTTQoS.Q2,
        retain=False,
        properties=MQTTPublishProps(ResponseTopic="response/topic"),
    )

    # Render the message, marking it as inflight.
    rendered = persistence.render(message_ids[0])
    assert rendered.packet == expected_publish
    assert rendered.alias_policy == AliasPolicy.TRY
    assert len(persistence) == 1

    # We should not be able to retrieve the message again.
    assert len(persistence.get(10)) == 0

    # Acknowledge the message.
    persistence.ack(1)
    assert len(persistence) == 1
    assert not handle.is_acked()

    # Retrieve the PUBREL from the store.
    message_ids = persistence.get(10)
    assert len(message_ids) == 1
    expected_pubrel = MQTTPubRelPacket(packet_id=1)

    # Render the message, marking it as inflight.
    rendered = persistence.render(message_ids[0])
    assert rendered.packet == expected_pubrel
    assert rendered.alias_policy == AliasPolicy.NEVER
    assert len(persistence) == 1

    # We should not be able to retrieve the message again.
    assert len(persistence.get(10)) == 0

    # Acknowledge the message.
    persistence.ack(1)
    assert len(persistence) == 0
    assert handle.is_acked()


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_properties(persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()
    persistence.open("test_client")

    # Add a message with all the properties.
    packet = MQTTPublishPacket(
        packet_id=1,
        topic="test/topic",
        payload=b"test payload",
        qos=MQTTQoS.Q1,
        retain=False,
        properties=MQTTPublishProps(
            ResponseTopic="response/topic",
            CorrelationData=b"correlation data",
            MessageExpiryInterval=60,
            UserProperty=[("key", "value")],
            SubscriptionIdentifier={123},
        ),
    )
    persistence.add(
        topic=packet.topic,
        payload=packet.payload,
        qos=packet.qos,
        retain=packet.retain,
        properties=packet.properties,
        alias_policy=AliasPolicy.TRY,
    )
    assert len(persistence) == 1

    # Retrieve the PUBLISH from the store.
    message_ids = persistence.get(10)
    rendered = persistence.render(message_ids[0])
    assert rendered.packet == packet


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_loose_alias(persistence_class: type[Persistence]) -> None:
    """We should not be able to add a message with an alias policy of ALWAYS."""
    persistence = persistence_class()
    persistence.open("test_client")

    # Add a message with all the properties.
    packet = MQTTPublishPacket(
        packet_id=1,
        topic="test/topic",
        payload=b"test payload",
        qos=MQTTQoS.Q1,
        retain=False,
        properties=MQTTPublishProps(),
    )
    with pytest.raises(ValueError):
        persistence.add(
            topic=packet.topic,
            payload=packet.payload,
            qos=packet.qos,
            retain=packet.retain,
            properties=packet.properties,
            alias_policy=AliasPolicy.ALWAYS,
        )
    assert len(persistence) == 0


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_incoming_qos2(persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()
    persistence.open("test_client")

    pub_packet = MQTTPublishPacket(
        packet_id=1,
        qos=MQTTQoS.Q1,
    )
    with pytest.raises(ValueError):
        persistence.check_rec(pub_packet)
    with pytest.raises(ValueError):
        persistence.set_rec(pub_packet)

    pub_packet.qos = MQTTQoS.Q2

    assert persistence.check_rec(pub_packet) is True
    persistence.set_rec(pub_packet)
    assert persistence.check_rec(pub_packet) is False

    rel_packet = MQTTPubRelPacket(packet_id=1)
    persistence.rel(rel_packet)

    assert persistence.check_rec(pub_packet) is True


@pytest.mark.parametrize("qos", [MQTTQoS.Q1, MQTTQoS.Q2])
@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_queue(qos: MQTTQoS, persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()
    persistence.open("test_client")

    rng = range(1, 3)
    for n in rng:
        persistence.add(
            topic=str(n),
            payload=b"bar",
            qos=qos,
            retain=False,
            properties=MQTTPublishProps(),
            alias_policy=AliasPolicy.NEVER,
        )
    for n in rng:
        queued = persistence.get(3)
        rendered = persistence.render(queued[0])
        assert isinstance(rendered.packet, MQTTPublishPacket)
        assert rendered.packet.topic == str(n)
        assert rendered.packet.packet_id == n


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_render_order(persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()
    persistence.open("test_client")

    for _ in (1, 2):
        persistence.add(
            topic="foo",
            payload=b"bar",
            qos=MQTTQoS.Q1,
            retain=False,
            properties=MQTTPublishProps(),
            alias_policy=AliasPolicy.NEVER,
        )
    queued = persistence.get(3)
    with pytest.raises(ValueError):
        persistence.render(queued[1])


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_unknown_ack(persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()
    persistence.open("test_client")

    with pytest.raises(ValueError, match="Unknown packet_id: 42"):
        persistence.ack(42)


@pytest.mark.parametrize("persistence_class", [SQLiteInMemory, InMemoryPersistence])
def test_persistence_unknown_render(persistence_class: type[Persistence]) -> None:
    persistence = persistence_class()
    persistence.open("test_client")

    with pytest.raises(KeyError):
        persistence.render(42)


def test_persistence_out_of_ids() -> None:
    """InMemoryPersistence does not allow more than USHRT_MAX unackowledged outbound messages."""
    persistence = InMemoryPersistence()
    persistence.open("test_client")

    for _ in range(65535):
        persistence.add("foo", b"bar", MQTTQoS.Q1, False, MQTTPublishProps(), AliasPolicy.NEVER)
    with pytest.raises(ValueError):
        persistence.add("foo", b"bar", MQTTQoS.Q1, False, MQTTPublishProps(), AliasPolicy.NEVER)


@pytest.mark.parametrize("db_fast", [True, False])
def test_persistence_sqlite_open(db_fast: bool, tempdbpath: str) -> None:
    """Test the SQLitePersistence class with a resume scenario."""
    persistence = SQLitePersistence(tempdbpath, db_fast=db_fast)
    persistence.open("test_client")
    assert len(persistence) == 0

    # Add a message to the store.
    outgoing_packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        qos=MQTTQoS.Q1,
        retain=False,
        properties=MQTTPublishProps(ResponseTopic="response/topic"),
    )
    persistence.add(
        topic=outgoing_packet.topic,
        payload=outgoing_packet.payload,
        qos=outgoing_packet.qos,
        retain=outgoing_packet.retain,
        properties=outgoing_packet.properties,
        alias_policy=AliasPolicy.TRY,
    )
    assert len(persistence) == 1

    # Mark the message inflight by rendering it.
    persistence.render(1)
    assert len(persistence.get(1)) == 0

    # Simulate an incoming QoS 2 message.
    incoming_packet = MQTTPublishPacket(packet_id=1, qos=MQTTQoS.Q2)
    persistence.set_rec(incoming_packet)

    # Close and reopen the persistence store.
    persistence = SQLitePersistence(tempdbpath)
    # This should not clear the store.
    persistence.open("test_client")
    assert len(persistence) == 1

    # We should be able to retrieve the message again.
    message_ids = persistence.get(1)
    assert len(message_ids) == 1
    # When rendering a second time, the packet should have the dup flag set.
    outgoing_packet.dup = True
    outgoing_packet.packet_id = 1
    rendered = persistence.render(message_ids[0])
    assert rendered.packet == outgoing_packet
    assert rendered.alias_policy == AliasPolicy.TRY

    # We should filter a duplicate incoming QoS 2 message.
    assert persistence.check_rec(incoming_packet) is False

    # Now open with a different client ID.
    persistence = SQLitePersistence(tempdbpath)
    # This should clear the store.
    persistence.open("test_client_2")
    assert len(persistence) == 0
    assert persistence.check_rec(incoming_packet) is True
