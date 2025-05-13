from pathlib import Path
import tempfile
from typing import Generator

import pytest

from ohmqtt.packet import MQTTPublishPacket, MQTTPubRelPacket
from ohmqtt.persistence.sqlite import SQLitePersistence
from ohmqtt.property import MQTTPublishProps
from ohmqtt.topic_alias import AliasPolicy


@pytest.fixture
def tempdbpath() -> Generator[str, None, None]:
    """Fixture to create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tempdir:
        yield str(Path(tempdir) / "test.db")


def test_persistence_sqlite_happy_path_qos1() -> None:
    """Test the SQLitePersistence class with a happy path scenario qos=1."""
    persistence = SQLitePersistence(":memory:")
    persistence.open("test_client")
    assert len(persistence) == 0

    # Add a message to the store.
    persistence.add(
        "test/topic",
        b"test payload",
        qos=1,
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
        qos=1,
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


def test_persistence_sqlite_happy_path_qos2() -> None:
    """Test the SQLitePersistence class with a happy path scenario qos=2."""
    persistence = SQLitePersistence(":memory:")
    persistence.open("test_client")
    assert len(persistence) == 0

    # Add a message to the store.
    persistence.add(
        "test/topic",
        b"test payload",
        qos=2,
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
        qos=2,
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


@pytest.mark.parametrize("db_fast", [True, False])
def test_persistence_sqlite_open(db_fast: bool, tempdbpath: str) -> None:
    """Test the SQLitePersistence class with a resume scenario."""
    persistence = SQLitePersistence(tempdbpath, db_fast=db_fast)
    persistence.open("test_client")
    assert len(persistence) == 0

    # Add a message to the store.
    packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        qos=1,
        retain=False,
        properties=MQTTPublishProps(ResponseTopic="response/topic"),
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

    # Mark the message inflight by rendering it.
    persistence.render(1)
    assert len(persistence.get(1)) == 0

    # Close and reopen the persistence store.
    persistence = SQLitePersistence(tempdbpath)
    # This should not clear the store.
    persistence.open("test_client")
    assert len(persistence) == 1

    # We should be able to retrieve the message again.
    message_ids = persistence.get(1)
    assert len(message_ids) == 1
    # When rendering a second time, the packet should have the dup flag set.
    packet.dup = True
    packet.packet_id = 1
    rendered = persistence.render(message_ids[0])
    assert rendered.packet == packet
    assert rendered.alias_policy == AliasPolicy.TRY

    # Now open with a different client ID.
    persistence = SQLitePersistence(tempdbpath)
    # This should clear the store.
    persistence.open("test_client_2")
    assert len(persistence) == 0


def test_persistence_sqlite_properties() -> None:
    persistence = SQLitePersistence(":memory:")
    persistence.open("test_client")

    # Add a message with all the properties.
    packet = MQTTPublishPacket(
        packet_id=1,
        topic="test/topic",
        payload=b"test payload",
        qos=1,
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


def test_persistence_sqlite_loose_alias() -> None:
    """We should not be able to add a message with an alias policy of ALWAYS."""
    persistence = SQLitePersistence(":memory:")
    persistence.open("test_client")

    # Add a message with all the properties.
    packet = MQTTPublishPacket(
        packet_id=1,
        topic="test/topic",
        payload=b"test payload",
        qos=1,
        retain=False,
        properties=MQTTPublishProps(),
    )
    with pytest.raises(AssertionError):
        persistence.add(
            topic=packet.topic,
            payload=packet.payload,
            qos=packet.qos,
            retain=packet.retain,
            properties=packet.properties,
            alias_policy=AliasPolicy.ALWAYS,
        )
    assert len(persistence) == 0
