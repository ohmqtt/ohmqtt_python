from typing import Callable

import pytest

from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTPropertyId, MQTTReasonCode
from ohmqtt.property import (
    MQTTConnAckProps,
    MQTTConnectProps,
    MQTTDisconnectProps,
    MQTTProperties,
    MQTTPubAckProps,
    MQTTPublishProps,
    MQTTSubscribeProps,
    MQTTWillProps,
)
from ohmqtt.serialization import encode_string, encode_uint8, encode_varint, encode_uint16, encode_uint32


def test_property_duplicate_key() -> None:
    data = bytearray(encode_varint(MQTTPropertyId.TopicAlias) + encode_uint16(1))
    data.extend(encode_varint(MQTTPropertyId.TopicAlias) + encode_uint16(2))
    data[0:0] = encode_varint(len(data))
    view = memoryview(data)
    view.toreadonly()

    with pytest.raises(MQTTError, match=r"Duplicate") as excinfo:
        MQTTPublishProps.decode(view)
    assert excinfo.value.reason_code == MQTTReasonCode.ProtocolError


def test_property_length_exceeds_data() -> None:
    data = encode_varint(10) + encode_varint(MQTTPropertyId.TopicAlias) + encode_uint16(1)

    with pytest.raises(MQTTError, match=r"exceeds packet length") as excinfo:
        MQTTPublishProps.decode(memoryview(data))
    assert excinfo.value.reason_code == MQTTReasonCode.MalformedPacket


def test_property_invalid_identifier() -> None:
    data = encode_varint(2) + b"\x05\x00"

    with pytest.raises(MQTTError, match=r"Invalid property identifier 5") as excinfo:
        MQTTPublishProps.decode(memoryview(data))
    assert excinfo.value.reason_code == MQTTReasonCode.MalformedPacket


@pytest.mark.parametrize(("props_type", "prop"), [
    pytest.param(MQTTPublishProps, encode_varint(MQTTPropertyId.ServerKeepAlive) + encode_uint16(5), id="publish"),
    pytest.param(MQTTPubAckProps, encode_varint(MQTTPropertyId.TopicAlias) + encode_uint16(1), id="puback"),
    pytest.param(MQTTConnAckProps, encode_varint(MQTTPropertyId.WillDelayInterval) + encode_uint32(1), id="connack"),
    pytest.param(MQTTDisconnectProps, encode_varint(MQTTPropertyId.AuthenticationMethod) + encode_string("x"), id="disconnect"),
    pytest.param(MQTTWillProps, encode_varint(MQTTPropertyId.TopicAlias) + encode_uint16(1), id="will"),
])
def test_property_not_valid_for_packet(props_type: type[MQTTProperties], prop: bytes) -> None:
    data = encode_varint(len(prop)) + prop

    with pytest.raises(MQTTError, match=r"not valid in") as excinfo:
        props_type.decode(memoryview(data))
    assert excinfo.value.reason_code == MQTTReasonCode.MalformedPacket


@pytest.mark.parametrize(("props_type", "prop_id", "encoder", "valid", "invalid"), [
    pytest.param(MQTTPublishProps, MQTTPropertyId.PayloadFormatIndicator, encode_uint8, [0, 1], [2, 255], id="PayloadFormatIndicator"),
    pytest.param(MQTTPublishProps, MQTTPropertyId.SubscriptionIdentifier, encode_varint, [1, 268_435_455], [0], id="SubscriptionIdentifier"),
    pytest.param(MQTTSubscribeProps, MQTTPropertyId.SubscriptionIdentifier, encode_varint, [1], [0], id="SubscribeSubscriptionIdentifier"),
    pytest.param(MQTTConnAckProps, MQTTPropertyId.ReceiveMaximum, encode_uint16, [1, 65535], [0], id="ReceiveMaximum"),
    pytest.param(MQTTConnectProps, MQTTPropertyId.ReceiveMaximum, encode_uint16, [1], [0], id="ConnectReceiveMaximum"),
    pytest.param(MQTTConnAckProps, MQTTPropertyId.MaximumQoS, encode_uint8, [0, 1], [2, 255], id="MaximumQoS"),
    pytest.param(MQTTConnAckProps, MQTTPropertyId.MaximumPacketSize, encode_uint32, [1, 0xffffffff], [0], id="MaximumPacketSize"),
])
def test_property_value_ranges(
    props_type: type[MQTTProperties],
    prop_id: MQTTPropertyId,
    encoder: Callable[[int], bytes],
    valid: list[int],
    invalid: list[int],
) -> None:
    for value in valid + invalid:
        prop = encode_varint(prop_id) + encoder(value)
        data = memoryview(encode_varint(len(prop)) + prop)
        if value in valid:
            decoded, _ = props_type.decode(data)
            expected = {value} if prop_id == MQTTPropertyId.SubscriptionIdentifier else value
            assert getattr(decoded, prop_id.name) == expected
        else:
            with pytest.raises(MQTTError, match=rf"Invalid value {value} for property {prop_id.name}") as excinfo:
                props_type.decode(data)
            assert excinfo.value.reason_code == MQTTReasonCode.ProtocolError


@pytest.mark.parametrize("props_type", [MQTTPublishProps, MQTTWillProps])
@pytest.mark.parametrize(("topic", "valid"), [
    ("response/topic", True),
    ("response/+", False),
    ("response/#", False),
    ("+", False),
    ("a+b", False),
])
def test_property_response_topic(props_type: type[MQTTProperties], topic: str, valid: bool) -> None:
    prop = encode_varint(MQTTPropertyId.ResponseTopic) + encode_string(topic)
    data = memoryview(encode_varint(len(prop)) + prop)
    if valid:
        decoded, _ = props_type.decode(data)
        assert decoded.ResponseTopic == topic
    else:
        with pytest.raises(MQTTError, match=r"ResponseTopic contains wildcards") as excinfo:
            props_type.decode(data)
        assert excinfo.value.reason_code == MQTTReasonCode.ProtocolError


def test_property_value_past_properties() -> None:
    data = encode_varint(2) + encode_varint(MQTTPropertyId.TopicAlias) + encode_uint16(1) + b"payload"

    with pytest.raises(MQTTError) as excinfo:
        MQTTPublishProps.decode(memoryview(data))
    assert excinfo.value.reason_code == MQTTReasonCode.MalformedPacket


def test_property_followed_by_data() -> None:
    props = encode_varint(MQTTPropertyId.TopicAlias) + encode_uint16(7)
    data = encode_varint(len(props)) + props + b"payload"

    decoded, size = MQTTPublishProps.decode(memoryview(data))
    assert decoded.TopicAlias == 7
    assert size == len(props) + 1
