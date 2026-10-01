import pytest

from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTPropertyId, MQTTReasonCode
from ohmqtt.property import MQTTPublishProps
from ohmqtt.serialization import encode_varint, encode_uint16


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
