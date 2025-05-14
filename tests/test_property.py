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
