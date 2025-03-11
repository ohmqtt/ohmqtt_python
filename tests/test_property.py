import pytest

from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTPacketType
from ohmqtt.property import MQTTProperties


def test_operators():
    props1 = MQTTProperties({
        "ContentType": b"application/json",
    })
    props2 = MQTTProperties({
        "ContentType": b"application/json",
    })
    props3 = MQTTProperties({
        "ContentType": b"text/plain",
    })
    assert props1 == props1
    assert props1 == props2
    assert props1 != props3
    assert props1 != None
    assert props1 != "foo"
    for key in props1:
        assert key == "ContentType"
        props1[key] == b"application/json"
    assert props1.copy() == props1


def test_validate():
    props = MQTTProperties({
        "ContentType": b"application/json",
    })
    props.validate()
    props.validate(packet_type=MQTTPacketType.PUBLISH)
    props.validate(is_will=True)

    props = MQTTProperties({
        "MaximumPacketSize": 65535,
    })
    props.validate()
    with pytest.raises(MQTTError):
        props.validate(packet_type=MQTTPacketType.PUBLISH)
    with pytest.raises(MQTTError):
        props.validate(is_will=True)
