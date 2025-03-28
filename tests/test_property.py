import pytest

from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTPacketType
from ohmqtt.property import MQTTPropertyDict, validate_properties


def test_validate():
    props: MQTTPropertyDict = {
        "ContentType": b"application/json",
    }
    validate_properties(props)
    validate_properties(props, packet_type=MQTTPacketType.PUBLISH)
    validate_properties(props, is_will=True)

    props = {
        "MaximumPacketSize": 65535,
    }
    validate_properties(props)
    with pytest.raises(MQTTError):
        validate_properties(props, packet_type=MQTTPacketType.PUBLISH)
    with pytest.raises(MQTTError):
        validate_properties(props, is_will=True)
