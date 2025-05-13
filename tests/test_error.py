from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode


def test_mqtt_error() -> None:
    e = MQTTError("test", MQTTReasonCode.ProtocolError)
    assert str(e) == "test (reason code: ProtocolError{0x82})"
    assert e.reason_code == MQTTReasonCode.ProtocolError
    assert isinstance(e, Exception)
