import pytest

from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode, MQTTPropertyId
from ohmqtt.packet import (
    decode_packet,
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
    MQTTSubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubscribePacket,
    MQTTUnsubAckPacket,
    MQTTPingReqPacket,
    MQTTPingRespPacket,
    MQTTDisconnectPacket,
    MQTTAuthPacket,
)
from ohmqtt.property import MQTTPropertyDict


def extract_props(data) -> MQTTPropertyDict:
    """Helper for extracting properties from test data."""
    props: MQTTPropertyDict = {}
    for prop in data:
        k = prop[0]
        prop_key = MQTTPropertyId[k]
        if MQTTPropertyDict.__annotations__[k] is bytes:
            prop_value = bytes.fromhex(prop[1])
        elif prop_key == MQTTPropertyId["SubscriptionIdentifier"]:
            prop_value = set(prop[1:])
        elif prop_key == MQTTPropertyId["UserProperty"]:
            prop_value = [tuple([pk, pv]) for pk, pv in prop[1:]]
        else:
            prop_value = prop[1]
        props[k] = prop_value
    return props


def extract_args(data, binary_args):
    """Helper for extracting arguments from test data."""
    args = {}
    for k, v in data.items():
        if k in binary_args:
            args[k] = bytes.fromhex(v)
        elif k == "properties":
            args[k] = extract_props(v)
        else:
            args[k] = v
    return args


def run_encode_cases(cls, test_data, binary_args=tuple(), transform_args=None):
    for case in test_data:
        args = extract_args(case["args"], binary_args)
        if transform_args is not None:
            transform_args(args)
        packet = cls(**args)
        encoded = packet.encode()
        assert encoded == bytes.fromhex(case["raw"]), encoded.hex()

        decoded = decode_packet(bytes.fromhex(case["raw"]))
        assert type(decoded) is cls
        for attr, value in args.items():
            assert getattr(decoded, attr) == value, f"{attr}: {getattr(decoded, attr)} != {value}"

        assert packet == packet
        assert packet == decoded
        assert packet != b"not a packet"
        assert str(packet) == str(decoded)
        assert not hasattr(packet, "__dict__")
        assert all(hasattr(packet, attr) for attr in packet.__slots__)
        with pytest.raises(TypeError):
            hash(packet)


def run_decode_error_cases(test_data):
    for case in test_data:
        try:
            decode_packet(bytes.fromhex(case["raw"]))
        except MQTTError as e:
            assert e.reason_code == case["reason_code"]
        else:
            pytest.fail(f"Expected MQTT error: {hex(case['reason_code'])}")


def test_packet_connect_encode(test_data):
    run_encode_cases(MQTTConnectPacket, test_data, ("password", "will_payload"))


def test_packet_connect_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_connack_encode(test_data):
    run_encode_cases(MQTTConnAckPacket, test_data)


def test_packet_connack_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_publish_encode(test_data):
    def transform_args(args):
        args["payload"] = args["payload"].encode("utf-8")
    run_encode_cases(MQTTPublishPacket, test_data, transform_args=transform_args)


def test_packet_publish_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_puback_encode(test_data):
    run_encode_cases(MQTTPubAckPacket, test_data)


def test_packet_puback_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_pubrec_encode(test_data):
    run_encode_cases(MQTTPubRecPacket, test_data)


def test_packet_pubrec_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_pubrel_encode(test_data):
    run_encode_cases(MQTTPubRelPacket, test_data)


def test_packet_pubrel_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_pubcomp_encode(test_data):
    run_encode_cases(MQTTPubCompPacket, test_data)


def test_packet_pubcomp_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_subscribe_encode(test_data):
    def transform_args(args):
        args["topics"] = [(t, int(q)) for t, q in args["topics"]]
    run_encode_cases(MQTTSubscribePacket, test_data, transform_args=transform_args)


def test_packet_subscribe_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_suback_encode(test_data):
    run_encode_cases(MQTTSubAckPacket, test_data)
        

def test_packet_suback_decode_errors(test_data):    
    run_decode_error_cases(test_data)


def test_packet_unsubscribe_encode(test_data):
    run_encode_cases(MQTTUnsubscribePacket, test_data)


def test_packet_unsubscribe_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_unsuback_encode(test_data):
    run_encode_cases(MQTTUnsubAckPacket, test_data)


def test_packet_unsuback_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_pingreq_packet():
    raw = b"\xc0\x00"
    packet = MQTTPingReqPacket()
    encoded = packet.encode()
    assert encoded == raw

    decoded = decode_packet(raw)
    assert type(decoded) is MQTTPingReqPacket
    assert packet == decoded
    assert str(packet) == str(decoded)
    assert not hasattr(packet, "__dict__")
    with pytest.raises(TypeError):
        hash(packet)

    with pytest.raises(MQTTError):
        decode_packet(b"\xc0")
    with pytest.raises(MQTTError):
        decode_packet(b"\xc0\x01\x00")
    with pytest.raises(MQTTError):
        decode_packet(b"\xc1\x00")


def test_packet_pingresp_packet():
    raw = b"\xd0\x00"
    packet = MQTTPingRespPacket()
    encoded = packet.encode()
    assert encoded == raw

    decoded = decode_packet(raw)
    assert type(decoded) is MQTTPingRespPacket
    assert packet == decoded
    assert str(packet) == str(decoded)
    assert not hasattr(packet, "__dict__")
    with pytest.raises(TypeError):
        hash(packet)

    with pytest.raises(MQTTError):
        decode_packet(b"\xd0")
    with pytest.raises(MQTTError):
        decode_packet(b"\xd0\x01\x00")
    with pytest.raises(MQTTError):
        decode_packet(b"\xd1\x00")


def test_packet_disconnect_encode(test_data):
    run_encode_cases(MQTTDisconnectPacket, test_data)


def test_packet_disconnect_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_auth_encode(test_data):
    run_encode_cases(MQTTAuthPacket, test_data)


def test_packet_auth_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_packet_comparison():
    packet1 = MQTTConnectPacket("foo", 1)
    packet2 = MQTTConnectPacket("foo", 1)
    packet3 = MQTTConnectPacket("foo", 2)
    assert packet1 == packet2
    assert packet1 != packet3
    assert packet1 != "foo"
    assert packet1 != 1
    assert packet1 is not None
    assert packet1 != object()


def test_packet_decode_packet_errors():
    for case in (b"\x00", b"\x20\x01"):
        try:
            decode_packet(case)
        except MQTTError as e:
            assert e.reason_code == MQTTReasonCode["MalformedPacket"]
        else:
            pytest.fail("Expected MQTT error")
