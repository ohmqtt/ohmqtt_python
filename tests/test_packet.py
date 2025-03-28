import binascii

import pytest

from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    decode_packet,
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTSubscribePacket,
    MQTTSubAckPacket,
    MQTTPingReqPacket,
    MQTTPingRespPacket,
    MQTTDisconnectPacket,
)
from ohmqtt.property import MQTTPropertyDict, MQTTPropertyId


def extract_props(data) -> MQTTPropertyDict:
    """Helper for extracting properties from test data."""
    props: MQTTPropertyDict = {}
    for prop in data:
        k = prop[0]
        prop_key = MQTTPropertyId[k]
        if MQTTPropertyDict.__annotations__[prop_key.name] == bytes:
            prop_value = binascii.unhexlify(prop[1])
        elif prop_key == MQTTPropertyId.SubscriptionIdentifier:
            prop_value = set(prop[1:])
        elif prop_key == MQTTPropertyId.UserProperty:
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
            args[k] = binascii.unhexlify(v)
        elif k == "properties":
            args[k] = extract_props(v)
        else:
            args[k] = v
    return args


def run_decode_error_cases(test_data):
    for case in test_data:
        try:
            decode_packet(binascii.unhexlify(case["raw"]))
        except MQTTError as e:
            assert e.reason_code == MQTTReasonCode(case["reason_code"])
        else:
            pytest.fail(f"Expected MQTT error: {hex(case['reason_code'])}")


def test_connect_encode(test_data):
    for case in test_data:
        args = extract_args(case["args"], ("password", "will_payload"))
        packet = MQTTConnectPacket(**args)
        encoded = packet.encode()
        assert encoded == binascii.unhexlify(case["raw"]), binascii.hexlify(encoded)

        decoded = decode_packet(binascii.unhexlify(case["raw"]))
        assert type(decoded) == MQTTConnectPacket
        for attr, value in args.items():
            assert getattr(decoded, attr) == value, f"{attr}: {getattr(decoded, attr)} != {value}"

        assert packet == decoded
        assert str(packet)


def test_connect_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_connack_encode(test_data):
    for case in test_data:
        args = extract_args(case["args"], ())
        args["reason_code"] = MQTTReasonCode(args["reason_code"])
        packet = MQTTConnAckPacket(**args)
        encoded = packet.encode()
        assert encoded == binascii.unhexlify(case["raw"]), binascii.hexlify(encoded)

        decoded = decode_packet(binascii.unhexlify(case["raw"]))
        assert type(decoded) == MQTTConnAckPacket
        for attr, value in args.items():
            assert getattr(decoded, attr) == value, f"{attr}: {getattr(decoded, attr)} != {value}"

        assert packet == decoded
        assert str(packet)


def test_connack_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_publish_encode(test_data):
    for case in test_data:
        args = extract_args(case["args"], ())
        args["payload"] = args["payload"].encode("utf-8")
        packet = MQTTPublishPacket(**args)
        encoded = packet.encode()
        assert encoded == binascii.unhexlify(case["raw"]), binascii.hexlify(encoded)

        decoded = decode_packet(binascii.unhexlify(case["raw"]))
        assert type(decoded) == MQTTPublishPacket
        for attr, value in args.items():
            assert getattr(decoded, attr) == value, f"{attr}: {getattr(decoded, attr)} != {value}"

        assert packet == decoded
        assert str(packet)


def test_publish_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_puback_encode(test_data):
    for case in test_data:
        args = extract_args(case["args"], ())
        if "reason_code" in args:
            args["reason_code"] = MQTTReasonCode(args["reason_code"])
        packet = MQTTPubAckPacket(**args)
        encoded = packet.encode()
        assert encoded == binascii.unhexlify(case["raw"]), binascii.hexlify(encoded)

        decoded = decode_packet(binascii.unhexlify(case["raw"]))
        assert type(decoded) == MQTTPubAckPacket
        for attr, value in args.items():
            assert getattr(decoded, attr) == value, f"{attr}: {getattr(decoded, attr)} != {value}"

        assert packet == decoded
        assert str(packet)


def test_puback_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_subscribe_encode(test_data):
    for case in test_data:
        args = extract_args(case["args"], ())
        args["topics"] = tuple([(t, int(q)) for t, q in args["topics"]])
        packet = MQTTSubscribePacket(**args)
        encoded = packet.encode()
        assert encoded == binascii.unhexlify(case["raw"]), binascii.hexlify(encoded)

        decoded = decode_packet(binascii.unhexlify(case["raw"]))
        assert type(decoded) == MQTTSubscribePacket
        for attr, value in args.items():
            assert getattr(decoded, attr) == value, f"{attr}: {getattr(decoded, attr)} != {value}"

        assert packet == decoded
        assert str(packet)


def test_subscribe_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_suback_encode(test_data):
    for case in test_data:
        args = extract_args(case["args"], ())
        args["reason_codes"] = [MQTTReasonCode(int(rc)) for rc in args["reason_codes"]]
        packet = MQTTSubAckPacket(**args)
        encoded = packet.encode()
        assert encoded == binascii.unhexlify(case["raw"]), binascii.hexlify(encoded)

        decoded = decode_packet(binascii.unhexlify(case["raw"]))
        assert type(decoded) == MQTTSubAckPacket
        for attr, value in args.items():
            assert getattr(decoded, attr) == value, f"{attr}: {getattr(decoded, attr)} != {value}"

        assert packet == decoded
        assert str(packet)
        

def test_suback_decode_errors(test_data):    
    run_decode_error_cases(test_data)


def test_pingreq_packet():
    raw = b"\xc0\x00"
    packet = MQTTPingReqPacket()
    encoded = packet.encode()
    assert encoded == raw

    decoded = decode_packet(raw)
    assert type(decoded) == MQTTPingReqPacket
    assert packet == decoded
    assert str(packet)

    with pytest.raises(MQTTError):
        decode_packet(b"\xc0")
    with pytest.raises(MQTTError):
        decode_packet(b"\xc0\x01\x00")
    with pytest.raises(MQTTError):
        decode_packet(b"\xc1\x00")


def test_pingresp_packet():
    raw = b"\xd0\x00"
    packet = MQTTPingRespPacket()
    encoded = packet.encode()
    assert encoded == raw

    decoded = decode_packet(raw)
    assert type(decoded) == MQTTPingRespPacket
    assert packet == decoded
    assert str(packet)

    with pytest.raises(MQTTError):
        decode_packet(b"\xd0")
    with pytest.raises(MQTTError):
        decode_packet(b"\xd0\x01\x00")
    with pytest.raises(MQTTError):
        decode_packet(b"\xd1\x00")


def test_disconnect_encode(test_data):
    for case in test_data:
        args = extract_args(case["args"], ())
        if "reason_code" in args:
            args["reason_code"] = MQTTReasonCode(args["reason_code"])
        packet = MQTTDisconnectPacket(**args)
        encoded = packet.encode()
        assert encoded == binascii.unhexlify(case["raw"]), binascii.hexlify(encoded)

        decoded = decode_packet(binascii.unhexlify(case["raw"]))
        assert type(decoded) == MQTTDisconnectPacket
        for attr, value in args.items():
            assert getattr(decoded, attr) == value, f"{attr}: {getattr(decoded, attr)} != {value}"

        assert packet == decoded
        assert str(packet)


def test_disconnect_decode_errors(test_data):
    run_decode_error_cases(test_data)


def test_comparison():
    packet1 = MQTTConnectPacket("foo", 1)
    packet2 = MQTTConnectPacket("foo", 1)
    assert packet1 == packet1
    assert packet1 == packet2
    assert packet1 != "foo"
    assert packet1 != 1
    assert packet1 != None
    assert packet1 != object()
    assert hash(packet1) == hash(packet2)
    assert str(packet1) == str(packet2)


def test_decode_packet_errors():
    for case in (b"\x00", b"\x20\x01"):
        try:
            decode_packet(case)
        except MQTTError as e:
            assert e.reason_code == MQTTReasonCode.MalformedPacket
        else:
            pytest.fail("Expected MQTT error")
