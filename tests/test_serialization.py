import pytest

from ohmqtt.error import MQTTError
from ohmqtt.serialization import (
    encode_bool,
    decode_bool,
    encode_uint8,
    decode_uint8,
    encode_uint16,
    decode_uint16,
    encode_uint32,
    decode_uint32,
    encode_string,
    decode_string,
    encode_string_pair,
    decode_string_pair,
    encode_binary,
    decode_binary,
    encode_varint,
    decode_varint,
)


def test_encode_bool(test_data):
    for case in test_data:
        assert encode_bool(case["input"]) == bytes.fromhex(case["output"])
        decoded, sz = decode_bool(bytes.fromhex(case["output"]))
        assert sz == 1

        assert decoded == case["input"]


def test_decode_bool_errors(test_data):
    for case in test_data:
        with pytest.raises(MQTTError):
            decode_bool(bytes.fromhex(case["input"]))


def test_encode_uint8(test_data):
    for case in test_data:
        assert encode_uint8(case["input"]) == bytes.fromhex(case["output"])
        decoded, sz = decode_uint8(bytes.fromhex(case["output"]))
        assert sz == 1

        assert decoded == case["input"]


def test_decode_uint8_errors(test_data):
    for case in test_data:
        with pytest.raises(MQTTError):
            decode_uint8(bytes.fromhex(case["input"]))


def test_encode_uint16(test_data):
    for case in test_data:
        assert encode_uint16(case["input"]) == bytes.fromhex(case["output"])
        decoded, sz = decode_uint16(bytes.fromhex(case["output"]))
        assert sz == 2

        assert decoded == case["input"]


def test_decode_uint16_errors(test_data):
    for case in test_data:
        with pytest.raises(MQTTError):
            decode_uint16(bytes.fromhex(case["input"]))


def test_encode_uint32(test_data):
    for case in test_data:
        assert encode_uint32(case["input"]) == bytes.fromhex(case["output"])
        decoded, sz = decode_uint32(bytes.fromhex(case["output"]))
        assert sz == 4

        assert decoded == case["input"]


def test_decode_uint32_errors(test_data):
    for case in test_data:
        with pytest.raises(MQTTError):
            decode_uint32(bytes.fromhex(case["input"]))


def test_encode_string(test_data):
    for case in test_data:
        encoded = encode_string(case["input"])
        assert encoded == bytes.fromhex(case["output"]), encoded.hex()
        decoded, sz = decode_string(encoded)
        assert sz == len(encoded)
        assert decoded == case["input"]


def test_decode_string(test_data):
    for case in test_data:
        decoded, sz = decode_string(bytes.fromhex(case["input"]))
        assert sz <= len(bytes.fromhex(case["input"]))
        assert decoded == case["output"], case["input"]


def test_decode_string_errors(test_data):
    for case in test_data:
        with pytest.raises(MQTTError):
            decode_string(bytes.fromhex(case["input"]))


def test_encode_string_pair(test_data):
    for case in test_data:
        pair = tuple(case["input"])
        encoded = encode_string_pair(pair)
        assert encoded == bytes.fromhex(case["output"]), encoded.hex()
        decoded, sz = decode_string_pair(encoded)
        assert sz == len(encoded)
        assert decoded == pair


def test_decode_string_pair_errors(test_data):
    for case in test_data:
        with pytest.raises(MQTTError):
            decode_string_pair(bytes.fromhex(case["input"]))


def test_encode_binary(test_data):
    for case in test_data:
        input_data = bytes.fromhex(case["input"])
        encoded = encode_binary(input_data)
        assert encoded == bytes.fromhex(case["output"]), encoded.hex()
        decoded, sz = decode_binary(encoded)
        assert sz == len(encoded)
        assert decoded == input_data


def test_decode_binary_errors(test_data):
    for case in test_data:
        with pytest.raises(MQTTError):
            decode_binary(bytes.fromhex(case["input"]))


def test_encode_varint(test_data):
    for case in test_data:
        assert encode_varint(case["input"]) == bytes.fromhex(case["output"])


def test_encode_varint_limits(test_data):
    for case in test_data:
        with pytest.raises(ValueError):
            encode_varint(case["input"])


def test_decode_varint(test_data):
    for case in test_data:
        decoded, sz = decode_varint(bytes.fromhex(case["input"]))
        assert sz <= len(bytes.fromhex(case["input"]))
        assert decoded == case["output"]


def test_decode_varint_limits(test_data):
    for case in test_data:
        with pytest.raises(MQTTError):
            decode_varint(bytes.fromhex(case["input"]))
