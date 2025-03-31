from typing import Callable, Mapping, Sequence, TypedDict

from .error import MQTTError
from .mqtt_spec import MQTTPacketType, MQTTPropertyId, MQTTReasonCode
from .serialization import (
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


class MQTTPropertyDict(TypedDict, total=False):
    PayloadFormatIndicator: int
    MessageExpiryInterval: int
    ContentType: str
    ResponseTopic: str
    CorrelationData: bytes
    SubscriptionIdentifier: set[int]
    SessionExpiryInterval: int
    AssignedClientIdentifier: str
    ServerKeepAlive: int
    AuthenticationMethod: str
    AuthenticationData: bytes
    RequestProblemInformation: bool
    WillDelayInterval: int
    RequestResponseInformation: bool
    ResponseInformation: str
    ServerReference: str
    ReasonString: str
    ReceiveMaximum: int
    TopicAliasMaximum: int
    TopicAlias: int
    MaximumQoS: int
    RetainAvailable: bool
    UserProperty: Sequence[tuple[str, str]]
    MaximumPacketSize: int
    WildcardSubscriptionAvailable: bool
    SubscriptionIdentifierAvailable: bool
    SharedSubscriptionAvailable: bool


# Possible property serializer function signatures.
_SerializerTypes = (
    Callable[[bool], bytes] |
    Callable[[bytes], bytes] |
    Callable[[int], bytes] |
    Callable[[str], bytes] |
    Callable[[tuple[str, str]], bytes]
)
# MQTT property serializers.
MQTTPropertySerializers: Mapping[MQTTPropertyId, _SerializerTypes] = {
    MQTTPropertyId.PayloadFormatIndicator: encode_uint8,
    MQTTPropertyId.MessageExpiryInterval: encode_uint32,
    MQTTPropertyId.ContentType: encode_string,
    MQTTPropertyId.ResponseTopic: encode_string,
    MQTTPropertyId.CorrelationData: encode_binary,
    MQTTPropertyId.SubscriptionIdentifier: encode_varint,
    MQTTPropertyId.SessionExpiryInterval: encode_uint32,
    MQTTPropertyId.AssignedClientIdentifier: encode_string,
    MQTTPropertyId.ServerKeepAlive: encode_uint16,
    MQTTPropertyId.AuthenticationMethod: encode_string,
    MQTTPropertyId.AuthenticationData: encode_binary,
    MQTTPropertyId.RequestProblemInformation: encode_bool,
    MQTTPropertyId.WillDelayInterval: encode_uint32,
    MQTTPropertyId.RequestResponseInformation: encode_bool,
    MQTTPropertyId.ResponseInformation: encode_string,
    MQTTPropertyId.ServerReference: encode_string,
    MQTTPropertyId.ReasonString: encode_string,
    MQTTPropertyId.ReceiveMaximum: encode_uint16,
    MQTTPropertyId.TopicAliasMaximum: encode_uint16,
    MQTTPropertyId.TopicAlias: encode_uint16,
    MQTTPropertyId.MaximumQoS: encode_uint8,
    MQTTPropertyId.RetainAvailable: encode_bool,
    MQTTPropertyId.UserProperty: encode_string_pair,
    MQTTPropertyId.MaximumPacketSize: encode_uint32,
    MQTTPropertyId.WildcardSubscriptionAvailable: encode_bool,
    MQTTPropertyId.SubscriptionIdentifierAvailable: encode_bool,
    MQTTPropertyId.SharedSubscriptionAvailable: encode_bool,
}


# Possible property deserializer function signatures.
_DeserializerTypes = (
    Callable[[bytes], tuple[bool, int]] |
    Callable[[bytes], tuple[bytes, int]] |
    Callable[[bytes], tuple[int, int]] |
    Callable[[bytes], tuple[str, int]] |
    Callable[[bytes], tuple[tuple[str, str], int]]
)
# MQTT property deserializers.
MQTTPropertyDeserializers: Mapping[MQTTPropertyId, _DeserializerTypes] = {
    MQTTPropertyId.PayloadFormatIndicator: decode_uint8,
    MQTTPropertyId.MessageExpiryInterval: decode_uint32,
    MQTTPropertyId.ContentType: decode_string,
    MQTTPropertyId.ResponseTopic: decode_string,
    MQTTPropertyId.CorrelationData: decode_binary,
    MQTTPropertyId.SubscriptionIdentifier: decode_varint,
    MQTTPropertyId.SessionExpiryInterval: decode_uint32,
    MQTTPropertyId.AssignedClientIdentifier: decode_string,
    MQTTPropertyId.ServerKeepAlive: decode_uint16,
    MQTTPropertyId.AuthenticationMethod: decode_string,
    MQTTPropertyId.AuthenticationData: decode_binary,
    MQTTPropertyId.RequestProblemInformation: decode_bool,
    MQTTPropertyId.WillDelayInterval: decode_uint32,
    MQTTPropertyId.RequestResponseInformation: decode_bool,
    MQTTPropertyId.ResponseInformation: decode_string,
    MQTTPropertyId.ServerReference: decode_string,
    MQTTPropertyId.ReasonString: decode_string,
    MQTTPropertyId.ReceiveMaximum: decode_uint16,
    MQTTPropertyId.TopicAliasMaximum: decode_uint16,
    MQTTPropertyId.TopicAlias: decode_uint16,
    MQTTPropertyId.MaximumQoS: decode_uint8,
    MQTTPropertyId.RetainAvailable: decode_bool,
    MQTTPropertyId.UserProperty: decode_string_pair,
    MQTTPropertyId.MaximumPacketSize: decode_uint32,
    MQTTPropertyId.WildcardSubscriptionAvailable: decode_bool,
    MQTTPropertyId.SubscriptionIdentifierAvailable: decode_bool,
    MQTTPropertyId.SharedSubscriptionAvailable: decode_bool,
}


# Allowed MQTT packet types for each property type.
MQTTPropertyPacketTypes: Mapping[MQTTPropertyId, frozenset[MQTTPacketType]] = {
    MQTTPropertyId.PayloadFormatIndicator: frozenset({MQTTPacketType.PUBLISH}),
    MQTTPropertyId.MessageExpiryInterval: frozenset({MQTTPacketType.PUBLISH}),
    MQTTPropertyId.ContentType: frozenset({MQTTPacketType.PUBLISH}),
    MQTTPropertyId.ResponseTopic: frozenset({MQTTPacketType.PUBLISH}),
    MQTTPropertyId.CorrelationData: frozenset({MQTTPacketType.PUBLISH}),
    MQTTPropertyId.SubscriptionIdentifier: frozenset({MQTTPacketType.PUBLISH, MQTTPacketType.SUBSCRIBE}),
    MQTTPropertyId.SessionExpiryInterval: frozenset({MQTTPacketType.CONNECT, MQTTPacketType.CONNACK, MQTTPacketType.DISCONNECT}),
    MQTTPropertyId.AssignedClientIdentifier: frozenset({MQTTPacketType.CONNACK}),
    MQTTPropertyId.ServerKeepAlive: frozenset({MQTTPacketType.CONNACK}),
    MQTTPropertyId.AuthenticationMethod: frozenset({MQTTPacketType.CONNECT, MQTTPacketType.CONNACK, MQTTPacketType.AUTH}),
    MQTTPropertyId.AuthenticationData: frozenset({MQTTPacketType.CONNECT, MQTTPacketType.CONNACK, MQTTPacketType.AUTH}),
    MQTTPropertyId.RequestProblemInformation: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.WillDelayInterval: frozenset(),
    MQTTPropertyId.RequestResponseInformation: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.ResponseInformation: frozenset({MQTTPacketType.CONNACK}),
    MQTTPropertyId.ServerReference: frozenset({MQTTPacketType.CONNACK}),
    MQTTPropertyId.ReasonString: frozenset({MQTTPacketType.CONNACK, MQTTPacketType.PUBACK, MQTTPacketType.SUBACK}),
    MQTTPropertyId.ReceiveMaximum: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.TopicAliasMaximum: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.TopicAlias: frozenset({MQTTPacketType.PUBLISH}),
    MQTTPropertyId.MaximumQoS: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.RetainAvailable: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.UserProperty: frozenset({
        MQTTPacketType.CONNECT,
        MQTTPacketType.CONNACK,
        MQTTPacketType.PUBLISH,
        MQTTPacketType.PUBACK,
        MQTTPacketType.SUBSCRIBE,
        MQTTPacketType.SUBACK,
        MQTTPacketType.UNSUBSCRIBE,
        MQTTPacketType.UNSUBACK,
    }),
    MQTTPropertyId.MaximumPacketSize: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.WildcardSubscriptionAvailable: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.SubscriptionIdentifierAvailable: frozenset({MQTTPacketType.CONNECT}),
    MQTTPropertyId.SharedSubscriptionAvailable: frozenset({MQTTPacketType.CONNECT}),
}


# Allowed MQTT properties in a Will message.
MQTTPropertyAllowedInWill: frozenset[MQTTPropertyId] = frozenset({
    MQTTPropertyId.PayloadFormatIndicator,
    MQTTPropertyId.MessageExpiryInterval,
    MQTTPropertyId.ContentType,
    MQTTPropertyId.ResponseTopic,
    MQTTPropertyId.CorrelationData,
    MQTTPropertyId.WillDelayInterval,
    MQTTPropertyId.UserProperty,
})


def encode_properties(properties: MQTTPropertyDict) -> bytes:
    """Encode MQTT properties to a buffer."""
    if not properties:
        # Fast path for empty properties.
        return b"\x00"
    data = b""
    for key, prop_value in properties.items():
        prop_id = MQTTPropertyId[key]
        serializer = MQTTPropertySerializers[prop_id]

        # MQTT specification calls for a variable integer for the property ID,
        #   but we know that the IDs are all 1 byte long,
        #   so we will encode them as uint8 to save a branch.
        encoded_prop_id = encode_uint8(prop_id)

        if key in ("SubscriptionIdentifier", "UserProperty"):
            # These properties may appear multiple times, in any order.
            # Ignore typing here because mypy doesn't follow the TypedDict.
            for sub_value in prop_value: # type: ignore
                data += encoded_prop_id + serializer(sub_value)
        else:
            # Ignore typing here because mypy doesn't follow the TypedDict.
            data += encoded_prop_id + serializer(prop_value) # type: ignore

    return encode_varint(len(data)) + data


def decode_properties(data: bytes) -> tuple[MQTTPropertyDict, int]:
    """Decode MQTT properties from a buffer.
    
    Returns a tuple of the decoded properties and the number of bytes consumed."""
    length, offset = decode_varint(data)
    if length == 0:
        # Fast path for empty properties.
        return {}, 1
    properties: MQTTPropertyDict = {}
    while offset < length:
        # The spec calls for a variable integer for the property ID,
        #   but we know that the IDs are all 1 byte long,
        #   so we will decode them as uint8 to save a branch.
        key, key_length = decode_uint8(data[offset:])
        offset += key_length
        prop_key = MQTTPropertyId(key)
        prop_name = prop_key.name
        deserializer: _DeserializerTypes = MQTTPropertyDeserializers[prop_key]
        value, value_length = deserializer(data[offset:])
        offset += value_length
        # Ignore typing below because mypy doesn't support non-literal TypedDict keys.
        if prop_key == MQTTPropertyId.SubscriptionIdentifier:
            # This property may appear multiple times, in any order.
            if prop_name not in properties:
                properties[prop_name] = set() # type: ignore
            properties[prop_name].add(value) # type: ignore
        elif prop_key == MQTTPropertyId.UserProperty:
            # This property may appear multiple times, in any order.
            if prop_name not in properties:
                properties[prop_name] = [] # type: ignore
            properties[prop_name].append(value) # type: ignore
        else:
            # Other properties must appear exactly once.
            properties[prop_name] = value # type: ignore
    return properties, offset


def validate_properties(properties: MQTTPropertyDict, packet_type: MQTTPacketType | None = None, is_will: bool = False) -> None:
    """Validate the properties against a packet type or as a will message."""
    if not properties:
        # Fast path for empty properties.
        return
    for key, value in properties.items():
        prop_id = MQTTPropertyId[key]
        if packet_type is not None and packet_type not in MQTTPropertyPacketTypes[prop_id]:
            raise MQTTError(f"Property {prop_id} not allowed in packet type {packet_type}", MQTTReasonCode.ProtocolError)
        if is_will and MQTTPropertyId[key] not in MQTTPropertyAllowedInWill:
            raise MQTTError(f"Property {prop_id} not allowed in Will message", MQTTReasonCode.ProtocolError)
    # TODO: Numeric limits
    # TODO: Uniqueness


def hash_properties(properties: MQTTPropertyDict) -> int:
    """Calculate the hash of the properties."""
    def _hashable_value(value):
        """Hash a value."""
        if isinstance(value, set):
            return frozenset(value)
        elif isinstance(value, list):
            return tuple(value)
        else:
            return value
    return hash(frozenset((key, _hashable_value(value)) for key, value in properties.items()))