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


# Allowed MQTT property types for each packet type.
MQTTPropertyPacketTypes: Mapping[MQTTPacketType, frozenset[MQTTPropertyId]] = {
    MQTTPacketType.CONNECT: frozenset({  # [MQ5 3.1.2.11]
        MQTTPropertyId.SessionExpiryInterval,
        MQTTPropertyId.ReceiveMaximum,
        MQTTPropertyId.MaximumPacketSize,
        MQTTPropertyId.TopicAliasMaximum,
        MQTTPropertyId.RequestResponseInformation,
        MQTTPropertyId.RequestProblemInformation,
        MQTTPropertyId.UserProperty,
        MQTTPropertyId.AuthenticationMethod,
        MQTTPropertyId.AuthenticationData,
    }),
    MQTTPacketType.CONNACK: frozenset({  # [MQ5 3.2.2.3]
        MQTTPropertyId.SessionExpiryInterval,
        MQTTPropertyId.ReceiveMaximum,
        MQTTPropertyId.MaximumQoS,
        MQTTPropertyId.RetainAvailable,
        MQTTPropertyId.MaximumPacketSize,
        MQTTPropertyId.AssignedClientIdentifier,
        MQTTPropertyId.TopicAliasMaximum,
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
        MQTTPropertyId.WildcardSubscriptionAvailable,
        MQTTPropertyId.SubscriptionIdentifierAvailable,
        MQTTPropertyId.SharedSubscriptionAvailable,
        MQTTPropertyId.ServerKeepAlive,
        MQTTPropertyId.ResponseInformation,
        MQTTPropertyId.ServerReference,
        MQTTPropertyId.AuthenticationMethod,
        MQTTPropertyId.AuthenticationData,
    }),
    MQTTPacketType.PUBLISH: frozenset({  # [MQ5 3.3.2.3]
        MQTTPropertyId.PayloadFormatIndicator,
        MQTTPropertyId.MessageExpiryInterval,
        MQTTPropertyId.TopicAlias,
        MQTTPropertyId.ResponseTopic,
        MQTTPropertyId.CorrelationData,
        MQTTPropertyId.UserProperty,
        MQTTPropertyId.SubscriptionIdentifier,
        MQTTPropertyId.ContentType,
    }),
    MQTTPacketType.PUBACK: frozenset({  # [MQ5 3.4.2.2]
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
    }),
    MQTTPacketType.PUBREC: frozenset({  # [MQ5 3.5.2.2]
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
    }),
    MQTTPacketType.PUBREL: frozenset({  # [MQ5 3.6.2.2]
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
    }),
    MQTTPacketType.PUBCOMP: frozenset({  # [MQ5 3.7.2.2]
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
    }),
    MQTTPacketType.SUBSCRIBE: frozenset({  # [MQ5 3.8.2.1]
        MQTTPropertyId.SubscriptionIdentifier,
        MQTTPropertyId.UserProperty,
    }),
    MQTTPacketType.SUBACK: frozenset({  # [MQ5 3.9.2.1]
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
    }),
    MQTTPacketType.UNSUBSCRIBE: frozenset({  # [MQ5 3.10.2.1]
        MQTTPropertyId.UserProperty,
    }),
    MQTTPacketType.UNSUBACK: frozenset({  # [MQ5 3.11.2.1]
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
    }),
    MQTTPacketType.DISCONNECT: frozenset({  # [MQ5 3.14.2.2]
        MQTTPropertyId.SessionExpiryInterval,
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
        MQTTPropertyId.ServerReference,
    }),
    MQTTPacketType.AUTH: frozenset({  # [MQ5 3.15.2.2]
        MQTTPropertyId.AuthenticationMethod,
        MQTTPropertyId.AuthenticationData,
        MQTTPropertyId.ReasonString,
        MQTTPropertyId.UserProperty,
    }),
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
    data = bytearray()
    for key, prop_value in properties.items():
        prop_id = MQTTPropertyId[key]
        serializer = MQTTPropertySerializers[prop_id]

        # MQTT specification calls for a variable integer for the property ID,
        #   but we know that the IDs are all 1 byte long,
        #   so we will encode them as single bytes to save a branch.

        if key in ("SubscriptionIdentifier", "UserProperty"):
            # These properties may appear multiple times, in any order.
            # Ignore typing here because mypy doesn't follow the TypedDict.
            for sub_value in prop_value:  # type: ignore
                data.append(prop_id)
                data.extend(serializer(sub_value))
        else:
            data.append(prop_id)
            # Ignore typing here because mypy doesn't follow the TypedDict.
            data.extend(serializer(prop_value))  # type: ignore

    return encode_varint(len(data)) + bytes(data)


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
        key = data[offset]
        offset += 1
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
    allowed_properties = MQTTPropertyPacketTypes[packet_type] if packet_type is not None else set()
    if is_will:
        allowed_properties = allowed_properties | MQTTPropertyAllowedInWill
    if allowed_properties and any(MQTTPropertyId[key] not in allowed_properties for key in properties):
        disallowed = ", ".join([str(MQTTPropertyId[key]) for key in properties if MQTTPropertyId[key] not in allowed_properties])
        raise MQTTError(
            f"Disallowed propert(ies) found in packet type {packet_type} (will: {is_will}): {disallowed}",
            MQTTReasonCode.ProtocolError,
        )
    # TODO: Numeric limits
    # TODO: Uniqueness


def hash_properties(properties: MQTTPropertyDict) -> int:
    """Calculate the hash of the properties."""
    def _hashable_value(value: object) -> object:
        """Hash a value."""
        if isinstance(value, set):
            return frozenset(value)
        elif isinstance(value, list):
            return tuple(value)
        else:
            return value
    return hash(frozenset((key, _hashable_value(value)) for key, value in properties.items()))