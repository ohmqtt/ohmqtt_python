from __future__ import annotations

from types import SimpleNamespace
from typing import Callable, dataclass_transform, Final, Mapping, Sequence, TypedDict, TypeVar

from .error import MQTTError
from .mqtt_spec import MQTTPacketType, MQTTPropertyIdStrings, MQTTPropertyIdReverse, MQTTReasonCode
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
    """Internal representation of MQTT properties."""
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


MQTTPropertiesBaseT = TypeVar("MQTTPropertiesBaseT", bound="MQTTPropertiesBase")
@dataclass_transform()
class MQTTPropertiesBase(SimpleNamespace):
    """Represents MQTT packet properties."""
    def to_dict(self) -> MQTTPropertyDict:
        """Convert the properties to a dictionary."""
        # Type ignore required at this conversion point.
        return self.__dict__  # type: ignore

    @classmethod
    def from_dict(cls: type[MQTTPropertiesBaseT], properties: MQTTPropertyDict) -> MQTTPropertiesBaseT:
        """Create a properties object from a dictionary."""
        return cls(**properties)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(f'{key}={value}' for key, value in self.__dict__.items())})"
    
    def __str__(self) -> str:
        return self.__repr__()


# Possible property serializer function signatures.
_SerializerTypes = (
    Callable[[bool], bytes] |
    Callable[[bytes], bytes] |
    Callable[[int], bytes] |
    Callable[[str], bytes] |
    Callable[[tuple[str, str]], bytes]
)
# MQTT property serializers.
_MQTTPropertySerializers: Final[Mapping[str, _SerializerTypes]] = {
    "PayloadFormatIndicator": encode_uint8,
    "MessageExpiryInterval": encode_uint32,
    "ContentType": encode_string,
    "ResponseTopic": encode_string,
    "CorrelationData": encode_binary,
    "SubscriptionIdentifier": encode_varint,
    "SessionExpiryInterval": encode_uint32,
    "AssignedClientIdentifier": encode_string,
    "ServerKeepAlive": encode_uint16,
    "AuthenticationMethod": encode_string,
    "AuthenticationData": encode_binary,
    "RequestProblemInformation": encode_bool,
    "WillDelayInterval": encode_uint32,
    "RequestResponseInformation": encode_bool,
    "ResponseInformation": encode_string,
    "ServerReference": encode_string,
    "ReasonString": encode_string,
    "ReceiveMaximum": encode_uint16,
    "TopicAliasMaximum": encode_uint16,
    "TopicAlias": encode_uint16,
    "MaximumQoS": encode_uint8,
    "RetainAvailable": encode_bool,
    "UserProperty": encode_string_pair,
    "MaximumPacketSize": encode_uint32,
    "WildcardSubscriptionAvailable": encode_bool,
    "SubscriptionIdentifierAvailable": encode_bool,
    "SharedSubscriptionAvailable": encode_bool,
}


# Possible property deserializer function signatures.
_DeserializerTypes = (
    Callable[[memoryview], tuple[bool, int]] |
    Callable[[memoryview], tuple[bytes, int]] |
    Callable[[memoryview], tuple[int, int]] |
    Callable[[memoryview], tuple[str, int]] |
    Callable[[memoryview], tuple[tuple[str, str], int]]
)
# MQTT property deserializers.
_MQTTPropertyDeserializers: Final[Mapping[str, _DeserializerTypes]] = {
    "PayloadFormatIndicator": decode_uint8,
    "MessageExpiryInterval": decode_uint32,
    "ContentType": decode_string,
    "ResponseTopic": decode_string,
    "CorrelationData": decode_binary,
    "SubscriptionIdentifier": decode_varint,
    "SessionExpiryInterval": decode_uint32,
    "AssignedClientIdentifier": decode_string,
    "ServerKeepAlive": decode_uint16,
    "AuthenticationMethod": decode_string,
    "AuthenticationData": decode_binary,
    "RequestProblemInformation": decode_bool,
    "WillDelayInterval": decode_uint32,
    "RequestResponseInformation": decode_bool,
    "ResponseInformation": decode_string,
    "ServerReference": decode_string,
    "ReasonString": decode_string,
    "ReceiveMaximum": decode_uint16,
    "TopicAliasMaximum": decode_uint16,
    "TopicAlias": decode_uint16,
    "MaximumQoS": decode_uint8,
    "RetainAvailable": decode_bool,
    "UserProperty": decode_string_pair,
    "MaximumPacketSize": decode_uint32,
    "WildcardSubscriptionAvailable": decode_bool,
    "SubscriptionIdentifierAvailable": decode_bool,
    "SharedSubscriptionAvailable": decode_bool,
}


# Allowed MQTT property types for each packet type.
_MQTTPropertyPacketTypes: Final[Mapping[int, frozenset[str]]] = {
    MQTTPacketType.CONNECT: frozenset({  # [MQ5 3.1.2.11]
        "SessionExpiryInterval",
        "ReceiveMaximum",
        "MaximumPacketSize",
        "TopicAliasMaximum",
        "RequestResponseInformation",
        "RequestProblemInformation",
        "UserProperty",
        "AuthenticationMethod",
        "AuthenticationData",
    }),
    MQTTPacketType.CONNACK: frozenset({  # [MQ5 3.2.2.3]
        "SessionExpiryInterval",
        "ReceiveMaximum",
        "MaximumQoS",
        "RetainAvailable",
        "MaximumPacketSize",
        "AssignedClientIdentifier",
        "TopicAliasMaximum",
        "ReasonString",
        "UserProperty",
        "WildcardSubscriptionAvailable",
        "SubscriptionIdentifierAvailable",
        "SharedSubscriptionAvailable",
        "ServerKeepAlive",
        "ResponseInformation",
        "ServerReference",
        "AuthenticationMethod",
        "AuthenticationData",
    }),
    MQTTPacketType.PUBLISH: frozenset({  # [MQ5 3.3.2.3]
        "PayloadFormatIndicator",
        "MessageExpiryInterval",
        "TopicAlias",
        "ResponseTopic",
        "CorrelationData",
        "UserProperty",
        "SubscriptionIdentifier",
        "ContentType",
    }),
    MQTTPacketType.PUBACK: frozenset({  # [MQ5 3.4.2.2]
        "ReasonString",
        "UserProperty",
    }),
    MQTTPacketType.PUBREC: frozenset({  # [MQ5 3.5.2.2]
        "ReasonString",
        "UserProperty",
    }),
    MQTTPacketType.PUBREL: frozenset({  # [MQ5 3.6.2.2]
        "ReasonString",
        "UserProperty",
    }),
    MQTTPacketType.PUBCOMP: frozenset({  # [MQ5 3.7.2.2]
        "ReasonString",
        "UserProperty",
    }),
    MQTTPacketType.SUBSCRIBE: frozenset({  # [MQ5 3.8.2.1]
        "SubscriptionIdentifier",
        "UserProperty",
    }),
    MQTTPacketType.SUBACK: frozenset({  # [MQ5 3.9.2.1]
        "ReasonString",
        "UserProperty",
    }),
    MQTTPacketType.UNSUBSCRIBE: frozenset({  # [MQ5 3.10.2.1]
        "UserProperty",
    }),
    MQTTPacketType.UNSUBACK: frozenset({  # [MQ5 3.11.2.1]
        "ReasonString",
        "UserProperty",
    }),
    MQTTPacketType.DISCONNECT: frozenset({  # [MQ5 3.14.2.2]
        "SessionExpiryInterval",
        "ReasonString",
        "UserProperty",
        "ServerReference",
    }),
    MQTTPacketType.AUTH: frozenset({  # [MQ5 3.15.2.2]
        "AuthenticationMethod",
        "AuthenticationData",
        "ReasonString",
        "UserProperty",
    }),
}


# Allowed MQTT properties in a Will message.
_MQTTPropertyAllowedInWill: Final[frozenset[str]] = frozenset({
    "PayloadFormatIndicator",
    "MessageExpiryInterval",
    "ContentType",
    "ResponseTopic",
    "CorrelationData",
    "WillDelayInterval",
    "UserProperty",
})


def encode_properties(properties: MQTTPropertyDict) -> bytes:
    """Encode MQTT properties to a buffer."""
    if not properties:
        # Fast path for empty properties.
        return b"\x00"
    data = bytearray()
    for key, prop_value in properties.items():
        prop_id = MQTTPropertyIdStrings[key]
        serializer = _MQTTPropertySerializers[key]

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
    data[0:0] = encode_varint(len(data))
    return bytes(data)


def decode_properties(data: memoryview) -> tuple[MQTTPropertyDict, int]:
    """Decode MQTT properties from a buffer.
    
    Returns a tuple of the decoded properties and the number of bytes consumed."""
    length, length_sz = decode_varint(data)
    if length == 0:
        # Fast path for empty properties.
        return {}, 1
    data = data[length_sz:]
    remaining = length
    properties: MQTTPropertyDict = {}
    while remaining:
        # The spec calls for a variable integer for the property ID,
        #   but we know that the IDs are all 1 byte long,
        #   so we will decode them as uint8 to save a branch.
        key = data[0]
        data  = data[1:]
        remaining -= 1
        prop_name = MQTTPropertyIdReverse[key]
        deserializer: _DeserializerTypes = _MQTTPropertyDeserializers[prop_name]
        value, sz = deserializer(data)
        data = data[sz:]
        remaining -= sz
        # Ignore typing below because mypy doesn't support non-literal TypedDict keys.
        if prop_name == "SubscriptionIdentifier":
            # This property may appear multiple times, in any order.
            if prop_name not in properties:
                properties[prop_name] = set() # type: ignore
            properties[prop_name].add(value) # type: ignore
        elif prop_name == "UserProperty":
            # This property may appear multiple times, in any order.
            if prop_name not in properties:
                properties[prop_name] = [] # type: ignore
            properties[prop_name].append(value) # type: ignore
        else:
            # Other properties must appear exactly once.
            properties[prop_name] = value # type: ignore
    return properties, length + length_sz


def validate_properties(properties: MQTTPropertyDict, packet_type: int | None = None, is_will: bool = False) -> None:
    """Validate the properties against a packet type or as a will message."""
    allowed_properties = _MQTTPropertyPacketTypes[packet_type] if packet_type is not None else set()
    if is_will:
        allowed_properties = allowed_properties | _MQTTPropertyAllowedInWill
    #if allowed_properties and any(key not in allowed_properties for key in properties):
    if not allowed_properties and packet_type is None:
        allowed_properties = frozenset(MQTTPropertyIdStrings.keys())
    if not frozenset(properties.keys()).issubset(allowed_properties):
        disallowed = ", ".join([key for key in properties if key not in allowed_properties])
        raise MQTTError(
            f"Disallowed properties found in packet type {packet_type} (will: {is_will}): {disallowed}",
            MQTTReasonCode.ProtocolError,
        )
    # TODO: Numeric limits
    # TODO: Uniqueness
