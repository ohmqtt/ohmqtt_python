from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Callable, Final, Mapping, NamedTuple, Sequence, TypeAlias, TypedDict

if sys.version_info >= (3, 11):
    from typing import dataclass_transform, Self
else:
    from typing_extensions import dataclass_transform, Self

from .mqtt_spec import MQTTPropertyId, MQTTPropertyName, MQTTPropertyIdReverse
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


@dataclass_transform()
class MQTTPropertiesBase(SimpleNamespace):
    """Represents MQTT packet properties."""
    def __len__(self) -> int:
        return len(self.__dict__)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(f'{key}={value}' for key, value in self.__dict__.items())})"
    
    def __str__(self) -> str:
        return self.__repr__()


IntFieldT: TypeAlias = int | None
StrFieldT: TypeAlias = str | None
BytesFieldT: TypeAlias = bytes | None
BoolFieldT: TypeAlias = bool | None

PayloadFormatIndicatorT: TypeAlias = IntFieldT
MessageExpiryIntervalT: TypeAlias = IntFieldT
ContentTypeT: TypeAlias = StrFieldT
ResponseTopicT: TypeAlias = StrFieldT
CorrelationDataT: TypeAlias = BytesFieldT
SubscriptionIdentifierT: TypeAlias = set[int] | None
TopicAliasT: TypeAlias = IntFieldT
SessionExpiryIntervalT: TypeAlias = IntFieldT
AssignedClientIdentifierT: TypeAlias = StrFieldT
ServerKeepAliveT: TypeAlias = IntFieldT
AuthenticationMethodT: TypeAlias = StrFieldT
AuthenticationDataT: TypeAlias = BytesFieldT
RequestProblemInformationT: TypeAlias = BoolFieldT
WillDelayIntervalT: TypeAlias = IntFieldT
RequestResponseInformationT: TypeAlias = BoolFieldT
ResponseInformationT: TypeAlias = StrFieldT
ServerReferenceT: TypeAlias = StrFieldT
ReasonStringT: TypeAlias = StrFieldT
ReceiveMaximumT: TypeAlias = IntFieldT
TopicAliasMaximumT: TypeAlias = IntFieldT
MaximumQoST: TypeAlias = IntFieldT
RetainAvailableT: TypeAlias = BoolFieldT
UserPropertyT: TypeAlias = Sequence[tuple[str, str]] | None
MaximumPacketSizeT: TypeAlias = IntFieldT
WildcardSubscriptionAvailableT: TypeAlias = BoolFieldT
SubscriptionIdentifierAvailableT: TypeAlias = BoolFieldT
SharedSubscriptionAvailableT: TypeAlias = BoolFieldT


_SerializerTypes: TypeAlias = (
    Callable[[bool], bytes] |
    Callable[[int], bytes] |
    Callable[[str], bytes] |
    Callable[[bytes], bytes] |
    Callable[[tuple[str, str]], bytes]
)
_DeserializerTypes: TypeAlias = (
    Callable[[memoryview], tuple[bool, int]] |
    Callable[[memoryview], tuple[int, int]] |
    Callable[[memoryview], tuple[str, int]] |
    Callable[[memoryview], tuple[bytes, int]] |
    Callable[[memoryview], tuple[tuple[str, str], int]]
)
class _SerializerPair(NamedTuple):
    """Pair of serializer and deserializer functions for a property."""
    serializer: _SerializerTypes
    deserializer: _DeserializerTypes


_BoolSerializer: Final = _SerializerPair(serializer=encode_bool, deserializer=decode_bool)
_UInt8Serializer: Final = _SerializerPair(serializer=encode_uint8, deserializer=decode_uint8)
_UInt16Serializer: Final = _SerializerPair(serializer=encode_uint16, deserializer=decode_uint16)
_UInt32Serializer: Final = _SerializerPair(serializer=encode_uint32, deserializer=decode_uint32)
_VarIntSerializer: Final = _SerializerPair(serializer=encode_varint, deserializer=decode_varint)
_BinarySerializer: Final = _SerializerPair(serializer=encode_binary, deserializer=decode_binary)
_StringSerializer: Final = _SerializerPair(serializer=encode_string, deserializer=decode_string)
_StringPairSerializer: Final = _SerializerPair(serializer=encode_string_pair, deserializer=decode_string_pair)


_PropertySerializers: Final[Mapping[str, _SerializerPair]] = {
    MQTTPropertyName.PayloadFormatIndicator: _UInt8Serializer,
    MQTTPropertyName.MessageExpiryInterval: _UInt32Serializer,
    MQTTPropertyName.ContentType: _StringSerializer,
    MQTTPropertyName.ResponseTopic: _StringSerializer,
    MQTTPropertyName.CorrelationData: _BinarySerializer,
    MQTTPropertyName.SubscriptionIdentifier: _VarIntSerializer,
    MQTTPropertyName.SessionExpiryInterval: _UInt32Serializer,
    MQTTPropertyName.AssignedClientIdentifier: _StringSerializer,
    MQTTPropertyName.ServerKeepAlive: _UInt16Serializer,
    MQTTPropertyName.AuthenticationMethod: _StringSerializer,
    MQTTPropertyName.AuthenticationData: _BinarySerializer,
    MQTTPropertyName.RequestProblemInformation: _BoolSerializer,
    MQTTPropertyName.WillDelayInterval: _UInt32Serializer,
    MQTTPropertyName.RequestResponseInformation: _BoolSerializer,
    MQTTPropertyName.ResponseInformation: _StringSerializer,
    MQTTPropertyName.ServerReference: _StringSerializer,
    MQTTPropertyName.ReasonString: _StringSerializer,
    MQTTPropertyName.ReceiveMaximum: _UInt16Serializer,
    MQTTPropertyName.TopicAliasMaximum: _UInt16Serializer,
    MQTTPropertyName.TopicAlias: _UInt16Serializer,
    MQTTPropertyName.MaximumQoS: _UInt8Serializer,
    MQTTPropertyName.RetainAvailable: _BoolSerializer,
    MQTTPropertyName.UserProperty: _StringPairSerializer,
    MQTTPropertyName.MaximumPacketSize: _UInt32Serializer,
    MQTTPropertyName.WildcardSubscriptionAvailable: _BoolSerializer,
    MQTTPropertyName.SubscriptionIdentifierAvailable: _BoolSerializer,
    MQTTPropertyName.SharedSubscriptionAvailable: _BoolSerializer
}
_PropertySerializersById: Final[Mapping[int, _SerializerPair]] = {
    getattr(MQTTPropertyId, key): value for key, value in _PropertySerializers.items()
}


class MQTTProperties(MQTTPropertiesBase):
    def encode(self) -> bytes:
        """Encode MQTT properties to a buffer."""
        properties = self.__dict__
        if not properties:
            # Fast path for empty properties.
            return b"\x00"
        data = bytearray()
        for key, prop_value in properties.items():
            prop_id = getattr(MQTTPropertyId, key)
            serializer = _PropertySerializers[key].serializer

            # MQTT specification calls for a variable integer for the property ID,
            #   but we know that the IDs are all 1 byte long,
            #   so we will encode them as single bytes to save a branch.

            if key in ("SubscriptionIdentifier", "UserProperty"):
                # These properties may appear multiple times, in any order.
                for sub_value in prop_value:
                    data.append(prop_id)
                    data.extend(serializer(sub_value))
            else:
                data.append(prop_id)
                data.extend(serializer(prop_value))
        data[0:0] = encode_varint(len(data))
        return bytes(data)

    @classmethod
    def decode(cls, data: memoryview) -> tuple[Self, int]:
        """Decode MQTT properties from a buffer.

        Returns a tuple of the decoded properties and the number of bytes consumed."""
        length, length_sz = decode_varint(data)
        if length == 0:
            # Fast path for empty properties.
            return cls(), 1
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
            deserializer = _PropertySerializersById[key].deserializer
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
        return cls(**properties), length + length_sz


class MQTTConnectProps(MQTTProperties):
    """Properties for MQTT CONNECT packet."""
    SessionExpiryInterval: SessionExpiryIntervalT = None
    ReceiveMaximum: ReceiveMaximumT = None
    MaximumPacketSize: MaximumPacketSizeT = None
    TopicAliasMaximum: TopicAliasMaximumT = None
    RequestResponseInformation: RequestResponseInformationT = None
    RequestProblemInformation: RequestProblemInformationT = None
    AuthenticationMethod: AuthenticationMethodT = None
    AuthenticationData: AuthenticationDataT = None
    UserProperty: UserPropertyT = None


class MQTTConnAckProps(MQTTProperties):
    """Properties for MQTT CONNACK packet."""
    SessionExpiryInterval: SessionExpiryIntervalT = None
    ReceiveMaximum: ReceiveMaximumT = None
    MaximumQoS: MaximumQoST = None
    RetainAvailable: RetainAvailableT = None
    MaximumPacketSize: MaximumPacketSizeT = None
    AssignedClientIdentifier: AssignedClientIdentifierT = None
    TopicAliasMaximum: TopicAliasMaximumT = None
    ReasonString: ReasonStringT = None
    WildcardSubscriptionAvailable: WildcardSubscriptionAvailableT = None
    SubscriptionIdentifierAvailable: SubscriptionIdentifierAvailableT = None
    SharedSubscriptionAvailable: SharedSubscriptionAvailableT = None
    ServerKeepAlive: ServerKeepAliveT = None
    ResponseInformation: ResponseInformationT = None
    ServerReference: ServerReferenceT = None
    AuthenticationMethod: AuthenticationMethodT = None
    AuthenticationData: AuthenticationDataT = None
    UserProperty: UserPropertyT = None


class MQTTPublishProps(MQTTProperties):
    """Properties for MQTT PUBLISH packet."""
    PayloadFormatIndicator: PayloadFormatIndicatorT = None
    MessageExpiryInterval: MessageExpiryIntervalT = None
    ContentType: ContentTypeT = None
    ResponseTopic: ResponseTopicT = None
    CorrelationData: CorrelationDataT = None
    SubscriptionIdentifier: SubscriptionIdentifierT = None
    TopicAlias: TopicAliasT = None
    UserProperty: UserPropertyT = None


class MQTTPubAckProps(MQTTProperties):
    """Properties for MQTT PUBACK packet."""
    ReasonString: ReasonStringT = None
    UserProperty: UserPropertyT = None


class MQTTPubRecProps(MQTTProperties):
    """Properties for MQTT PUBREC packet."""
    ReasonString: ReasonStringT = None
    UserProperty: UserPropertyT = None


class MQTTPubRelProps(MQTTProperties):
    """Properties for MQTT PUBREL packet."""
    ReasonString: ReasonStringT = None
    UserProperty: UserPropertyT = None


class MQTTPubCompProps(MQTTProperties):
    """Properties for MQTT PUBCOMP packet."""
    ReasonString: ReasonStringT = None
    UserProperty: UserPropertyT = None


class MQTTSubscribeProps(MQTTProperties):
    """Properties for MQTT SUBSCRIBE packet."""
    SubscriptionIdentifier: SubscriptionIdentifierT = None
    UserProperty: UserPropertyT = None


class MQTTSubAckProps(MQTTProperties):
    """Properties for MQTT SUBACK packet."""
    ReasonString: ReasonStringT = None
    UserProperty: UserPropertyT = None


class MQTTUnsubscribeProps(MQTTProperties):
    """Properties for MQTT UNSUBSCRIBE packet."""
    UserProperty: UserPropertyT = None


class MQTTUnsubAckProps(MQTTProperties):
    """Properties for MQTT UNSUBACK packet."""
    ReasonString: ReasonStringT = None
    UserProperty: UserPropertyT = None


class MQTTDisconnectProps(MQTTProperties):
    """Properties for MQTT DISCONNECT packet."""
    SessionExpiryInterval: SessionExpiryIntervalT = None
    ReasonString: ReasonStringT = None
    ServerReference: ServerReferenceT = None
    UserProperty: UserPropertyT = None


class MQTTAuthProps(MQTTProperties):
    """Properties for MQTT AUTH packet."""
    ReasonString: ReasonStringT = None
    AuthenticationMethod: AuthenticationMethodT = None
    AuthenticationData: AuthenticationDataT = None
    UserProperty: UserPropertyT = None


class MQTTWillProps(MQTTProperties):
    """Properties for MQTT Will message."""
    PayloadFormatIndicator: PayloadFormatIndicatorT = None
    MessageExpiryInterval: MessageExpiryIntervalT = None
    ContentType: ContentTypeT = None
    ResponseTopic: ResponseTopicT = None
    CorrelationData: CorrelationDataT = None
    WillDelayInterval: WillDelayIntervalT = None
    UserProperty: UserPropertyT = None
