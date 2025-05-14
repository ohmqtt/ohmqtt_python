from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Callable, cast, Final, Mapping, NamedTuple, Sequence, TypeAlias

if sys.version_info >= (3, 11):
    from typing import dataclass_transform, Self
else:
    from typing_extensions import dataclass_transform, Self

from .error import MQTTError
from .mqtt_spec import MQTTPropertyId, MQTTReasonCode
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


@dataclass_transform(kw_only_default=True)
class MQTTPropertiesBase(SimpleNamespace):
    """Represents MQTT packet properties."""
    def __len__(self) -> int:
        return len(self.__dict__)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(f'{key}={value}' for key, value in self.__dict__.items())})"


BoolFieldT: TypeAlias = bool
IntFieldT: TypeAlias = int
StrFieldT: TypeAlias = str
BytesFieldT: TypeAlias = bytes
SetIntFieldT: TypeAlias = set[int]
SeqStrPairFieldT: TypeAlias = Sequence[tuple[str, str]]
AnyFieldT: TypeAlias = IntFieldT | StrFieldT | BytesFieldT | BoolFieldT | SetIntFieldT | SeqStrPairFieldT

PayloadFormatIndicatorT: TypeAlias = IntFieldT | None
MessageExpiryIntervalT: TypeAlias = IntFieldT | None
ContentTypeT: TypeAlias = StrFieldT | None
ResponseTopicT: TypeAlias = StrFieldT | None
CorrelationDataT: TypeAlias = BytesFieldT | None
SubscriptionIdentifierT: TypeAlias = SetIntFieldT | None
TopicAliasT: TypeAlias = IntFieldT | None
SessionExpiryIntervalT: TypeAlias = IntFieldT | None
AssignedClientIdentifierT: TypeAlias = StrFieldT | None
ServerKeepAliveT: TypeAlias = IntFieldT | None
AuthenticationMethodT: TypeAlias = StrFieldT | None
AuthenticationDataT: TypeAlias = BytesFieldT | None
RequestProblemInformationT: TypeAlias = BoolFieldT | None
WillDelayIntervalT: TypeAlias = IntFieldT | None
RequestResponseInformationT: TypeAlias = BoolFieldT | None
ResponseInformationT: TypeAlias = StrFieldT | None
ServerReferenceT: TypeAlias = StrFieldT | None
ReasonStringT: TypeAlias = StrFieldT | None
ReceiveMaximumT: TypeAlias = IntFieldT | None
TopicAliasMaximumT: TypeAlias = IntFieldT | None
MaximumQoST: TypeAlias = IntFieldT | None
RetainAvailableT: TypeAlias = BoolFieldT | None
UserPropertyT: TypeAlias = SeqStrPairFieldT | None
MaximumPacketSizeT: TypeAlias = IntFieldT | None
WildcardSubscriptionAvailableT: TypeAlias = BoolFieldT | None
SubscriptionIdentifierAvailableT: TypeAlias = BoolFieldT | None
SharedSubscriptionAvailableT: TypeAlias = BoolFieldT | None


# Build some data structures for fast serialization/deserialization of properties.
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
    MQTTPropertyId.PayloadFormatIndicator.name: _UInt8Serializer,
    MQTTPropertyId.MessageExpiryInterval.name: _UInt32Serializer,
    MQTTPropertyId.ContentType.name: _StringSerializer,
    MQTTPropertyId.ResponseTopic.name: _StringSerializer,
    MQTTPropertyId.CorrelationData.name: _BinarySerializer,
    MQTTPropertyId.SubscriptionIdentifier.name: _VarIntSerializer,  # This serializer is used for each set element.
    MQTTPropertyId.SessionExpiryInterval.name: _UInt32Serializer,
    MQTTPropertyId.AssignedClientIdentifier.name: _StringSerializer,
    MQTTPropertyId.ServerKeepAlive.name: _UInt16Serializer,
    MQTTPropertyId.AuthenticationMethod.name: _StringSerializer,
    MQTTPropertyId.AuthenticationData.name: _BinarySerializer,
    MQTTPropertyId.RequestProblemInformation.name: _BoolSerializer,
    MQTTPropertyId.WillDelayInterval.name: _UInt32Serializer,
    MQTTPropertyId.RequestResponseInformation.name: _BoolSerializer,
    MQTTPropertyId.ResponseInformation.name: _StringSerializer,
    MQTTPropertyId.ServerReference.name: _StringSerializer,
    MQTTPropertyId.ReasonString.name: _StringSerializer,
    MQTTPropertyId.ReceiveMaximum.name: _UInt16Serializer,
    MQTTPropertyId.TopicAliasMaximum.name: _UInt16Serializer,
    MQTTPropertyId.TopicAlias.name: _UInt16Serializer,
    MQTTPropertyId.MaximumQoS.name: _UInt8Serializer,
    MQTTPropertyId.RetainAvailable.name: _BoolSerializer,
    MQTTPropertyId.UserProperty.name: _StringPairSerializer,  # This serializer is used for each Sequence element.
    MQTTPropertyId.MaximumPacketSize.name: _UInt32Serializer,
    MQTTPropertyId.WildcardSubscriptionAvailable.name: _BoolSerializer,
    MQTTPropertyId.SubscriptionIdentifierAvailable.name: _BoolSerializer,
    MQTTPropertyId.SharedSubscriptionAvailable.name: _BoolSerializer
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
        properties: dict[str, AnyFieldT] = {}
        while remaining:
            # The spec calls for a variable integer for the property ID,
            #   but we know that the IDs are all 1 byte long,
            #   so we will decode them as uint8 to save a branch.
            key = data[0]
            data  = data[1:]
            remaining -= 1
            prop_name = MQTTPropertyId(key).name
            deserializer = _PropertySerializersById[key].deserializer
            value, sz = deserializer(data)
            data = data[sz:]
            remaining -= sz
            if prop_name == "SubscriptionIdentifier":
                # This property may appear multiple times, in any order.
                if prop_name not in properties:
                    properties[prop_name] = set()
                cast(SetIntFieldT, properties[prop_name]).add(cast(int, value))
            elif prop_name == "UserProperty":
                # This property may appear multiple times, in any order.
                if prop_name not in properties:
                    properties[prop_name] = []
                cast(list[tuple[str, str]], properties[prop_name]).append(cast(tuple[str, str], value))
            elif prop_name in properties:
                # Other properties must appear exactly once.
                raise MQTTError(f"Duplicate property {prop_name}", MQTTReasonCode.ProtocolError)
            else:
                properties[prop_name] = value  # type: ignore[assignment]
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
