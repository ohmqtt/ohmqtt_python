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


# Mapping of keyword arguments to MQTT property names.
_MQTTPropertyKwargToName: dict[str, str] = {
    "payload_format_indicator": "PayloadFormatIndicator",
    "message_expiry_interval": "MessageExpiryInterval",
    "content_type": "ContentType",
    "response_topic": "ResponseTopic",
    "correlation_data": "CorrelationData",
    "subscription_identifier": "SubscriptionIdentifier",
    "session_expiry_interval": "SessionExpiryInterval",
    "assigned_client_identifier": "AssignedClientIdentifier",
    "server_keep_alive": "ServerKeepAlive",
    "authentication_method": "AuthenticationMethod",
    "authentication_data": "AuthenticationData",
    "request_problem_information": "RequestProblemInformation",
    "will_delay_interval": "WillDelayInterval",
    "request_response_information": "RequestResponseInformation",
    "response_information": "ResponseInformation",
    "server_reference": "ServerReference",
    "reason_string": "ReasonString",
    "receive_maximum": "ReceiveMaximum",
    "topic_alias_maximum": "TopicAliasMaximum",
    "topic_alias": "TopicAlias",
    "maximum_qos": "MaximumQoS",
    "retain_available": "RetainAvailable",
    "user_property": "UserProperty",
    "maximum_packet_size": "MaximumPacketSize",
    "wildcard_subscription_available": "WildcardSubscriptionAvailable",
    "subscription_identifier_available": "SubscriptionIdentifierAvailable",
    "shared_subscription_available": "SharedSubscriptionAvailable",
}


# Mapping of MQTT property names to keyword arguments.
_MQTTPropertyNameToKwarg: dict[str, str] = {
    "PayloadFormatIndicator": "payload_format_indicator",
    "MessageExpiryInterval": "message_expiry_interval",
    "ContentType": "content_type",
    "ResponseTopic": "response_topic",
    "CorrelationData": "correlation_data",
    "SubscriptionIdentifier": "subscription_identifier",
    "SessionExpiryInterval": "session_expiry_interval",
    "AssignedClientIdentifier": "assigned_client_identifier",
    "ServerKeepAlive": "server_keep_alive",
    "AuthenticationMethod": "authentication_method",
    "AuthenticationData": "authentication_data",
    "RequestProblemInformation": "request_problem_information",
    "WillDelayInterval": "will_delay_interval",
    "RequestResponseInformation": "request_response_information",
    "ResponseInformation": "response_information",
    "ServerReference": "server_reference",
    "ReasonString": "reason_string",
    "ReceiveMaximum": "receive_maximum",
    "TopicAliasMaximum": "topic_alias_maximum",
    "TopicAlias": "topic_alias",
    "MaximumQoS": "maximum_qos",
    "RetainAvailable": "retain_available",
    "UserProperty": "user_property",
    "MaximumPacketSize": "maximum_packet_size",
    "WildcardSubscriptionAvailable": "wildcard_subscription_available",
    "SubscriptionIdentifierAvailable": "subscription_identifier_available",
    "SharedSubscriptionAvailable": "shared_subscription_available",
}


class MQTTProperties:
    """Container for MQTT packet properties."""
    properties: MQTTPropertyDict
    __slots__ = ("properties",)

    def __init__(self, properties: MQTTPropertyDict | None = None):
        self.properties = properties or {}

    def __str__(self) -> str:
        prop_list = ", ".join([f"{key}={value!r}" for key, value in self.properties.items()])
        return f"MQTTProperties[{prop_list}]"

    def __iter__(self):
        return iter(self.properties)

    def __len__(self):
        return len(self.properties)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MQTTProperties):
            return NotImplemented
        return self.properties == other.properties

    def __getitem__(self, key: str):
        # Ignore typing here because mypy doesn't support non-literal TypedDict keys.
        return self.properties[key] # type: ignore

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.properties.items())))

    def copy(self) -> "MQTTProperties":
        """Return a copy of the properties."""
        return MQTTProperties(self.properties)

    def validate(self, packet_type: MQTTPacketType | None = None, is_will: bool = False) -> None:
        """Validate the properties against a packet type or as a will message."""
        for key, value in self.properties.items():
            prop_id = MQTTPropertyId[key]
            if packet_type is not None and packet_type not in MQTTPropertyPacketTypes[prop_id]:
                raise MQTTError(f"Property {prop_id} not allowed in packet type {packet_type}", MQTTReasonCode.ProtocolError)
            if is_will and MQTTPropertyId[key] not in MQTTPropertyAllowedInWill:
                raise MQTTError(f"Property {prop_id} not allowed in Will message", MQTTReasonCode.ProtocolError)
        # TODO: Numeric limits
        # TODO: Uniqueness

    def encode(self) -> bytes:
        """Encode MQTT properties to a buffer."""
        data = b""
        for key, prop_value in self.properties.items():
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

    @classmethod
    def decode(cls, data: bytes) -> tuple["MQTTProperties", int]:
        """Decode MQTT properties from a buffer.
        
        Returns a tuple of the decoded properties and the number of bytes consumed."""
        length, offset = decode_varint(data)
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
        return cls(properties), offset
