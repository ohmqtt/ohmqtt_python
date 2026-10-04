"""Constants from the MQTT specification."""

from enum import IntEnum
from typing import Final


MAX_PACKET_ID: Final = 0xffff


class MQTTPacketType(IntEnum):
    """Types of MQTT control packets, mapped to identifiers from the specification."""
    CONNECT = 0x01
    CONNACK = 0x02
    PUBLISH = 0x03
    PUBACK = 0x04
    PUBREC = 0x05
    PUBREL = 0x06
    PUBCOMP = 0x07
    SUBSCRIBE = 0x08
    SUBACK = 0x09
    UNSUBSCRIBE = 0x0a
    UNSUBACK = 0x0b
    PINGREQ = 0x0c
    PINGRESP = 0x0d
    DISCONNECT = 0x0e
    AUTH = 0x0f


class MQTTPropertyId(IntEnum):
    """Types of MQTT properties, mapped to identifiers from the specification."""
    PayloadFormatIndicator = 0x01
    MessageExpiryInterval = 0x02
    ContentType = 0x03
    ResponseTopic = 0x08
    CorrelationData = 0x09
    SubscriptionIdentifier = 0x0b
    SessionExpiryInterval = 0x11
    AssignedClientIdentifier = 0x12
    ServerKeepAlive = 0x13
    AuthenticationMethod = 0x15
    AuthenticationData = 0x16
    RequestProblemInformation = 0x17
    WillDelayInterval = 0x18
    RequestResponseInformation = 0x19
    ResponseInformation = 0x1a
    ServerReference = 0x1c
    ReasonString = 0x1f
    ReceiveMaximum = 0x21
    TopicAliasMaximum = 0x22
    TopicAlias = 0x23
    MaximumQoS = 0x24
    RetainAvailable = 0x25
    UserProperty = 0x26
    MaximumPacketSize = 0x27
    WildcardSubscriptionAvailable = 0x28
    SubscriptionIdentifierAvailable = 0x29
    SharedSubscriptionAvailable = 0x2a


class MQTTReasonCode(IntEnum):
    """Indicates the result of an operation."""
    Success = 0x00
    NormalDisconnection = 0x00
    GrantedQoS0 = 0x00
    GrantedQoS1 = 0x01
    GrantedQoS2 = 0x02
    DisconnectWithWillMessage = 0x04
    NoMatchingSubscribers = 0x10
    NoSubscriptionExisted = 0x11
    ContinueAuthentication = 0x18
    ReAuthenticate = 0x19
    UnspecifiedError = 0x80
    MalformedPacket = 0x81
    ProtocolError = 0x82
    ImplementationSpecificError = 0x83
    UnsupportedProtocolVersion = 0x84
    ClientIdentifierNotValid = 0x85
    BadUserNameOrPassword = 0x86
    NotAuthorized = 0x87
    ServerUnavailable = 0x88
    ServerBusy = 0x89
    Banned = 0x8A
    ServerShuttingDown = 0x8B
    BadAuthenticationMethod = 0x8C
    KeepAliveTimeout = 0x8D
    SessionTakenOver = 0x8E
    TopicFilterInvalid = 0x8F
    TopicNameInvalid = 0x90
    PacketIdentifierInUse = 0x91
    PacketIdentifierNotFound = 0x92
    ReceiveMaximumExceeded = 0x93
    TopicAliasInvalid = 0x94
    PacketTooLarge = 0x95
    MessageRateTooHigh = 0x96
    QuotaExceeded = 0x97
    AdministrativeAction = 0x98
    PayloadFormatInvalid = 0x99
    RetainNotSupported = 0x9A
    QoSNotSupported = 0x9B
    UseAnotherServer = 0x9C
    ServerMoved = 0x9D
    SharedSubscriptionsNotSupported = 0x9E
    ConnectionRateExceeded = 0x9F
    MaximumConnectTime = 0xA0
    SubscriptionIdentifiersNotSupported = 0xA1
    WildcardSubscriptionsNotSupported = 0xA2

    def is_error(self) -> bool:
        """Check if the reason code indicates an error."""
        return self >= 0x80


CONNACK_REASON_CODES: Final = frozenset((
    MQTTReasonCode.Success,
    MQTTReasonCode.UnspecifiedError,
    MQTTReasonCode.MalformedPacket,
    MQTTReasonCode.ProtocolError,
    MQTTReasonCode.ImplementationSpecificError,
    MQTTReasonCode.UnsupportedProtocolVersion,
    MQTTReasonCode.ClientIdentifierNotValid,
    MQTTReasonCode.BadUserNameOrPassword,
    MQTTReasonCode.NotAuthorized,
    MQTTReasonCode.ServerUnavailable,
    MQTTReasonCode.ServerBusy,
    MQTTReasonCode.Banned,
    MQTTReasonCode.BadAuthenticationMethod,
    MQTTReasonCode.TopicNameInvalid,
    MQTTReasonCode.PacketTooLarge,
    MQTTReasonCode.QuotaExceeded,
    MQTTReasonCode.PayloadFormatInvalid,
    MQTTReasonCode.RetainNotSupported,
    MQTTReasonCode.QoSNotSupported,
    MQTTReasonCode.UseAnotherServer,
    MQTTReasonCode.ServerMoved,
    MQTTReasonCode.ConnectionRateExceeded,
))
PUBACK_REASON_CODES: Final = frozenset((
    MQTTReasonCode.Success,
    MQTTReasonCode.NoMatchingSubscribers,
    MQTTReasonCode.UnspecifiedError,
    MQTTReasonCode.ImplementationSpecificError,
    MQTTReasonCode.NotAuthorized,
    MQTTReasonCode.TopicNameInvalid,
    MQTTReasonCode.PacketIdentifierInUse,
    MQTTReasonCode.QuotaExceeded,
    MQTTReasonCode.PayloadFormatInvalid,
))
PUBREC_REASON_CODES: Final = PUBACK_REASON_CODES
PUBREL_REASON_CODES: Final = frozenset((
    MQTTReasonCode.Success,
    MQTTReasonCode.PacketIdentifierNotFound,
))
PUBCOMP_REASON_CODES: Final = PUBREL_REASON_CODES
SUBACK_REASON_CODES: Final = frozenset((
    MQTTReasonCode.GrantedQoS0,
    MQTTReasonCode.GrantedQoS1,
    MQTTReasonCode.GrantedQoS2,
    MQTTReasonCode.UnspecifiedError,
    MQTTReasonCode.ImplementationSpecificError,
    MQTTReasonCode.NotAuthorized,
    MQTTReasonCode.TopicFilterInvalid,
    MQTTReasonCode.PacketIdentifierInUse,
    MQTTReasonCode.QuotaExceeded,
    MQTTReasonCode.SharedSubscriptionsNotSupported,
    MQTTReasonCode.SubscriptionIdentifiersNotSupported,
    MQTTReasonCode.WildcardSubscriptionsNotSupported,
))
UNSUBACK_REASON_CODES: Final = frozenset((
    MQTTReasonCode.Success,
    MQTTReasonCode.NoSubscriptionExisted,
    MQTTReasonCode.UnspecifiedError,
    MQTTReasonCode.ImplementationSpecificError,
    MQTTReasonCode.NotAuthorized,
    MQTTReasonCode.TopicFilterInvalid,
    MQTTReasonCode.PacketIdentifierInUse,
))
DISCONNECT_REASON_CODES: Final = frozenset((
    MQTTReasonCode.NormalDisconnection,
    MQTTReasonCode.DisconnectWithWillMessage,
    MQTTReasonCode.UnspecifiedError,
    MQTTReasonCode.MalformedPacket,
    MQTTReasonCode.ProtocolError,
    MQTTReasonCode.ImplementationSpecificError,
    MQTTReasonCode.NotAuthorized,
    MQTTReasonCode.ServerBusy,
    MQTTReasonCode.ServerShuttingDown,
    MQTTReasonCode.KeepAliveTimeout,
    MQTTReasonCode.SessionTakenOver,
    MQTTReasonCode.TopicFilterInvalid,
    MQTTReasonCode.TopicNameInvalid,
    MQTTReasonCode.ReceiveMaximumExceeded,
    MQTTReasonCode.TopicAliasInvalid,
    MQTTReasonCode.PacketTooLarge,
    MQTTReasonCode.MessageRateTooHigh,
    MQTTReasonCode.QuotaExceeded,
    MQTTReasonCode.AdministrativeAction,
    MQTTReasonCode.PayloadFormatInvalid,
    MQTTReasonCode.RetainNotSupported,
    MQTTReasonCode.QoSNotSupported,
    MQTTReasonCode.UseAnotherServer,
    MQTTReasonCode.ServerMoved,
    MQTTReasonCode.SharedSubscriptionsNotSupported,
    MQTTReasonCode.ConnectionRateExceeded,
    MQTTReasonCode.MaximumConnectTime,
    MQTTReasonCode.SubscriptionIdentifiersNotSupported,
    MQTTReasonCode.WildcardSubscriptionsNotSupported,
))
AUTH_REASON_CODES: Final = frozenset((
    MQTTReasonCode.Success,
    MQTTReasonCode.ContinueAuthentication,
    MQTTReasonCode.ReAuthenticate,
))


class MQTTQoS(IntEnum):
    """Quality of Service level for messaging."""
    Q0 = 0
    Q1 = 1
    Q2 = 2
