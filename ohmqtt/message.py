from dataclasses import dataclass, field
from typing import Sequence

from .property import MQTTPropertiesBase


class MQTTMessageProps(MQTTPropertiesBase):
    """User interface for reading and writing MQTT properties."""
    PayloadFormatIndicator: int | None = None
    MessageExpiryInterval: int | None = None
    ContentType: str | None = None
    ResponseTopic: str | None = None
    CorrelationData: bytes | None = None
    SubscriptionIdentifier: set[int] | None = None
    SessionExpiryInterval: int | None = None
    AssignedClientIdentifier: str | None = None
    ServerKeepAlive: int | None = None
    AuthenticationMethod: str | None = None
    AuthenticationData: bytes | None = None
    RequestProblemInformation: bool | None = None
    WillDelayInterval: int | None = None
    RequestResponseInformation: bool | None = None
    ResponseInformation: str | None = None
    ServerReference: str | None = None
    ReasonString: str | None = None
    ReceiveMaximum: int | None = None
    TopicAliasMaximum: int | None = None
    TopicAlias: int | None = None
    MaximumQoS: int | None = None
    RetainAvailable: bool | None = None
    UserProperty: Sequence[tuple[str, str]] | None = None
    MaximumPacketSize: int | None = None
    WildcardSubscriptionAvailable: bool | None = None
    SubscriptionIdentifierAvailable: bool | None = None
    SharedSubscriptionAvailable: bool | None = None


@dataclass(match_args=True, slots=True)
class MQTTMessage:
    """Represents a message in the MQTT protocol."""
    topic: str = ""
    payload: bytes = b""
    qos: int = 0
    packet_id: int = 0
    retain: bool = False
    dup: bool = False
    properties: MQTTMessageProps = field(default_factory=MQTTMessageProps)
