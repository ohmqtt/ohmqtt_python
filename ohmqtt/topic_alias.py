from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import NamedTuple

from .error import MQTTError
from .mqtt_spec import MQTTReasonCode
from .packet import MQTTPublishPacket


class MaxOutboundAliasError(Exception):
    """Exception raised when the maximum number of topic aliases is reached."""


class AliasPolicy(IntEnum):
    """Topic alias policy.

    NEVER: Never use topic aliases.

    TRY: Use topic aliases if possible.
        If an alias does not exist, attempt to create a new one.
        If the maximum number of aliases is reached, an alias will not be used.

    ALWAYS: Always use topic aliases.
        If an alias does not exist, attempt to create a new one.
        If the maximum number of aliases is reached, an exception will be raised."""
    NEVER = 0
    TRY = 1
    ALWAYS = 2


class OutboundLookupResult(NamedTuple):
    """Result of an outbound lookup.

    If the alias is 0, a topic alias should not be used in the publish packet.

    If the alias is not 0 and existed is False,
        the alias and topic should both be sent in the publish packet.

    If the alias is not 0 and existed is True,
        the alias should be sent in the publish packet and the topic should not be sent."""
    alias: int
    existed: bool


@dataclass(slots=True)
class InboundTopicAlias:
    """Inbound topic alias store."""
    aliases: dict[int, str] = field(default_factory=dict)
    max_alias: int = field(default=0, init=False)

    def reset(self) -> None:
        """Reset the topic alias state.

        This should be called at least when the connection is opened."""
        self.aliases.clear()
        self.max_alias = 0

    def handle(self, packet: MQTTPublishPacket) -> None:
        """Handle the topic alias in a publish packet.

        Stores the topic alias in the store if it is not recognized.

        If the topic alias is recognized, the topic is replaced with the stored topic."""
        topic = packet.topic
        alias = packet.properties.TopicAlias
        if alias is None:
            if not topic:
                raise MQTTError(
                    "Topic alias not found and topic is empty",
                    reason_code=MQTTReasonCode.ProtocolError,
                )
            return
        if alias > self.max_alias:
            raise MQTTError(
                f"Topic alias {alias} out of range",
                reason_code=MQTTReasonCode.TopicAliasInvalid,
            )

        if alias in self.aliases:
            if topic and self.aliases[alias] != topic:
                raise MQTTError(
                    f"Topic alias {alias} already exists",
                    reason_code=MQTTReasonCode.TopicAliasInvalid,
                )
            packet.topic = self.aliases[alias]
        else:
            if not topic:
                raise MQTTError(
                    "Topic alias not found and topic is empty",
                    reason_code=MQTTReasonCode.TopicAliasInvalid,
                )
            self.aliases[alias] = topic


@dataclass(slots=True)
class OutboundTopicAlias:
    """Inbound topic alias store."""
    aliases: dict[str, int] = field(default_factory=dict)
    next_alias: int = field(default=1, init=False)
    max_alias: int = field(default=0, init=False)

    def reset(self) -> None:
        """Reset the topic alias state.

        This should be called at least when the connection is opened."""
        self.aliases.clear()
        # Do not reset max_alias, as it is set upon connection.
        self.next_alias = 1

    def lookup(self, topic: str, policy: AliasPolicy) -> OutboundLookupResult:
        """Get the topic alias for a given topic from the client.

        An alias integer and a boolean indicating if the alias already existed will be returned.

        If the alias integer is 0, the alias was not created and the topic is not in the store.
        In this case, the topic alias must not be used in the publish packet."""
        if policy == AliasPolicy.NEVER:
            return OutboundLookupResult(0, False)
        if topic in self.aliases:
            return OutboundLookupResult(self.aliases[topic], True)
        if self.next_alias > self.max_alias:
            if policy == AliasPolicy.ALWAYS:
                raise MaxOutboundAliasError("Out of topic aliases and policy is ALWAYS")
            return OutboundLookupResult(0, False)
        alias = self.next_alias
        self.aliases[topic] = alias
        self.next_alias += 1
        return OutboundLookupResult(alias, False)

    def pop(self) -> None:
        """Remove the last assigned alias."""
        self.aliases.pop(next(reversed(self.aliases)), None)
        self.next_alias -= 1
