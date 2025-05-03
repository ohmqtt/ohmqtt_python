from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import NamedTuple

from .error import MQTTError
from .mqtt_spec import MQTTReasonCode


class MaxOutboundAliasError(Exception):
    """Exception raised when the maximum number of topic aliases is reached."""
    pass


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
class TopicAlias:
    """Topic alias store for MQTT v5."""
    out_aliases: dict[str, int] = field(default_factory=dict)
    next_out_alias: int = field(default=1, init=False)
    max_out_alias: int = field(default=0, init=False)
    in_aliases: dict[int, str] = field(default_factory=dict)
    next_in_alias: int = field(default=1, init=False)
    max_in_alias: int = field(default=0, init=False)

    def reset(self) -> None:
        """Reset the topic alias state."""
        self.out_aliases.clear()
        # Do not reset max_out_alias, as it is set by the client in connect.
        self.next_out_alias = 1
        self.in_aliases.clear()
        self.max_in_alias = 0

    def lookup_outbound(self, topic: str, policy: AliasPolicy) -> OutboundLookupResult:
        """Get the topic alias for a given topic from the client.
        
        An alias integer and a boolean indicating if the alias already existed will be returned.
        
        If the alias integer is 0, the alias was not created and the topic is not in the store.
        In this case, the topic alias must not be used in the publish packet."""
        if policy == AliasPolicy.NEVER:
            return OutboundLookupResult(0, False)
        if topic in self.out_aliases:
            return OutboundLookupResult(self.out_aliases[topic], True)
        if self.next_out_alias > self.max_out_alias:
            if policy == AliasPolicy.ALWAYS:
                raise MaxOutboundAliasError("Out of topic aliases and policy is ALWAYS")
            else:
                return OutboundLookupResult(0, False)
        alias = self.next_out_alias
        self.out_aliases[topic] = alias
        self.next_out_alias += 1
        return OutboundLookupResult(alias, False)

    def remove_outbound(self, topic: str) -> None:
        """Remove the topic alias for a given topic from the client."""
        self.out_aliases.pop(topic)

    def store_inbound(self, alias: int, topic: str) -> None:
        """Store the topic for a given topic alias from the server."""
        if alias > self.max_in_alias:
            raise MQTTError(
                f"Topic alias {alias} out of range",
                reason_code=MQTTReasonCode.TopicAliasInvalid,
            )
        if alias in self.in_aliases and self.in_aliases[alias] != topic:
            raise MQTTError(
                f"Topic alias {alias} already exists",
                reason_code=MQTTReasonCode.TopicAliasInvalid,
            )
        self.in_aliases[alias] = topic

    def lookup_inbound(self, alias: int) -> str:
        """Get the topic for a given topic alias from the server."""
        if alias > self.max_in_alias:
            raise MQTTError(
                f"Topic alias {alias} out of range",
                reason_code=MQTTReasonCode.TopicAliasInvalid,
            )
        if alias in self.in_aliases:
            return self.in_aliases[alias]
        raise MQTTError(
            f"Topic alias {alias} not found",
            reason_code=MQTTReasonCode.TopicAliasInvalid,
        )
