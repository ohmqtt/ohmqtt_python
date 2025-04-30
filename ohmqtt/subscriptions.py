from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, TYPE_CHECKING

from .message import MQTTMessage
from .topic_filter import match_topic_filter, validate_topic_filter

if TYPE_CHECKING:
    from .client import Client


SubscribeCallback = Callable[["Client", MQTTMessage], None]


@dataclass(match_args=True, slots=True, frozen=True)
class SubscriptionHandle:
    """Represents a subscription to a topic filter with a callback."""
    topic_filter: str
    callback: SubscribeCallback
    _client: Client

    def unsubscribe(self) -> None:
        """Unsubscribe from the topic filter."""
        self._client.unsubscribe(self.topic_filter, self.callback)


@dataclass(match_args=True, slots=True)
class Subscription:
    """Represents subscriptions to a topic filter with a callback."""
    max_qos: int = field(default=0, init=False)
    callbacks: set[SubscribeCallback] = field(init=False, default_factory=set)


class Subscriptions:
    """Container for MQTT subscriptions and their callbacks."""
    __slots__ = ("_subscriptions",)
    _subscriptions: dict[str, Subscription]

    def __init__(self) -> None:
        self._subscriptions = {}

    def add(self, topic_filter: str, qos: int, callback: SubscribeCallback) -> None:
        """Add a subscription with a callback."""
        validate_topic_filter(topic_filter)
        if topic_filter not in self._subscriptions:
            self._subscriptions[topic_filter] = Subscription()
        self._subscriptions[topic_filter].callbacks.add(callback)
        self._subscriptions[topic_filter].max_qos = max(
            self._subscriptions[topic_filter].max_qos, qos
        )

    def remove(self, topic_filter: str, callback: SubscribeCallback) -> int:
        """Remove a callback from a subscription.
        
        Returns the number of remaining callbacks for the topic filter."""
        if topic_filter in self._subscriptions:
            self._subscriptions[topic_filter].callbacks.discard(callback)
            remaining = len(self._subscriptions[topic_filter].callbacks)
            if remaining == 0:
                del self._subscriptions[topic_filter]
            return remaining
        else:
            return 0

    def remove_all(self, topic_filter: str) -> None:
        """Remove all callbacks from a subscription."""
        if topic_filter in self._subscriptions:
            del self._subscriptions[topic_filter]

    def get_callbacks(self, topic: str) -> frozenset[SubscribeCallback]:
        """Get all callbacks for a given topic."""
        callbacks = set()
        for topic_filter, subscription in self._subscriptions.items():
            if match_topic_filter(topic_filter, topic):
                callbacks.update(subscription.callbacks)
        return frozenset(callbacks)

    def clear(self) -> None:
        """Clear all subscriptions."""
        self._subscriptions.clear()

    def get_topics(self) -> dict[str, int]:
        """Get a dictionary of all subscriptions and their max QoS."""
        return {topic_filter: subscription.max_qos for topic_filter, subscription in self._subscriptions.items()}
