from typing import Callable

from .property import MQTTPropertyDict
from .topic_filter import MQTTTopicFilter


SubscribeCallback = Callable[[str, bytes, MQTTPropertyDict], None]


class Subscriptions:
    """Container for MQTT subscriptions and their callbacks."""
    __slots__ = ("_subscriptions",)
    _subscriptions: dict[MQTTTopicFilter, set[SubscribeCallback]]

    def __init__(self) -> None:
        self._subscriptions = {}

    def add(self, topic_filter: str, callback: SubscribeCallback) -> None:
        """Add a subscription with a callback."""
        filter_obj = MQTTTopicFilter(topic_filter)
        if filter_obj not in self._subscriptions:
            self._subscriptions[filter_obj] = set()
        self._subscriptions[filter_obj].add(callback)

    def remove(self, topic_filter: str, callback: SubscribeCallback) -> int:
        """Remove a callback from a subscription.
        
        Returns the number of remaining callbacks for the topic filter."""
        filter_obj = MQTTTopicFilter(topic_filter)
        if filter_obj in self._subscriptions:
            self._subscriptions[filter_obj].discard(callback)
            remaining = len(self._subscriptions[filter_obj])
            if remaining == 0:
                del self._subscriptions[filter_obj]
            return remaining
        else:
            return 0

    def remove_all(self, topic_filter: str) -> None:
        """Remove all callbacks from a subscription."""
        filter_obj = MQTTTopicFilter(topic_filter)
        if filter_obj in self._subscriptions:
            del self._subscriptions[filter_obj]

    def get_callbacks(self, topic: str) -> frozenset[SubscribeCallback]:
        """Get all callbacks for a given topic."""
        callbacks = set()
        for filter_obj, filter_callbacks in self._subscriptions.items():
            if filter_obj.match(topic):
                callbacks.update(filter_callbacks)
        return frozenset(callbacks)
