class MQTTTopicFilter:
    """A topic filter for MQTT."""
    __slots__ = ("_topic_filter",)
    _topic_filter: str

    def __init__(self, topic_filter: str):
        if len(topic_filter) == 0:
            raise ValueError("Topic filter cannot be empty")
        if "\u0000" in topic_filter:
            raise ValueError("Topic filter cannot contain null characters")
        if len(topic_filter.encode("utf-8")) > 65535:
            raise ValueError("Topic filter is too long (> 65535 bytes encoded as UTF-8)")
        multi_level_wildcard_index = topic_filter.find("#")
        if multi_level_wildcard_index != -1:
            if multi_level_wildcard_index != len(topic_filter) - 1:
                raise ValueError("Multi-level wildcard '#' must be the last character in the topic filter")
            if len(topic_filter) > 1 and topic_filter[-2] != "/":
                raise ValueError("Multi-level wildcard '#' must be preceded by a '/' unless it is the only character in the topic filter")
        self._topic_filter = topic_filter

    def __hash__(self) -> int:
        # The hash of a TopicFilter must be the hash of the topic filter string.
        return hash(self._topic_filter)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, MQTTTopicFilter):
            return self._topic_filter == other._topic_filter
        elif isinstance(other, str):
            # Allow comparison with a string.
            return self._topic_filter == other
        else:
            return NotImplemented

    def __repr__(self) -> str:
        return f"MQTTTopicFilter{{{self._topic_filter}}}"

    def __str__(self) -> str:
        return repr(self)

    @property
    def topic_filter(self) -> str:
        """Get the topic filter string."""
        return self._topic_filter

    def match(self, topic: str) -> bool:
        """Check if the topic matches the filter."""
        if len(topic) == 0:
            raise ValueError("Topic name cannot be empty")
        if "\u0000" in topic:
            raise ValueError("Topic name cannot contain null characters")
        if "#" in topic or "+" in topic:
            raise ValueError("Topic name cannot contain wildcards '#' or '+'")
        if len(topic.encode("utf-8")) > 65535:
            raise ValueError("Topic name is too long (> 65535 bytes encoded as UTF-8)")
        if self._topic_filter == topic:
            return True  # Exact match.
        hidden_topic = topic.startswith("$")
        if self._topic_filter == "#" and not hidden_topic:
            return True  # Matches everything that doesn't start with '$'.
        if "#" in self._topic_filter:
            base = self._topic_filter[:-2]
            if base and topic.startswith(base):
                return True  # Matches everything under the base topic.
            return False
        if "+" in self._topic_filter:
            if self._topic_filter.startswith("+") and hidden_topic:
                return False
            filter_levels = self._topic_filter.split("/")
            topic_levels = topic.split("/")
            if len(filter_levels) != len(topic_levels):
                return False
            for filter_level, topic_level in zip(filter_levels, topic_levels):
                if filter_level != "+" and filter_level != topic_level:
                    return False
            return True
        return False
