from dataclasses import dataclass, field

from .property import MQTTPropertyDict


@dataclass(match_args=True, slots=True)
class MQTTMessage:
    """Represents a message in the MQTT protocol."""
    topic: str = ""
    payload: bytes = b""
    qos: int = 0
    packet_id: int = 0
    retain: bool = False
    dup: bool = False
    properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())
