from abc import ABCMeta, abstractmethod
from typing import cast, Mapping

from .mqtt_spec import MQTTPacketType
from .packet import MQTTPacketWithId, MQTTPublishPacket


class SessionPersistenceBackend(metaclass=ABCMeta):
    @abstractmethod
    def clear(self, client_id: str) -> None:
        """Clear all packets from the session for a client."""
        ...  # pragma: no cover

    @abstractmethod
    def has(self, client_id: str) -> bool:
        """Check if a client has a session."""
        ...  # pragma: no cover

    @abstractmethod
    def put(self, client_id: str, packet: MQTTPacketWithId) -> None:
        """Add a packet to the session for a client."""
        ...  # pragma: no cover

    @abstractmethod
    def get(self, client_id: str, exclude: set[MQTTPacketWithId], count: int) -> list[MQTTPacketWithId]:
        """Get packets from the session for a client."""
        ...  # pragma: no cover

    @abstractmethod
    def mark_dup(self, client_id: str, packet_id: int) -> None:
        """Mark a publish packet as a duplicate transmission for a client."""
        ...  # pragma: no cover

    @abstractmethod
    def ack(self, client_id: str, packet: MQTTPacketWithId) -> MQTTPacketWithId:
        """Handle an acknowledgement packet by removing the acknowledged from persistence.
        
        Returns the acknowledged packet."""
        ...  # pragma: no cover

    @abstractmethod
    def next_packet_id(self, client_id: str) -> int:
        """Get the next pubblish packet ID for a client."""
        ...  # pragma: no cover


class InMemorySessionPersistence(SessionPersistenceBackend):
    _sessions: dict[str, list[MQTTPacketWithId]]
    _ack_map: Mapping[MQTTPacketType, MQTTPacketType] = {
        MQTTPacketType.PUBACK: MQTTPacketType.PUBLISH,
        MQTTPacketType.PUBREC: MQTTPacketType.PUBLISH,
        MQTTPacketType.PUBREL: MQTTPacketType.PUBREC,
        MQTTPacketType.PUBCOMP: MQTTPacketType.PUBREL,
    }

    def __init__(self) -> None:
        self._sessions = {}

    def clear(self, client_id: str) -> None:
        self._sessions.pop(client_id, None)

    def has(self, client_id: str) -> bool:
        return client_id in self._sessions and len(self._sessions[client_id]) > 0

    def put(self, client_id: str, packet: MQTTPacketWithId) -> None:
        if client_id not in self._sessions:
            self._sessions[client_id] = []
        self._sessions[client_id].append(packet)

    def get(self, client_id: str, exclude: set[MQTTPacketWithId], count: int) -> list[MQTTPacketWithId]:
        if client_id not in self._sessions:
            return []
        return [p for p in self._sessions[client_id] if p not in exclude][:count]

    def mark_dup(self, client_id: str, packet_id: int) -> None:
        packet = next((p for p in self._sessions[client_id] if p.packet_id == packet_id), None)
        if packet is None:
            raise KeyError(f"Publish packet: {packet_id} not found in session for client: {client_id}")
        cast(MQTTPublishPacket, packet).dup = True
    
    def ack(self, client_id: str, packet: MQTTPacketWithId) -> MQTTPacketWithId:
        try:
            ref_packet_type = self._ack_map[packet.packet_type]
        except KeyError:
            raise ValueError(f"Cannot ack packet of type {packet.packet_type}")
        packet_id = packet.packet_id
        ref_packet = next(
            (p for p in self._sessions[client_id] if p.packet_type == ref_packet_type and p.packet_id == packet_id),
            None
        )
        if ref_packet is None:
            raise KeyError(f"Packet: {ref_packet_type}:{packet_id} not found in session for client: {client_id}")
        self._sessions[client_id].remove(ref_packet)
        return ref_packet

    def next_packet_id(self, client_id: str) -> int:
        if client_id not in self._sessions:
            self._sessions[client_id] = []
        packet_ids = frozenset(
            p.packet_id for p in self._sessions[client_id] if p.packet_type in (MQTTPacketType.PUBLISH, MQTTPacketType.PUBREL)
        )
        if not packet_ids:
            return 1
        next_id = max(packet_ids) + 1
        if next_id > 65535:
            next_id = 1
            while next_id in packet_ids:
                next_id += 1
            if next_id > 65535:
                raise ValueError("No available packet IDs")
        return next_id
