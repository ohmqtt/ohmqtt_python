from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
import itertools
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
    def get(self, client_id: str, exclude: set[int], count: int) -> list[MQTTPacketWithId]:
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
        """Get the next publish packet ID for a client."""
        ...  # pragma: no cover


@dataclass
class _SessionState:
    publish_ids: set[int] = field(default_factory=set)
    pubrel_ids: set[int] = field(default_factory=set)
    packets: list[MQTTPacketWithId] = field(default_factory=list)


class InMemorySessionPersistence(SessionPersistenceBackend):
    _sessions: dict[str, _SessionState]
    _ack_map: Mapping[MQTTPacketType, MQTTPacketType] = {
        MQTTPacketType.PUBACK: MQTTPacketType.PUBLISH,
        MQTTPacketType.PUBREC: MQTTPacketType.PUBLISH,
        MQTTPacketType.PUBREL: MQTTPacketType.PUBREC,
        MQTTPacketType.PUBCOMP: MQTTPacketType.PUBREL,
    }

    def __init__(self) -> None:
        self._sessions = {}
        self._next_packet_id = 1

    def clear(self, client_id: str) -> None:
        self._sessions.pop(client_id, None)

    def has(self, client_id: str) -> bool:
        return client_id in self._sessions and len(self._sessions[client_id].packets) > 0

    def put(self, client_id: str, packet: MQTTPacketWithId) -> None:
        if client_id not in self._sessions:
            self._sessions[client_id] = _SessionState()
        self._sessions[client_id].packets.append(packet)
        if packet.packet_type == MQTTPacketType.PUBLISH:
            self._sessions[client_id].publish_ids.add(packet.packet_id)
        elif packet.packet_type == MQTTPacketType.PUBREL:
            self._sessions[client_id].pubrel_ids.add(packet.packet_id)

    def get(self, client_id: str, exclude: set[int], count: int) -> list[MQTTPacketWithId]:
        if client_id not in self._sessions:
            return []
        slice = itertools.islice((p for p in self._sessions[client_id].packets if p.packet_id not in exclude), count)
        return list(slice)

    def mark_dup(self, client_id: str, packet_id: int) -> None:
        packet = next((p for p in self._sessions[client_id].packets if p.packet_type == MQTTPacketType.PUBLISH and p.packet_id == packet_id), None)
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
            (p for p in self._sessions[client_id].packets if p.packet_type == ref_packet_type and p.packet_id == packet_id),
            None
        )
        if ref_packet is None:
            raise KeyError(f"Packet: {ref_packet_type}:{packet_id} not found in session for client: {client_id}")
        self._sessions[client_id].packets.remove(ref_packet)
        if ref_packet.packet_type == MQTTPacketType.PUBLISH:
            self._sessions[client_id].publish_ids.remove(ref_packet.packet_id)
        elif ref_packet.packet_type == MQTTPacketType.PUBREL:
            self._sessions[client_id].pubrel_ids.remove(ref_packet.packet_id)
        return ref_packet

    def next_packet_id(self, client_id: str) -> int:
        if client_id not in self._sessions:
            self._sessions[client_id] = _SessionState()
            return self._next_packet_id
        next_id = self._next_packet_id
        used_ids = frozenset(self._sessions[client_id].publish_ids | self._sessions[client_id].pubrel_ids)
        if len(used_ids) >= 65535:
            raise ValueError("No available packet IDs")
        while next_id in used_ids:
            next_id += 1
            if next_id > 65535:
                next_id = 1
        self._next_packet_id = next_id
        if next_id > 65535:
            next_id = 1
        return next_id
