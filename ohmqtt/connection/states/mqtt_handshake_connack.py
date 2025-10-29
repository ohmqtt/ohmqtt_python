from __future__ import annotations

from typing import cast, Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .connected import ConnectedState
from ..decoder import ClosedSocketError
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger
from ...mqtt_spec import MQTTPacketType
from ...packet import MQTTConnAckPacket

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.mqtt_handshake_connack")


class MQTTHandshakeConnAckState(FSMState):
    """Receiving MQTT CONNACK packet from the broker."""
    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("MQTT CONNACK keepalive timeout")
            fsm.change_state(ClosedState)
            return True

        want_read = False
        try:
            packet = state_data.decoder.decode(state_data.sock)
            if packet is None:
                want_read = True
        except ClosedSocketError:
            logger.exception("Socket was closed")
            fsm.change_state(ClosedState)
            return True

        if want_read:
            # Incomplete packet, wait for more data.
            with fsm.selector:
                timeout = state_data.timeout.get_timeout(max_wait)
                fsm.selector.select(read=True, timeout=timeout)
            return False

        if packet is not None and packet.packet_type == MQTTPacketType.CONNACK:
            packet = cast(MQTTConnAckPacket, packet)
            state_data.connack = packet
            if packet.properties.ServerKeepAlive is not None:
                state_data.keepalive.keepalive_interval = packet.properties.ServerKeepAlive
            fsm.change_state(ConnectedState)
            return True
        pt = packet.packet_type.name if packet is not None else "None"
        logger.error("Unexpected '%s' packet while waiting for CONNACK", pt)
        fsm.change_state(ClosedState)
        return True
