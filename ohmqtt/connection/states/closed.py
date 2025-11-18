from __future__ import annotations

import socket
from typing import Final, TYPE_CHECKING

from .base import FSMState
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger
from ...packet import MQTTDisconnectPacket

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.closed")


class ClosedState(FSMState):
    """Connection is closed.

    All buffers will be flushed and the socket will be closed after conditionally sending a DISCONNECT."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        # Avoid circular imports.
        from .connecting import ConnectingState  # noqa: PLC0415
        from .reconnect_wait import ReconnectWaitState  # noqa: PLC0415

        if state_data.open_called:
            state_data.open_called = False
            if state_data.disconnect_rc is not None and not state_data.write_buffer:
                disconnect_packet = MQTTDisconnectPacket(reason_code=state_data.disconnect_rc)
                # Try to send a DISCONNECT packet, but no problem if we can't.
                try:
                    state_data.sock.send(disconnect_packet.encode())
                    logger.debug("---> %s", disconnect_packet)
                except OSError:
                    logger.debug("Failed to send DISCONNECT packet")
            if params.reconnect_delay > 0 and fsm.requested_state == ConnectingState:
                fsm.change_state(ReconnectWaitState)
        try:
            state_data.sock.shutdown(socket.SHUT_RDWR)
        except OSError as exc:
            logger.debug("Error while shutting down socket: %s", exc)
        try:
            state_data.sock.close()
        except OSError as exc:
            logger.debug("Error while closing socket: %s", exc)
        state_data.decoder.reset()
        state_data.write_buffer.clear()
        env.packet_buffer.clear()
