from __future__ import annotations

import ssl
import socket
from typing import Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger
from ...mqtt_spec import MQTTReasonCode

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.closing")


class ClosingState(FSMState):
    """Gracefully closing the connection.

    The socket will be shutdown for reading, but existing buffers will be flushed."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        # Avoid circular imports.
        from .connected import ConnectedState  # noqa: PLC0415

        if fsm.previous_state == ConnectedState:
            logger.debug("Gracefully closing connection")
            if state_data.disconnect_rc is None:
                state_data.disconnect_rc = MQTTReasonCode.NormalDisconnection
        else:
            logger.debug("Skipping ClosingState")
            fsm.change_state(ClosedState)
            return

        state_data.timeout.interval = params.connect_timeout
        state_data.timeout.mark()

        # Shutdown only the read side of the socket, so we can still send data.
        # We don't want to shutdown the socket if we're using TLS, it would cause the protocol to fail.
        if not params.address.use_tls:
            try:
                state_data.sock.shutdown(socket.SHUT_RD)
            except OSError as exc:
                logger.debug("Error while shutting down socket reading: %s", exc)

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        # Wait for the socket to be writable.
        if state_data.timeout.exceeded():
            logger.error("ClosingState timed out")
            fsm.change_state(ClosedState)
            return True

        if not state_data.write_buffer:
            logger.debug("No more data to send, connection closed")
            fsm.change_state(ClosedState)
            return True

        with fsm.selector:
            timeout = state_data.timeout.get_timeout(max_wait)
            _, writable = fsm.selector.select(write=True, timeout=timeout)

        if writable:
            try:
                sent = state_data.sock.send(state_data.write_buffer)
                del state_data.write_buffer[:sent]
                state_data.keepalive.mark_send()
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                pass
            except BrokenPipeError:
                logger.error("Socket lost while closing")
                fsm.change_state(ClosedState)
                return True
        return False
