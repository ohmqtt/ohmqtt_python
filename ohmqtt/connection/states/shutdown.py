from __future__ import annotations

from typing import Final, TYPE_CHECKING

from .base import FSMState
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.shutdown")


class ShutdownState(FSMState):
    """The final state.

    All buffers will be flushed and the socket will be closed immediately on entry."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        # We can enter this state from any other state.
        # Free up as many resources as possible.
        try:
            state_data.sock.close()
        except OSError as exc:
            logger.debug("Error while closing socket: %s", exc)
        state_data.decoder.reset()
        state_data.ws_decoder.reset()
        state_data.ws_handshake_buffer.clear()
        state_data.ws_nonce = ""
        state_data.write_buffer.clear()
        with fsm.selector:
            env.packet_buffer.clear()
            fsm.selector.close()
