from __future__ import annotations

from typing import Final, TYPE_CHECKING

from .base import FSMState
from .connecting import ConnectingState
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.reconnect_wait")


class ReconnectWaitState(FSMState):
    """Waiting to reconnect to the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.timeout.interval = params.reconnect_delay
        state_data.timeout.mark()

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        # Here we repurpose the keepalive timer to wait for a reconnect.
        if state_data.timeout.exceeded():
            logger.debug("Reconnecting")
            fsm.change_state(ConnectingState)
            return True
        with fsm.cond:
            if fsm.state is not ReconnectWaitState:
                # The state has changed, don't wait.
                return True
            if max_wait is None or max_wait > 0.0:
                timeout = state_data.timeout.get_timeout(max_wait)
                fsm.cond.wait(timeout)
        return False
