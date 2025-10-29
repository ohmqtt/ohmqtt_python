from __future__ import annotations

from typing import ClassVar, Sequence, TYPE_CHECKING

from ..types import ConnectParams, StateData, StateEnvironment

if TYPE_CHECKING:
    from ..fsm import FSM


class FSMState:
    """A finite state in the FSM."""
    can_request_from: ClassVar[Sequence[type[FSMState]]] = ()
    transitions_to: ClassVar[Sequence[type[FSMState]]] = ()

    def __init__(self) -> None:
        raise TypeError("Do not instantiate FSMStates")

    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        """Called when entering the state.

        This method must not block."""

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        """Called when handling the state.

        This method may block if max_wait is >0 or None.

        :return: True if the state is finished."""
        return True
