from __future__ import annotations

from typing import ClassVar, Sequence, TYPE_CHECKING

from ..types import ConnectParams, StateData, StateEnvironment

if TYPE_CHECKING:
    from ..fsm import FSM


class FSMState:
    """A finite state in the FSM."""
    request_from: ClassVar[Sequence[type[FSMState]]] = ()
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

    @classmethod
    def can_request_from(cls, prev_state: type[FSMState]) -> bool:
        """Check if the FSM can request this state from the given previous state.

        :param prev_state: The previous state.
        :return: True if the FSM can request this state from the given previous state, False otherwise."""
        return prev_state in cls.request_from

    @classmethod
    def can_transition_to(cls, next_state: type[FSMState]) -> bool:
        """Check if the FSM can transition to the given state.

        :param next_state: The state to check.
        :return: True if the FSM can transition to the given state, False otherwise."""
        return next_state in cls.transitions_to


    @classmethod
    def is_final_state(cls) -> bool:
        """Check if this state is a final state.

        :return: True if this state is a final state, False otherwise."""
        return not cls.transitions_to
