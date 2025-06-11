from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import ClassVar, Final, Sequence

from .selector import InterruptibleSelector
from .timeout import Timeout
from .types import ConnectParams, StateData, StateEnvironment
from ..logger import get_logger

logger: Final = get_logger("connection.fsm")


class InvalidStateError(Exception):
    """Exception raised when an operation is performed in an invalid state."""


@dataclass(slots=True, init=False)
class FSM:
    """Threadsafe Finite State Machine."""
    env: StateEnvironment
    previous_state: type[FSMState]
    requested_state: type[FSMState]
    state: type[FSMState]
    error_state: type[FSMState]
    lock: threading.RLock
    cond: threading.Condition
    selector: InterruptibleSelector
    params: ConnectParams
    _state_changed: bool
    _state_requested: bool
    _state_data: StateData

    def __init__(self, env: StateEnvironment, init_state: type[FSMState], error_state: type[FSMState]) -> None:
        self.env = env
        self.previous_state = init_state
        self.requested_state = init_state
        self.state = init_state
        self.error_state = error_state
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.selector = InterruptibleSelector(self.lock)
        self.params = ConnectParams()
        self._state_changed = True
        self._state_requested = False
        self._state_data = StateData()

    def set_params(self, params: ConnectParams) -> None:
        """Set the connection parameters."""
        with self.lock:
            self.params = params

    def get_state(self) -> type[FSMState]:
        """Get the current state."""
        with self.lock:
            return self.state

    def wait_for_state(self, states: Sequence[type[FSMState]], timeout: float | None = None) -> bool:
        """Wait for a specific state to be reached.

        :return: True if the state is reached, False if the timeout is reached."""
        with self.cond:
            if self.state in states:
                return True
            return self.cond.wait_for(lambda: self.state in states, timeout)

    def change_state(self, state: type[FSMState]) -> None:
        """Change to a new state.

        This method must only be called from within a state."""
        with self.cond:
            if state is self.state:
                return
            if state not in self.state.transitions_to:
                raise InvalidStateError(f"Cannot transition from {self.state.__name__} to {state.__name__}")
            self.previous_state = self.state
            self.state = state
            self._state_changed = True
            self.cond.notify_all()

    def request_state(self, state: type[FSMState]) -> None:
        """Request a state change from outside the FSM."""
        with self.cond:
            if state not in self.state.transitions_to or self.state not in state.can_request_from:
                logger.debug("Ignoring invalid request to change from %s to %s", self.state.__name__, state.__name__)
                return
            logger.debug("Requesting state change from %s to %s", self.state.__name__, state.__name__)
            self.requested_state = state
            self._state_requested = True
            self.cond.notify_all()

    def loop_once(self, max_wait: float | None = 0.0) -> bool:
        """Do the current state.

        State transition will be run if needed.

        :return: True if the state is finished and the calling thread should wait for a change."""
        try:
            with self.cond:
                # Consume state change requests.
                if self._state_requested:
                    if self.requested_state not in self.state.transitions_to or self.state not in self.requested_state.can_request_from:
                        logger.debug("Ignoring invalid request to change from %s to %s", self.state.__name__, self.requested_state.__name__)
                        self._state_requested = False
                        self.requested_state = self.state
                    else:
                        logger.debug("Handling request to change from %s to %s", self.state.__name__, self.requested_state.__name__)
                        self.previous_state = self.state
                        self.state = self.requested_state
                        self._state_requested = False
                        self._state_changed = True
                # Consume state changes, either from a state or from a request.
                if self._state_changed:
                    self._state_changed = False
                    logger.debug("Entering state %s", self.state.__name__)
                    self.state.enter(self, self._state_data, self.env, self.params)
                    return False  # Run the state on the next loop, unless it has changed.

            # Run the state.  Do not run this while holding the lock, as it may block.
            return self.state.handle(self, self._state_data, self.env, self.params, max_wait)
        except Exception:
            # If there is an unhandled exception anywhere in the loop, try to go to the error state.
            with self.cond:
                if self.state != self.error_state:
                    if self.error_state not in self.state.transitions_to:
                        logger.exception("Unhandled exception in FSM loop, cannot transition to %s, exploding", self.error_state.__name__)
                        raise
                    logger.exception("Unhandled exception in FSM loop, going to %s", self.error_state.__name__)
                    self.previous_state = self.state
                    self.state = self.error_state
                    self._state_changed = True
                    self._state_requested = False
                    self.cond.notify_all()
            raise

    def loop_until_state(self, targets: Sequence[type[FSMState]], timeout: float | None = None) -> bool:
        """Run the state machine until a specific state(s) has been entered.

        :return: True if a target state is reached, False if another final state was finished."""
        to = Timeout(timeout)
        while True:
            state_done = self.loop_once(max_wait=to.get_timeout())
            if to.exceeded():
                return False
            with self.cond:
                if self.state in targets and not self._state_changed:
                    # We are in a target state and the state has been entered.
                    return True
                if state_done and not self._state_requested and not self._state_changed:
                    if not self.state.transitions_to:
                        # The state is final and finished, we are done.
                        return False
                    # State is finished, wait for a change.
                    self.cond.wait(to.get_timeout())


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
