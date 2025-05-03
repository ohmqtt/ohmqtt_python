from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import ClassVar, Final, Sequence, Type

from .selector import InterruptibleSelector
from .types import ConnectParams, StateData, StateEnvironment
from ..logger import get_logger
from ..threading_lite import ConditionLite

logger: Final = get_logger("connection.fsm")


class InvalidStateError(Exception):
    """Exception raised when an operation is performed in an invalid state."""
    pass


@dataclass(slots=True, init=False)
class FSM:
    """Threadsafe Finite State Machine."""
    env: StateEnvironment
    previous_state: Type[FSMState]
    requested_state: Type[FSMState]
    state: Type[FSMState]
    lock: threading.RLock
    cond: ConditionLite
    selector: InterruptibleSelector
    params: ConnectParams
    _state_changed: bool
    _state_requested: bool
    _state_data: StateData

    def __init__(self, env: StateEnvironment, init_state: Type[FSMState]) -> None:
        self.env = env
        self.previous_state = init_state
        self.requested_state = init_state
        self.state = init_state
        self.lock = threading.RLock()
        self.cond = ConditionLite(self.lock)
        self.selector = InterruptibleSelector(self.lock)
        self.params = ConnectParams()
        self._state_changed = True
        self._state_requested = False
        self._state_data = StateData()

    def set_params(self, params: ConnectParams) -> None:
        """Set the connection parameters."""
        self.params = params

    def get_state(self) -> Type[FSMState]:
        """Get the current state."""
        with self.lock:
            return self.state

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for a state change or valid request.
        
        Returns True if a state change or request has occurred, False if the timeout is reached."""
        with self.cond:
            return self.cond.wait(timeout)

    def wait_for_state(self, states: Sequence[Type[FSMState]], timeout: float | None = None) -> bool:
        """Wait for a specific state to be reached.

        Returns True if the state is reached, False if the timeout is reached."""
        with self.cond:
            if self.state in states:
                return True
            return self.cond.wait_for(lambda: self.state in states, timeout)

    def change_state(self, state: Type[FSMState]) -> None:
        """Change to a new state.
        
        This method must only be called from within a state."""
        with self.cond:
            if state == self.state:
                return
            if state not in self.state.transitions_to:
                raise InvalidStateError(f"Cannot transition from {self.state.__name__} to {state.__name__}")
            self.previous_state = self.state
            self.state = state
            self._state_changed = True
            self.cond.notify_all()

    def request_state(self, state: Type[FSMState]) -> None:
        """Request a state change from outside the FSM."""
        with self.cond:
            if state not in self.state.transitions_to or self.state not in state.can_request_from:
                logger.debug(f"Ignoring invalid request to change from {self.state.__name__} to {self.requested_state.__name__}")
                return
            logger.debug(f"Requesting state change from {self.state.__name__} to {state.__name__}")
            self.requested_state = state
            self._state_requested = True
            self.cond.notify_all()

    def loop_once(self, block: bool = False) -> bool:
        """Do the current state.

        State transition will be run if needed.

        Returns True if the state is finished and the calling thread should wait for a change."""
        with self.cond:
            if self._state_requested:
                if self.requested_state not in self.state.transitions_to or self.state not in self.requested_state.can_request_from:
                    logger.debug(f"Ignoring invalid request to change from {self.state.__name__} to {self.requested_state.__name__}")
                    self._state_requested = False
                    self.requested_state = self.state
                else:
                    logger.debug(f"Handling request to change from {self.state.__name__} to {self.requested_state.__name__}")
                    self.previous_state = self.state
                    self.state = self.requested_state
                    self._state_requested = False
                    self._state_changed = True
            if self._state_changed:
                self._state_changed = False
                self.cond.notify_all()
                logger.debug(f"Entering state {self.state.__name__}")
                self.state.enter(self, self._state_data, self.env, self.params)
                return False  # Run the state on the next loop, unless it has changed.
            state = self.state
        # Do not hold the condition while in handle.
        return state.handle(self, self._state_data, self.env, self.params, block)

    def loop_forever(self) -> None:
        """Run the state machine.

        This method will run in a loop until the state machine is finalized.
        It will call the appropriate state methods based on the current state."""
        while True:
            try:
                state_done = self.loop_once(block=True)
                with self.cond:
                    if state_done and not self._state_changed and not self._state_requested:
                        if not self.state.transitions_to:
                            # The state is final and finished, we are done.
                            logger.debug("Shutting down loop")
                            break
                        else:
                            # State is finished, wait for a change.
                            self.cond.wait()
            except Exception:
                logger.exception("Unhandled exception in FSM loop, breaking the loop")
                raise

    def loop_until_state(self, state: Type[FSMState]) -> bool:
        """Run the state machine until a specific state has been entered.

        Return True if the state is reached, False if another final state was finished."""
        while True:
            state_done = self.loop_once(block=True)
            with self.cond:
                if not self._state_changed:
                    if self.state == state:
                        return True
                    elif state_done and not self._state_requested:
                        if not self.state.transitions_to:
                            # The state is final and finished, we are done.
                            return False
                        else:
                            # State is finished, wait for a change.
                            self.cond.wait()


class FSMState:
    """A finite state in the FSM."""
    can_request_from: ClassVar[Sequence[Type[FSMState]]] = ()
    transitions_to: ClassVar[Sequence[Type[FSMState]]] = ()

    def __init__(self) -> None:
        raise TypeError("Do not instantiate FSMStates")

    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        """Called when entering the state.

        This method must not block."""
        pass

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, block: bool) -> bool:
        """Called when handling the state.

        This method may block if block=True.

        Returns True if the state is finished."""
        return True
