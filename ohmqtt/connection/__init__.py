from __future__ import annotations

import threading
from typing import Final

from .address import Address as Address
from .fsm import FSM
from .fsm import InvalidStateError as InvalidStateError
from .states import (
    ClosingState,
    ClosedState,
    ConnectingState,
    ConnectedState,
    ReconnectWaitState,
    ShutdownState,
)
from .types import ConnectParams as ConnectParams
from .types import ConnectionCloseCallback as ConnectionCloseCallback
from .types import ConnectionOpenCallback as ConnectionOpenCallback
from .types import ConnectionReadCallback as ConnectionReadCallback
from .types import ConnectionCloseCondition as ConnectionCloseCondition
from .types import StateEnvironment
from ..logger import get_logger

logger: Final = get_logger("connection")


class Connection:
    """Interface for the MQTT connection."""
    __slots__ = ("fsm", "_thread")

    def __init__(
        self,
        close_callback: ConnectionCloseCallback,
        open_callback: ConnectionOpenCallback,
        read_callback: ConnectionReadCallback,
    ) -> None:
        state_env = StateEnvironment(
            close_callback=close_callback,
            open_callback=open_callback,
            read_callback=read_callback,
        )
        self.fsm = FSM(env=state_env, init_state=ClosedState)

        self._thread: threading.Thread | None = None

    def __enter__(self) -> Connection:
        """Start the connection loop in a separate thread."""
        self.start_loop()
        return self

    def __exit__(self, *args: object) -> None:
        """Shutdown the connection."""
        self.shutdown()

    def can_send(self) -> bool:
        """Check if the connection is in a state where data can be sent."""
        with self.fsm.lock:
            state = self.fsm.get_state()
            return state is ConnectedState

    def send(self, data: bytes) -> None:
        """Send data to the connection."""
        logger.debug(f"Sending {len(data)} bytes")
        with self.fsm.lock:
            if not self.can_send():
                state = self.fsm.get_state()
                raise InvalidStateError(f"Cannot send data in state {state.__name__}")
            self.fsm.env.write_buffer.extend(data)
            self.fsm.selector.interrupt()

    def connect(self, params: ConnectParams) -> None:
        """Connect to the MQTT broker."""
        with self.fsm.lock:
            self.fsm.set_params(params)
            self.fsm.request_state(ConnectingState)
            self.fsm.selector.interrupt()

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        with self.fsm.lock:
            self.fsm.request_state(ClosingState)
            self.fsm.selector.interrupt()

    def shutdown(self) -> None:
        """Shutdown the connection."""
        with self.fsm.lock:
            self.fsm.request_state(ShutdownState)
            self.fsm.selector.interrupt()

    def is_connected(self) -> bool:
        """Check if the connection is established."""
        return self.fsm.get_state() == ConnectedState

    def wait_for_connect(self, timeout: float | None = None) -> bool:
        """Wait for the connection to be established.

        Returns True if the connection is established, False if the timeout is reached."""
        return self.fsm.wait_for_state((ConnectedState,), timeout)

    def wait_for_disconnect(self, timeout: float | None = None) -> bool:
        """Wait for the connection to be closed.

        Returns True if the connection is closed, False if the timeout is reached."""
        return self.fsm.wait_for_state((ClosedState, ShutdownState, ReconnectWaitState), timeout)

    def loop_once(self) -> None:
        """Run a single iteration of the state machine, without blocking."""
        self.fsm.loop_once()

    def loop_forever(self) -> None:
        """Run the state machine until the connection is closed."""
        self.fsm.loop_forever()

    def loop_until_connected(self) -> bool:
        """Run the state machine until the connection is established.

        Returns True if the connection is established, False if the timeout is reached."""
        self.fsm.loop_until_state(ConnectedState)

    def start_loop(self) -> None:
        """Start the state machine in a separate thread."""
        if self._thread is not None:
            raise RuntimeError("Connection loop already started")
        self._thread = threading.Thread(target=self.fsm.loop_forever, daemon=True)
        self._thread.start()

    def is_alive(self) -> bool:
        """Check if the connection thread is alive."""
        return self._thread is not None and self._thread.is_alive()

    def join(self, timeout: float | None = None) -> bool:
        """Wait for the connection thread to finish.

        Returns True if the thread has finished, False if the timeout is reached."""
        if self._thread is None:
            return True
        self._thread.join(timeout)
        return not self._thread.is_alive()
