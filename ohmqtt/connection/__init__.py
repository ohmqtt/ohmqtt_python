from __future__ import annotations

from typing import Final

from .address import Address as Address
from .fsm import FSM
from .fsm import InvalidStateError as InvalidStateError
from .handlers import MessageHandlers as MessageHandlers
from .states import (
    ClosingState,
    ClosedState,
    ConnectingState,
    ConnectedState,
    ReconnectWaitState,
    ShutdownState,
)
from .types import ConnectParams as ConnectParams
from .types import StateEnvironment, ReceivablePacketT, SendablePacketT
from ..error import MQTTError
from ..logger import get_logger

logger: Final = get_logger("connection")


class Connection:
    """Interface for the MQTT connection."""
    __slots__ = ("_handlers", "fsm")

    def __init__(self, handlers: MessageHandlers) -> None:
        state_env = StateEnvironment(packet_callback=self.handle_packet)
        self.fsm = FSM(env=state_env, init_state=ClosedState, error_state=ShutdownState)
        self._handlers = handlers

    def handle_packet(self, packet: ReceivablePacketT) -> None:
        """Handle incoming packets by routing them to registered handlers."""
        logger.debug("<--- %s", packet)
        exceptions = self._handlers.handle(packet)
        if exceptions:
            if any(True for exc in exceptions if isinstance(exc, MQTTError)):
                # If there is an MQTTError, raise it.
                raise next(exc for exc in exceptions if isinstance(exc, MQTTError))
            # Otherwise. raise the first exception.
            raise exceptions[0]

    def can_send(self) -> bool:
        """Check if the connection is in a state where data can be sent.

        :return: True if the connection is in a state where data can be sent, False otherwise."""
        with self.fsm.lock:
            state = self.fsm.get_state()
            return state is ConnectedState

    def send(self, packet: SendablePacketT) -> None:
        """Send data to the connection.

        :raises InvalidStateError: If the connection is not in a state where data can be sent."""
        with self.fsm.lock:
            if not self.can_send():
                state = self.fsm.get_state()
                raise InvalidStateError(f"Cannot send data in state {state.__name__}")
            logger.debug("---> %s", packet)
            data = packet.encode()
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

        :return: True if the connection is established, False if the timeout is reached."""
        return self.fsm.wait_for_state((ConnectedState,), timeout)

    def wait_for_disconnect(self, timeout: float | None = None) -> bool:
        """Wait for the connection to be closed.

        :return: True if the connection is closed, False if the timeout is reached."""
        return self.fsm.wait_for_state((ClosedState, ShutdownState, ReconnectWaitState), timeout)

    def wait_for_shutdown(self, timeout: float | None = None) -> bool:
        """Wait for the connection to be closed and finalized.

        :return: True if the connection is closed, False if the timeout is reached."""
        return self.fsm.wait_for_state((ShutdownState,), timeout)

    def loop_once(self, max_wait: float | None = 0.0) -> None:
        """Run a single iteration of the state machine.

        If max_wait is None, wait indefinitely. Otherwise, wait for the specified time."""
        self.fsm.loop_once(max_wait)

    def loop_forever(self) -> None:
        """Run the state machine until the connection is closed."""
        self.fsm.loop_until_state((ShutdownState,))

    def loop_until_connected(self, timeout: float | None = None) -> bool:
        """Run the state machine until the connection is established.

        :return: True if the connection is established, False otherwise."""
        return self.fsm.loop_until_state((ConnectedState,), timeout)
