from __future__ import annotations

from http.client import HTTPResponse
from io import BytesIO
import socket
import ssl
from typing import cast, Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .mqtt_handshake_connect import MQTTHandshakeConnectState
from ..types import ConnectParams, StateData, StateEnvironment
from ..wslib import validate_handshake_key
from ...logger import get_logger

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.mqtt_handshake_connack")


class MockSocket:
    """Wrapper class to provide a makefile method for a buffer.

    This lets us use HTTPResponse on a BytesIO buffer."""
    def __init__(self, buffer: BytesIO) -> None:
        self.buffer = buffer

    def makefile(self, mode: str = "r") -> socket.SocketIO:
        return cast(socket.SocketIO, self.buffer)


class WebsocketHandshakeResponseState(FSMState):
    """Receiving Websocket handshake response from the broker."""
    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("Websocket handshake response keepalive timeout")
            fsm.change_state(ClosedState)
            return True

        try:
            state_data.ws_handshake_buffer.extend(state_data.sock.recv(0xffff))
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
            logger.debug("Websocket handshake response recv would block, waiting for readable")

        validation_result = cls.validate_response(state_data.ws_nonce, state_data.ws_handshake_buffer)
        if validation_result is True:
            state_data.ws_handshake_buffer.clear()
            fsm.change_state(MQTTHandshakeConnectState)
            return True
        if validation_result is False:
            state_data.ws_handshake_buffer.clear()
            fsm.change_state(ClosedState)
            return True

        # Incomplete response, wait for more data.
        with fsm.selector:
            timeout = state_data.timeout.get_timeout(max_wait)
            fsm.selector.select(read=True, timeout=timeout)
        return False

    @classmethod
    def validate_response(cls, nonce: str, buffer: bytearray) -> bool | None:
        """Validate the websocket handshake response.

        Return True if valid, False if invalid, None if incomplete."""
        try:
            bio = BytesIO(buffer)
            sio = cast(socket.socket, MockSocket(bio))
            response = HTTPResponse(sio)
            response.begin()
            if response.status != 101:
                logger.error("Websocket handshake failed with status %d", response.status)
                return False
            accept_key = response.getheader("Sec-WebSocket-Accept")
            if accept_key is None or not validate_handshake_key(nonce, accept_key):
                logger.error("Websocket handshake failed: invalid Sec-WebSocket-Accept")
                return False
            protocol = response.getheader("Sec-WebSocket-Protocol")
            if protocol != "mqtt":
                logger.error("Websocket handshake failed: invalid Sec-WebSocket-Protocol")
                return False
            return True
        except Exception:
            logger.debug("Incomplete websocket handshake response, waiting for more data")
            return None
