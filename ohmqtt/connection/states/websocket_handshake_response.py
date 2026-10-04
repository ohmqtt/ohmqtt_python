from __future__ import annotations

from http.client import HTTPException, HTTPResponse
from io import BytesIO
import socket
import ssl
from typing import cast, Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .mqtt_handshake_connect import MQTTHandshakeConnectState
from ..types import ConnectParams, StateData, StateEnvironment
from ..wslib import generate_handshake_key, WebsocketError
from ...logger import get_logger

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("connection.states.websocket_handshake_response")


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
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.ws_handshake_buffer.clear()

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("Websocket handshake response keepalive timeout")
            env.connect_error = TimeoutError("Websocket handshake timed out")
            fsm.change_state(ClosedState)
            return True

        try:
            data = state_data.sock.recv(0xffff)
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
            logger.debug("Websocket handshake response recv would block, waiting for readable")
        except OSError as exc:
            logger.error("Websocket handshake response recv failed, closing connection: %s", exc)
            env.connect_error = exc
            fsm.change_state(ClosedState)
            return True
        else:
            if not data:
                logger.error("Websocket connection closed by broker during handshake")
                env.connect_error = ConnectionError("Websocket connection closed by broker during handshake")
                fsm.change_state(ClosedState)
                return True
            state_data.ws_handshake_buffer.extend(data)

        header_end = state_data.ws_handshake_buffer.find(b"\r\n\r\n")
        if header_end == -1:
            # Incomplete response, wait for more data.
            with fsm.selector:
                timeout = state_data.timeout.get_timeout(max_wait)
                fsm.selector.select(read=True, timeout=timeout)
            return False
        header_end += 4
        response = state_data.ws_handshake_buffer[:header_end]
        extra = state_data.ws_handshake_buffer[header_end:]
        state_data.ws_handshake_buffer.clear()
        if not cls.validate_response(state_data.ws_nonce, response):
            env.connect_error = WebsocketError("Websocket handshake failed")
            fsm.change_state(ClosedState)
            return True
        # The broker may send frames right behind the response.
        state_data.ws_decoder.feed(extra)
        fsm.change_state(MQTTHandshakeConnectState)
        return True

    @classmethod
    def validate_response(cls, nonce: str, buffer: bytearray) -> bool:
        """Validate a complete websocket handshake response."""
        try:
            bio = BytesIO(buffer)
            sio = cast(socket.socket, MockSocket(bio))
            response = HTTPResponse(sio)
            response.begin()
        except HTTPException as exc:
            logger.error("Websocket handshake failed: malformed response: %r", exc)
            return False
        if response.status != 101:
            logger.error("Websocket handshake failed with status %d", response.status)
            return False
        accept_key = response.getheader("Sec-WebSocket-Accept")
        expected_key = generate_handshake_key(nonce)
        if accept_key is None or accept_key != expected_key:
            logger.error("Websocket handshake failed: invalid Sec-WebSocket-Accept")
            return False
        protocol = response.getheader("Sec-WebSocket-Protocol")
        if protocol != "mqtt":
            logger.error("Websocket handshake failed: invalid Sec-WebSocket-Protocol")
            return False
        return True
