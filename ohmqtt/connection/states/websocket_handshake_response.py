from __future__ import annotations

import base64
import hashlib
from http.client import HTTPResponse
from io import BytesIO
import socket
from typing import cast, Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .mqtt_handshake_connect import MQTTHandshakeConnectState
from ..decoder import ClosedSocketError
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.mqtt_handshake_connack")

WS_GUID: Final = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


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
            state_data.sock.recv_into(state_data.ws_handshake_buffer, 4096)
        except ClosedSocketError:
            logger.exception("Socket was closed")
            fsm.change_state(ClosedState)
            return True

        validation_result = cls.validate_response(state_data.ws_nonce, state_data.ws_handshake_buffer)
        if validation_result is True:
            fsm.change_state(MQTTHandshakeConnectState)
            return True
        if validation_result is False:
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
            expected_key = nonce + WS_GUID
            expected_key_digest = hashlib.sha1(expected_key.encode("utf-8")).digest()
            expected_key_b64 = base64.b64encode(expected_key_digest).decode("utf-8")
            if accept_key != expected_key_b64:
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
