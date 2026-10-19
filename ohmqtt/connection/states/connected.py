from __future__ import annotations

import ssl
from typing import cast, get_args, Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .closing import ClosingState
from ..decoder import ClosedSocketError
from ..types import ConnectParams, ReceivablePacketT, StateData, StateEnvironment
from ..wslib import OpCode, frame_ws_data, WebsocketError
from ...error import MQTTError
from ...logger import get_logger
from ...mqtt_spec import MQTTPacketType, MQTTReasonCode
from ...packet import decode_packet, MQTTPacket, MQTTPingReqPacket, MQTTPingRespPacket

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("connection.states.connected")


class ConnectedState(FSMState):
    """Connected to the broker. Full duplex messaging in this state."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.keepalive.mark_init()
        env.packet_buffer.clear()
        state_data.write_buffer.clear()
        state_data.write_buffer_has_mqtt = False
        assert state_data.connack is not None, "Got to ConnectedState without a CONNACK"
        state_data.open_called = True
        try:
            env.packet_callback(state_data.connack)
        except MQTTError as exc:
            logger.error("There was a problem with the CONNACK from broker, closing connection: %s", exc)
            state_data.disconnect_rc = exc.reason_code
            env.connect_error = exc
            fsm.change_state(ClosedState)
            return
        state_data.was_connected = True

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        if state_data.keepalive.should_close():
            logger.error("Keepalive timeout, closing socket")
            fsm.change_state(ClosedState)
            return True

        if state_data.keepalive.should_send_ping():
            logger.debug("---> PING")
            env.packet_buffer.append(MQTTPingReqPacket())
            state_data.keepalive.mark_ping()

        readable, writable = cls.wait_for_io(fsm, state_data, env, max_wait)

        cls.fill_write_buffer(state_data, env, params)

        try:
            if writable:
                sent = state_data.sock.send(state_data.write_buffer)
                del state_data.write_buffer[:sent]
                # WebSocket control frames don't count as MQTT traffic for keep alive.
                if state_data.write_buffer_has_mqtt:
                    state_data.keepalive.mark_send()
                    state_data.write_buffer_has_mqtt = bool(state_data.write_buffer)
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
            pass
        except OSError as exc:
            logger.error("MQTT connection was closed: %s", exc)
            fsm.change_state(ClosedState)
            return True

        # Read one incoming packet at a time.
        try:
            if readable:
                cls.read_packet(fsm, state_data, env, params)
        except ClosedSocketError:
            logger.debug("Connection closed")
            fsm.change_state(ClosedState)
            return True
        except WebsocketError as exc:
            logger.error("WebSocket protocol error while connected: %s", exc)
            fsm.change_state(ClosedState)
            return True
        except MQTTError as exc:
            logger.error("There was a problem with data from broker, closing connection: %s", exc)
            state_data.disconnect_rc = exc.reason_code
            fsm.change_state(ClosedState)
            return True

        return False

    @classmethod
    def fill_write_buffer(cls, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        """Batch encode outgoing packets until we have at least 64kB of data or the queue is drained."""
        while env.packet_buffer and len(state_data.write_buffer) < 0xffff:
            packet = env.packet_buffer.popleft()
            if params.address.is_websocket():
                ws_frame = frame_ws_data(OpCode.BINARY, packet.encode())
                state_data.write_buffer.extend(ws_frame)
            else:
                state_data.write_buffer.extend(packet.encode())
            state_data.write_buffer_has_mqtt = True

    @classmethod
    def wait_for_io(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, max_wait: float | None) -> tuple[bool, bool]:
        """Wait until the socket is readable, or writable if there is data to send.

        :return: A tuple of (readable, writable)."""
        # Decrypted data buffered in the SSL socket doesn't make the socket readable.
        tls_pending = isinstance(state_data.sock, ssl.SSLSocket) and state_data.sock.pending() > 0
        with fsm.selector:
            timeout = 0 if tls_pending else state_data.keepalive.get_next_timeout(max_wait)
            write_check = bool(state_data.write_buffer or env.packet_buffer)
            readable, writable = fsm.selector.select(read=True, write=write_check, timeout=timeout)
        return readable or tls_pending, writable

    @classmethod
    def pull_tcp(cls, state_data: StateData) -> MQTTPacket | None:
        """Read an incoming packet from the TCP socket.

        Returns an MQTTPacket if one was read, or None if no complete MQTT packet was available."""
        return state_data.decoder.decode(state_data.sock)

    @classmethod
    def pull_ws(cls, state_data: StateData) -> MQTTPacket | None:
        """Read an incoming packet from the WebSocket.

        Non-data WebSocket frames (PING, PONG, CLOSE) are handled here.

        Returns an MQTTPacket if one was read, or None if no complete MQTT packet was available."""
        decode_result = state_data.ws_decoder.decode(state_data.sock)
        if decode_result is None:
            # No complete WebSocket frame available yet.
            return None
        opcode, payload = decode_result
        if opcode == OpCode.PING:
            logger.debug("<--- WEBSOCKET PING PONG --->")
            pong_frame = frame_ws_data(OpCode.PONG, payload)
            state_data.write_buffer.extend(pong_frame)
            return None
        if opcode == OpCode.PONG:
            logger.debug("<--- WEBSOCKET PONG")
            return None
        if opcode == OpCode.CLOSE:
            raise ClosedSocketError("WebSocket connection closed by peer")
        if opcode != OpCode.BINARY:
            raise WebsocketError(f"Unexpected WebSocket opcode {opcode.name} while connected")
        return decode_packet(payload)

    @classmethod
    def read_packet(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        """Called by the underlying SocketWrapper when the socket is ready to read.

        Incrementally reads and decodes a packet from the socket.
        Complete packets are passed up to the read callback."""
        if params.address.is_websocket():
            packet = cls.pull_ws(state_data)
        else:
            packet = cls.pull_tcp(state_data)
        if packet is None:
            # Incomplete or handled internally.
            return

        # Ping requests and responses are handled at this layer.
        if packet.packet_type == MQTTPacketType.PINGRESP:
            logger.debug("<--- PONG")
            state_data.keepalive.mark_pong()
        elif packet.packet_type == MQTTPacketType.PINGREQ:
            logger.debug("<--- PING PONG --->")
            env.packet_buffer.append(MQTTPingRespPacket())
        elif packet.packet_type == MQTTPacketType.DISCONNECT:
            logger.debug("<--- %s", packet)
            logger.info("Broker sent DISCONNECT, closing connection")
            fsm.change_state(ClosingState)
        elif not isinstance(packet, get_args(ReceivablePacketT)):
            # To cast later, we must handle the exceptional cases at runtime.
            raise MQTTError("Unexpected packet type", reason_code=MQTTReasonCode.ProtocolError)
        else:
            # All other packets are passed to the read callback.
            env.packet_callback(cast(ReceivablePacketT, packet))
