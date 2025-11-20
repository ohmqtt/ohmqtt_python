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

logger: Final = get_logger("ohmqtt.connection.states.connected")


class ConnectedState(FSMState):
    """Connected to the broker. Full duplex messaging in this state."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.keepalive.mark_init()
        env.packet_buffer.clear()
        state_data.write_buffer.clear()
        assert state_data.connack is not None, "Got to ConnectedState without a CONNACK"
        env.packet_callback(state_data.connack)
        state_data.open_called = True

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

        with fsm.selector:
            timeout = state_data.keepalive.get_next_timeout(max_wait)
            write_check = bool(state_data.write_buffer or env.packet_buffer)
            readable, writable = fsm.selector.select(read=True, write=write_check, timeout=timeout)

        while env.packet_buffer:
            packet = env.packet_buffer.popleft()
            if params.address.is_websocket():
                ws_frame = frame_ws_data(OpCode.BINARY, packet.encode())
                state_data.write_buffer.extend(ws_frame)
            else:
                state_data.write_buffer.extend(packet.encode())

        if writable:
            try:
                sent = state_data.sock.send(state_data.write_buffer)
                del state_data.write_buffer[:sent]
                state_data.keepalive.mark_send()
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                pass
            except (BrokenPipeError, ConnectionResetError) as exc:
                logger.error("MQTT connection was closed: %s", exc)
                fsm.change_state(ClosedState)
                return True

        if readable:
            try:
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
    def read_packet(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> bool:
        """Called by the underlying SocketWrapper when the socket is ready to read.

        Incrementally reads and decodes a packet from the socket.
        Complete packets are passed up to the read callback.

        Returns False if an incomplete packet was read, True if a complete packet was read."""
        packet: MQTTPacket | None
        if params.address.is_websocket():
            decode_result = state_data.ws_decoder.decode(state_data.sock)
            if decode_result is None:
                # No complete WebSocket frame available yet.
                return False
            opcode, payload = decode_result
            if opcode == OpCode.PING:
                logger.debug("<--- WEBSOCKET PING PONG --->")
                pong_frame = frame_ws_data(OpCode.PONG, payload)
                state_data.write_buffer.extend(pong_frame)
                return True
            if opcode == OpCode.CLOSE:
                raise ClosedSocketError("WebSocket connection closed by peer")
            if opcode != OpCode.BINARY:
                raise WebsocketError(f"Unexpected WebSocket opcode {opcode.name} while connected")
            packet = decode_packet(payload)
        else:
            packet = state_data.decoder.decode(state_data.sock)
            if packet is None:
                # No complete packet available yet.
                return False

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
            return True
        elif not isinstance(packet, get_args(ReceivablePacketT)):
            # To cast later, we must handle the exceptional cases at runtime.
            raise MQTTError("Unexpected packet type", reason_code=MQTTReasonCode.ProtocolError)
        else:
            # All other packets are passed to the read callback.
            env.packet_callback(cast(ReceivablePacketT, packet))
        return True
