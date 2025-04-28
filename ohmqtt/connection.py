from __future__ import annotations

from dataclasses import dataclass, field
import select
import socket
import ssl
import threading
from typing import Callable, cast, Final
from types import TracebackType

from .decoder import IncrementalDecoder
from .error import MQTTError
from .keepalive import KeepAlive
from .logger import get_logger
from .mqtt_spec import MQTTPacketType, MQTTReasonCode
from .packet import (
    MQTTPacket,
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTDisconnectPacket,
    PING,
    PONG,
)
from .property import MQTTPropertyDict

logger: Final = get_logger("connection")

ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[MQTTConnAckPacket], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


STATE_SHUTDOWN: Final = 0
STATE_CLOSING: Final = 1
STATE_CLOSED: Final = 2
STATE_CONNECTING: Final = 3
STATE_CONNECT: Final = 4


_InitAddress: Final[tuple[str, int]] = ("", -1)


class ConnectionCloseCondition(Exception):
    """Any exception which should cause the socket to close."""
    pass


class KeepAliveTimeout(Exception):
    """This exception means a PINGRESP has not been received within the interval."""
    pass


def _get_socket() -> socket.socket:
    """Helper function to get a socket.

    This mostly helps us mock sockets in tests."""
    return socket.socket(socket.AF_INET, socket.SOCK_STREAM)


@dataclass(slots=True, match_args=True, frozen=True)
class ConnectParams:
    host: str
    port: int
    client_id: str = ""
    reconnect_delay: float = 0.0
    keepalive_interval: int = 0
    tcp_nodelay: bool = True
    use_tls: bool = False
    tls_context: ssl.SSLContext = field(default_factory=ssl.create_default_context)
    tls_hostname: str = ""
    protocol_version: int = 5
    clean_start: bool = False
    connect_properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())


class Connection(threading.Thread):
    """Non-blocking socket wrapper with TLS and application keepalive support."""
    __slots__ = (
        "_params",
        "_address",
        "_close_callback",
        "_open_callback",
        "_read_callback",
        "_write_buffer",
        "_write_buffer_lock",
        "_interrupt_r",
        "_interrupt_w",
        "_in_read",
        "_state",
        "_decoder",
        "_keepalive",
        "_state_cond",
    )

    def __init__(
        self,
        close_callback: ConnectionCloseCallback,
        open_callback: ConnectionOpenCallback,
        read_callback: ConnectionReadCallback,
    ) -> None:
        super().__init__(daemon=True)
        self._close_callback = close_callback
        self._open_callback = open_callback
        self._read_callback = read_callback
        self._params = ConnectParams(*_InitAddress)
        self._address = _InitAddress

        self._state = STATE_CLOSED
        self._write_buffer = bytearray()
        self._write_buffer_lock = threading.Lock()
        self._interrupt_r, self._interrupt_w = socket.socketpair()
        self._in_read = False
        self._state_cond = threading.Condition()
        self._decoder = IncrementalDecoder()
        self._keepalive = KeepAlive()

    def __enter__(self) -> Connection:
        self.start()
        return self

    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.shutdown()

    def connect(self, params: ConnectParams) -> None:
        """Connect to the broker.

        This method is non-blocking and will return immediately. The connection will be established in the background."""
        with self._state_cond:
            if self._state >= STATE_CONNECTING:
                raise RuntimeError("Already connecting or connected")
            self._params = params
            self._keepalive.keepalive_interval = params.keepalive_interval
            self._address = params.host, params.port
            self._goto_state(STATE_CONNECTING)

    def wait_for_connect(self, timeout: float | None = None) -> None:
        """Wait for the connection to be established.

        This method will block until the connection is established or the timeout is reached."""
        with self._state_cond:
            self._state_cond.wait_for(lambda: self._state >= STATE_CONNECT, timeout=timeout)
            if self._state < STATE_CONNECT:
                raise TimeoutError("Socket did not connect in time")

    def disconnect(self) -> None:
        """Close the connection.

        This method does not guarantee pending reads or writes will be completed."""
        self._address = _InitAddress
        self._goto_state(STATE_CLOSING)

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the connection to be closed.

        This method will block until the connection is closed or the timeout is reached."""
        with self._state_cond:
            self._state_cond.wait_for(lambda: self._state <= STATE_CLOSED, timeout=timeout)
            if self._state > STATE_CLOSED:
                raise TimeoutError("Socket did not close in time")
            
    def is_connected(self) -> bool:
        """Check if the connection is established.

        This method will return True if the connection is established, False otherwise."""
        return self._state >= STATE_CONNECT

    def shutdown(self) -> None:
        """Shutdown the connection, finalizing the instance.

        This method does not guarantee pending reads or writes will be completed."""
        self._goto_state(STATE_SHUTDOWN)

    def send(self, data: bytes) -> None:
        """Write data to the broker."""
        if self._state < STATE_CONNECT:
            return
        with self._write_buffer_lock:
            do_interrupt = not self._in_read and not self._write_buffer
            self._write_buffer.extend(data)
        if do_interrupt:
            self._interrupt()

    def _goto_state(self, state: int) -> None:
        """Set the state of the connection and wake up any sleeping thread states.

        This method must only be called from threads other than the Connection thread
            (the public interface of this class, other than run)."""
        with self._state_cond:
            self._state = state
            self._state_cond.notify_all()
            self._interrupt()

    def _try_to_send_disconnect(self, sock: socket.socket | ssl.SSLSocket, reason_code: int) -> None:
        """Try to send a DISCONNECT packet to the broker and close the socket.

        If the socket is not ready to write, we will not wait for it."""
        disconnect_packet = MQTTDisconnectPacket(reason_code=reason_code)
        try:
            sock.send(disconnect_packet.encode())
            logger.debug(f"---> {str(disconnect_packet)}")
        except (BlockingIOError, ssl.SSLWantWriteError, OSError):
            pass

    def _interrupt(self) -> None:
        """Interrupt select to wake up the thread."""
        self._interrupt_w.send(b"\x00")

    def _handshake_loop(self, sock: socket.socket) -> ssl.SSLSocket:
        """Run the TLS handshake in a loop until it is complete."""
        sock = self._params.tls_context.wrap_socket(sock, server_hostname=self._params.tls_hostname, do_handshake_on_connect=False)
        while self._state == STATE_CONNECTING:
            try:
                sock.do_handshake()
                return sock
            except ssl.SSLWantReadError:
                select.select([sock, self._interrupt_r], [], [])
            except ssl.SSLWantWriteError:
                select.select([self._interrupt_r], [sock], [])
        raise ConnectionCloseCondition("Handshake loop did not complete")

    def _try_write(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Try to flush the write buffer to the socket."""
        try:
            with self._write_buffer_lock:
                sent = sock.send(self._write_buffer)
                self._keepalive.mark_send()
                del self._write_buffer[:sent]
        except ssl.SSLWantWriteError:
            pass

    def _try_read(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Try to read data from the socket."""
        self._in_read = True
        try:
            self._read_packet(sock)
        except ssl.SSLWantReadError:
            pass
        finally:
            self._in_read = False

    def _read_packet(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Called by the underlying SocketWrapper when the socket is ready to read.
        
        Incrementally reads and decodes a packet from the socket.
        Complete packets are passed up to the read callback."""
        packet = self._decoder.decode(sock)
        if packet is None:
            # No complete packet available yet.
            return

        # Ping requests and responses are handled at this layer.
        if packet.packet_type == MQTTPacketType["PINGRESP"]:
            self._keepalive.mark_pong()
            logger.debug("<--- PONG")
        elif packet.packet_type == MQTTPacketType["PINGREQ"]:
            self.send(PONG)
            logger.debug("<--- PING PONG --->")
        elif packet.packet_type == MQTTPacketType["CONNACK"]:
            packet = cast(MQTTConnAckPacket, packet)
            if "ServerKeepAlive" in packet.properties:
                # Override the keepalive interval with the server's value.
                keepalive_interval = packet.properties["ServerKeepAlive"]
                self._keepalive.keepalive_interval = keepalive_interval
                logger.debug(f"Keepalive interval set by server to {keepalive_interval} seconds")
            with self._state_cond:
                if self._state == STATE_CONNECTING:
                    self._open_callback(packet)
                    self._state = STATE_CONNECT
                    self._state_cond.notify_all()
            logger.debug(f"<--- {str(packet)}")
        else:
            # All other packets are passed to the read callback.
            self._read_callback(packet)

    def _check_keepalive(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Check if we should send a ping or close the connection."""
        if self._keepalive.should_close():
            raise KeepAliveTimeout("Keepalive timeout")
        if self._keepalive.should_send_ping():
            self.send(PING)
            logger.debug("---> PING")
            self._keepalive.mark_ping()

    def _check_connect(self) -> bool:
        """Check if we should connect to the server.

        Raises ConnectionCloseCondition if the thread should be shutdown instead."""
        if self._state == STATE_SHUTDOWN:
            raise ConnectionCloseCondition("Shutting down")
        return bool(self._state == STATE_CONNECTING and self._address != _InitAddress)

    def run(self) -> None:
        while self._state > STATE_SHUTDOWN:
            was_open = False
            sent_disconnect = False
            sock: socket.socket | ssl.SSLSocket = _get_socket()
            try:
                with self._state_cond:
                    self._state_cond.wait_for(self._check_connect)
                    address = self._address

                if self._params.tcp_nodelay:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.connect(address)
                sock.setblocking(False)

                if self._params.use_tls:
                    sock = self._handshake_loop(sock)

                # Send CONNECT
                connect_packet = MQTTConnectPacket(
                    client_id=self._params.client_id,
                    protocol_version=self._params.protocol_version,
                    clean_start=self._params.clean_start,
                    keep_alive=self._params.keepalive_interval,
                    properties=self._params.connect_properties,
                )
                self._write_buffer.extend(connect_packet.encode())
                logger.debug(f"---> {str(connect_packet)}")

                self._keepalive.mark_init()
                was_open = True

                while self._state >= STATE_CONNECTING:
                    next_timeout = self._keepalive.get_next_timeout()
                    write_check = (sock,) if self._write_buffer else tuple()
                    readable, writable, _ = select.select([sock, self._interrupt_r], write_check, [], next_timeout)

                    if sock in writable:
                        self._try_write(sock)

                    if sock in readable:
                        self._try_read(sock)

                    self._check_keepalive(sock)

                    if self._interrupt_r in readable:
                        self._interrupt_r.recv(1024)

            except MQTTError as exc:
                logger.error(f"There was a problem with data from broker, closing connection: {exc}")
                self._try_to_send_disconnect(sock, exc.reason_code)
                sent_disconnect = True
            except ConnectionCloseCondition as exc:
                logger.debug(f"Closing socket: {exc}")
            except KeepAliveTimeout:
                logger.error("Keepalive timeout, closing socket")
                # Do not try to send a DISCONNECT in this case.
                sent_disconnect = True
            except Exception:
                logger.exception("Unhandled error in socket read thread, shutting down")
                self._state = STATE_SHUTDOWN
            finally:
                self._state = min(self._state, STATE_CLOSED)
                with self._state_cond:
                    # Wake up wait_for_disconnect.
                    self._state_cond.notify_all()
                if was_open:
                    try:
                        self._close_callback()
                    except Exception:
                        logger.exception("Error while calling close callback")
                    if not sent_disconnect:
                        self._try_to_send_disconnect(sock, MQTTReasonCode["NormalDisconnection"])
                sock.close()
                self._decoder.reset()
                self._write_buffer.clear()
                if self._state > STATE_SHUTDOWN and self._params.reconnect_delay > 0:
                    with self._state_cond:
                        self._state_cond.wait(timeout=self._params.reconnect_delay)
                        if self._address == _InitAddress:
                            logger.debug("Reconnect cancelled")
                            break
        logger.debug("Shutdown complete")
