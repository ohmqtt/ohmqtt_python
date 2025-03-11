import os
import selectors
import socket
import threading
import weakref

from .packet import decode_packet, MQTTPacket, MQTTConnectPacket, MQTTPublishPacket


class Client:
    def __init__(self, client_id: str, keep_alive: int = 60, clean_session: bool = True):
        self.client_id = client_id
        self.client_keep_alive = keep_alive
        self.clean_session = clean_session
        # Lock for the public interface.
        self._lock = threading.RLock()
        # Host of the broker.
        self._host: str | None = None
        # Port of the broker.
        self._port: int | None = None
        # Socket for the connection.
        self._sock: socket.socket | None = None
        # Thread for reading packets from the socket.
        self._read_thread: threading.Thread | None = None
        # Selector for reading from the socket and shutting down the read thread.
        self._read_selector: selectors.BaseSelector | None = None
        # Pipe for shutting down the read thread.
        self._read_pipe_r: int | None = None
        self._read_pipe_w: int | None = None

    def connect(self, host: str, port: int):
        with self._lock:
            if self._sock is not None:
                raise RuntimeError("Already connected")
            self._do_connect(host, port)

    def _do_connect(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
        except OSError as e:
            sock.close()
            raise e
        self._host = host
        self._port = port
        self._sock = sock
        self._read_pipe_r, self._read_pipe_w = os.pipe()
        self._read_selector = selectors.DefaultSelector()
        self._read_selector.register(self._sock, selectors.EVENT_READ)
        self._read_selector.register(self._read_pipe_r, selectors.EVENT_READ)
        selector_ref = weakref.ref(self._read_selector)
        self._read_thread = threading.Thread(target=self._read_loop, args=(selector_ref,), daemon=True)
        self._read_thread.start()
        connect_packet = MQTTConnectPacket(5, 3, "foo")
        self._write_packet(connect_packet)

    def disconnect(self):
        with self._lock:
            if self._sock is None:
                raise RuntimeError("Not connected")
            # Try to write disconnect here
            self._sock.close()
            os.write(self._read_pipe_w, b"\0")
            if threading.current_thread() != self._read_thread:
                self._read_thread.join()
            os.close(self._read_pipe_r)
            os.close(self._read_pipe_w)
            self._sock = None
            self._read_thread = None
            self._read_pipe_r = None
            self._read_pipe_w = None
            self._read_selector.close()
            self._read_selector = None

    def publish(self, topic: str, payload: bytes, qos: int = 0, retain: bool = False):
        with self._lock:
            packet = MQTTPublishPacket(topic, payload, qos=qos, retain=retain)
            self._write_packet(packet)

    def _write_packet(self, packet: MQTTPacket) -> None:
        try:
            if self._sock is None:
                raise RuntimeError("Not connected")
            self._sock.sendall(packet.encode())
            print("->", packet)
        except:
            raise
    
    def _read_packet(self, data: bytes) -> None:
        packet = decode_packet(data)
        print("<-", packet)

    def _read_loop(self, selector_ref: weakref.ReferenceType[selectors.BaseSelector]) -> None:
        try:
            while True:
                sel = selector_ref()
                if sel is None:
                    return
                events = sel.select(timeout=1)
                for key, _ in events:
                    if key.fileobj == self._sock:
                        data = self._sock.recv(65535)
                        if not data:
                            self.disconnect()
                            return
                        self._read_packet(data)
                    elif key.fileobj == self._read_pipe_r:
                        os.read(self._read_pipe_r, 1)
                        return
                del sel  # Ensure strong reference to sel is released
        except:
            self.disconnect()
            raise


if __name__ == "__main__":
    import time

    client = Client("test")
    client.connect("localhost", 1883)
    time.sleep(1)
    client.publish("foo", b"bar")
    time.sleep(1)
    print("descope")
    #client.disconnect()
    
