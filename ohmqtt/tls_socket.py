import socket
import ssl


class TLSSocket:
    """TLS socket wrapper."""
    def __init__(self, sock: socket.socket, server_hostname: str, context: ssl.SSLContext | None = None):
        if context is None:
            context = ssl.create_default_context()
        self.context = context
        self.ssock = self.context.wrap_socket(sock, server_hostname=server_hostname, do_handshake_on_connect=False)

    def __enter__(self) -> "TLSSocket":
        return self
    
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def do_handshake(self) -> None:
        self.ssock.do_handshake()

    def close(self) -> None:
        self.ssock.close()

    def fileno(self) -> int:
        return self.ssock.fileno()

    def getblocking(self) -> bool:
        return self.ssock.getblocking()
    
    def recv(self, bufsize: int) -> bytes:
        return self.ssock.recv(bufsize)

    def recv_into(self, buffer: bytearray) -> int:
        return self.ssock.recv_into(buffer)
    
    def send(self, data: bytes) -> int:
        return self.ssock.send(data)

    def setblocking(self, flag: bool) -> None:
        self.ssock.setblocking(flag)

    def shutdown(self, how: int) -> None:
        self.ssock.shutdown(how)
