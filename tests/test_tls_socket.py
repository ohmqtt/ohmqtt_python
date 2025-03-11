import socket

from ohmqtt.tls_socket import TLSSocket


def test_tls_socket_loopback(loopback_tls_socket, ssl_client_context):
    loopback_tls_socket.test_do_handshake()
    context = ssl_client_context(loopback_tls_socket.cert_pem)
    with TLSSocket(loopback_tls_socket.mocksock, "localhost", context) as sock:
        sock.do_handshake()
        loopback_tls_socket.test_sendall(b"hello")
        assert sock.recv(5) == b"hello"
        sock.sendall(b"world")
        assert loopback_tls_socket.test_recv(5) == b"world"

        loopback_tls_socket.test_sendall(b"buffered")
        buf = bytearray(8)
        assert sock.recv_into(buf) == 8
        assert buf == b"buffered"
        
        assert sock.fileno() == sock.ssock.fileno()
        sock.shutdown(socket.SHUT_RDWR)


def test_tls_socket_default_context(loopback_tls_socket):
    # Construct with default context, but don't test operations.
    sock = TLSSocket(loopback_tls_socket.mocksock, "localhost")
