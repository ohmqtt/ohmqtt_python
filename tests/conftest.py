import os
import pytest
import socket
import ssl
import tempfile
import threading
import yaml

from tests.util.selfsigned import generate_selfsigned_cert


@pytest.fixture
def test_data(request):
    """Load test data from a YAML file.
    
    The YAML file must be named after the test suite, and contain a mapping of test names to test data."""
    suite_name = request.module.__name__.split(".")[-1]
    test_name = request.node.name
    with open(f"tests/data/{suite_name}.yml") as f:
        data = yaml.safe_load(f)
    return data[test_name]


class LoopbackSocket:
    """A pair of connected sockets for testing.
    
    Return an instance of this class from a mock to use as a socket in tests."""
    def __init__(self):
        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.mocksock.close()
        self.testsock.close()

    def __del__(self):
        self.mocksock.close()
        self.testsock.close()

    def reset(self) -> None:
        """Reset the socket pair."""
        self.mocksock, self.testsock = socket.socketpair()
        self.testsock.settimeout(5.0)
        self.connect_calls = []

    def test_close(self) -> None:
        self.testsock.close()

    def test_sendall(self, *args, **kwargs) -> None:
        self.testsock.sendall(*args, **kwargs)

    def test_recv(self, *args, **kwargs) -> bytes:
        return self.testsock.recv(*args, **kwargs)

    def test_shutdown(self, *args, **kwargs) -> None:
        self.testsock.shutdown(*args, **kwargs)

    def close(self) -> None:
        self.mocksock.close()

    def connect(self, address) -> None:
        self.connect_calls.append(address)

    def detach(self):
        return self.mocksock.detach()

    @property
    def family(self) -> int:
        return self.mocksock.family

    def fileno(self) -> int:
        return self.mocksock.fileno()

    def getblocking(self) -> bool:
        return self.mocksock.getblocking()

    def getsockopt(self, *args, **kwargs):
        return self.mocksock.getsockopt(*args, **kwargs)

    def gettimeout(self, *args, **kwargs):
        return self.mocksock.gettimeout(*args, **kwargs)
    
    @property
    def proto(self) -> int:
        return self.mocksock.proto

    def recv(self, *args, **kwargs) -> bytes:
        return self.mocksock.recv(*args, **kwargs)

    def recv_into(self, *args, **kwargs) -> int:
        return self.mocksock.recv_into(*args, **kwargs)

    def send(self, *args, **kwargs) -> int:
        return self.mocksock.send(*args, **kwargs)

    def sendall(self, *args, **kwargs) -> None:
        self.mocksock.sendall(*args, **kwargs)

    def setblocking(self, *args, **kwargs) -> None:
        self.mocksock.setblocking(*args, **kwargs)

    def setsockopt(self, *args, **kwargs) -> None:
        self.mocksock.setsockopt(*args, **kwargs)

    def shutdown(self, *args, **kwargs) -> None:
        self.mocksock.shutdown(*args, **kwargs)

    @property
    def type(self) -> int:
        return self.mocksock.type


@pytest.fixture
def loopback_socket():
    with LoopbackSocket() as loopback:
        yield loopback


class LoopbackTLSSocket(LoopbackSocket):
    """A pair of connected sockets for testing. The test side is wrapped in an SSL context.
    
    Return an instance of this class from a mock to use as a socket in tests.
    
    You must call test_do_handshake() before using either end of the socket."""
    def __init__(self):
        super().__init__()
        self._wrap_socket()

    def reset(self) -> None:
        """Reset the socket pair."""
        super().reset()
        self._wrap_socket()

    def _wrap_socket(self) -> None:
        """Wrap the test side of the socket in an SSL context."""
        self.server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.cert_pem, key_pem = generate_selfsigned_cert("localhost")
        with tempfile.TemporaryDirectory() as tmpdir:
            certfile = os.path.join(tmpdir, "cert.pem")
            keyfile = os.path.join(tmpdir, "key.pem")
            with open(certfile, "wb") as f:
                f.write(self.cert_pem)
            with open(keyfile, "wb") as f:
                f.write(key_pem)
            self.server_context.load_cert_chain(certfile, keyfile)
        self.testsock = self.server_context.wrap_socket(self.testsock, server_side=True, do_handshake_on_connect=False)

    def _do_handshake(self) -> None:
        self.testsock.do_handshake()

    def test_do_handshake(self) -> None:
        """Call do_handshake() on the test side of the socket in a thread, to avoid blocking the test."""
        self.handshake_thread = threading.Thread(target=self._do_handshake, daemon=True)
        self.handshake_thread.start()


@pytest.fixture
def loopback_tls_socket():
    with LoopbackTLSSocket() as loopback:
        yield loopback


@pytest.fixture
def ssl_client_context():
    """Provides a function for getting a new SSL client context with provided certificate."""
    def _ssl_client_context(cert_pem: bytes) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with tempfile.TemporaryDirectory() as tmpdir:
            certfile = os.path.join(tmpdir, "cert.pem")
            with open(certfile, "wb") as f:
                f.write(cert_pem)
            context.load_verify_locations(certfile)
        return context
    return _ssl_client_context
