from __future__ import annotations

import logging
from pathlib import Path
import pytest
import socket
import ssl
import tempfile
import threading
from typing import Any, Callable, Generator, TypeVar
import yaml

from tests.util.selfsigned import generate_selfsigned_cert

from ohmqtt.platform import HAS_AF_UNIX

logger = logging.getLogger(__name__)

LoopSelfT = TypeVar("LoopSelfT", bound="LoopbackSocket")


@pytest.fixture
def test_data(request: pytest.FixtureRequest) -> list[dict[str, Any]]:
    """Load test data from a YAML file.

    The YAML file must be named after the test suite, and contain a mapping of test names to test data."""
    suite_name = request.module.__name__.split(".")[-1]
    test_name = request.node.name
    data_path = Path("tests") / "data" / f"{suite_name}.yml"
    with data_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data[test_name]  # type: ignore[no-any-return]


class LoopbackSocket:
    """A pair of connected sockets for testing.

    Return an instance of this class from a mock to use as a socket in tests."""
    connect_calls: list[tuple[Any, ...] | str]

    def __init__(self) -> None:
        self.reset()
        self.test_close = self.testsock.close
        self.test_sendall = self.testsock.sendall
        self.test_recv = self.testsock.recv
        self.test_shutdown = self.testsock.shutdown
        self.close = self.mocksock.close
        self.detach = self.mocksock.detach
        self.family = self.mocksock.family
        self.fileno = self.mocksock.fileno
        self.getblocking = self.mocksock.getblocking
        self.getsockopt = self.mocksock.getsockopt
        self.gettimeout = self.mocksock.gettimeout
        self.proto = self.mocksock.proto
        self.recv = self.mocksock.recv
        self.recv_into = self.mocksock.recv_into
        self.send = self.mocksock.send
        self.sendall = self.mocksock.sendall
        self.setblocking = self.mocksock.setblocking
        self.shutdown = self.mocksock.shutdown
        self.type = self.mocksock.type

    def __enter__(self: LoopSelfT) -> LoopSelfT:
        return self

    def __exit__(self, *args: Any) -> None:
        self.mocksock.close()
        self.testsock.close()

    def __del__(self) -> None:
        self.mocksock.close()
        self.testsock.close()

    def reset(self) -> None:
        """Reset the socket pair."""
        self.mocksock, self.testsock = socket.socketpair()
        self.testsock.settimeout(5.0)
        self.connect_calls = []

    def connect(self, address: tuple[Any, ...] | str) -> None:
        self.connect_calls.append(address)

    def setsockopt(self, level: int, optname: int, value: int) -> None:
        if optname == socket.TCP_NODELAY:
            # Where AF_UNIX exists, do not set TCP_NODELAY on either side of the socket.
            # We are spoofing TCP but the socketpair is a Unix domain socket.
            if HAS_AF_UNIX:
                logger.info("Not setting TCP_NODELAY on Unix domain socket")
                return
        self.mocksock.setsockopt(level, optname, value)


@pytest.fixture
def loopback_socket() -> Generator[LoopbackSocket, None, None]:
    with LoopbackSocket() as loop:
        yield loop


class LoopbackTLSSocket(LoopbackSocket):
    """A pair of connected sockets for testing. The test side is wrapped in an SSL context.

    Return an instance of this class from a mock to use as a socket in tests.

    You must call test_do_handshake() before using either end of the socket."""
    testsock: ssl.SSLSocket

    def __init__(self) -> None:
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
            certfile = Path(tmpdir) / "cert.pem"
            keyfile = Path(tmpdir) / "key.pem"
            with certfile.open("wb") as f:
                f.write(self.cert_pem)
            with keyfile.open("wb") as f:
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
def loopback_tls_socket() -> Generator[LoopbackTLSSocket, None, None]:
    with LoopbackTLSSocket() as loop:
        yield loop


@pytest.fixture
def ssl_client_context() -> Callable[[bytes], ssl.SSLContext]:
    """Provides a function for getting a new SSL client context with provided certificate."""
    def _ssl_client_context(cert_pem: bytes) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with tempfile.TemporaryDirectory() as tmpdir:
            certfile = Path(tmpdir) / "cert.pem"
            with certfile.open("wb") as f:
                f.write(cert_pem)
            context.load_verify_locations(certfile)
        return context
    return _ssl_client_context
