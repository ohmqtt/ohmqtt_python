import socket

import pytest

from ohmqtt.connection.address import Address
from ohmqtt.platform import HAS_AF_UNIX


def lookup_family(family: str) -> int:
    """Convert family string to socket address family."""
    if family == "AF_INET":
        return socket.AF_INET
    if family == "AF_INET6":
        return socket.AF_INET6
    if family == "AF_UNIX" and HAS_AF_UNIX:
        return socket.AF_UNIX
    raise ValueError(f"Unsupported address family: {family}")


def test_address_valid(test_data):
    """Test the Address class with a Unix socket address."""
    for case in test_data:
        case_addr = case["address"]
        address = Address(case_addr)
        assert address.scheme == case["scheme"], f"scheme for {case_addr}"
        assert address.family == lookup_family(case["family"]), f"family for {case_addr}"
        assert address.host == case["host"], f"host for {case_addr}"
        assert address.port == case["port"], f"port for {case_addr}"
        assert address.username == case.get("username", None), f"username for {case_addr}"
        assert address.password == case.get("password", None), f"password for {case_addr}"
        assert address.use_tls is case.get("use_tls", False), f"use_tls for {case_addr}"
        assert repr(address)
        if case.get("password", None) is not None:
            assert case["password"] not in repr(address), f"password not hidden for {case_addr}"


@pytest.mark.skipif(
    not HAS_AF_UNIX,
    reason="Unix domain sockets are not available on this platform",
)
def test_address_unix(test_data):
    test_address_valid(test_data)


def test_address_invalid(test_data):
    """Test the Address class with invalid addresses."""
    for case in test_data:
        try:
            Address(case["address"])
        except ValueError:
            pass
        else:
            pytest.fail(f"Expected ValueError for address: {case['address']}")


def test_address_empty():
    """Test the Address class with an empty address.

    This should result in an Address object with no values set."""
    address = Address("")
    for attr in Address.__slots__:
        assert not hasattr(address, attr), attr
    assert not address.use_tls
    repr(address)
