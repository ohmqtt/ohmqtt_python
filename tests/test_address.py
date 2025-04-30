import socket

import pytest

from ohmqtt.connection.address import Address


def lookup_family(family: str) -> int:
    """Convert family string to socket address family."""
    if family == "AF_INET":
        return socket.AF_INET
    elif family == "AF_INET6":
        return socket.AF_INET6
    elif family == "AF_UNIX":
        return socket.AF_UNIX
    else:
        raise ValueError(f"Unsupported address family: {family}")


def test_address_valid(test_data):
    """Test the Address class with a Unix socket address."""
    for case in test_data:
        address = Address(case["address"])
        assert address.family == lookup_family(case["family"]), case["address"]
        assert address.host == case["host"], case["address"]
        assert address.port == case["port"], case["address"]
        if "username" in case:
            assert address.username == case["username"], case["address"]
        else:
            assert address.username is None, case["address"]
        if "password" in case:
            assert address.password == case["password"], case["address"]
        else:
            assert address.password is None, case["address"]

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
