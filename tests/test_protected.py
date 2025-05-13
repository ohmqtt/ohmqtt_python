from typing import Any

import pytest

from ohmqtt.protected import Protected, protect


class ProtectedClass(Protected):
    """A class that inherits from Protected."""
    def __init__(self, value: Any) -> None:
        super().__init__()
        self._value = value

    @protect
    def get_value(self) -> Any:
        return self.value

    @protect
    def set_value(self, value: Any) -> None:
        self._value = value

    @property
    @protect
    def value(self) -> Any:
        return self._value

    @value.setter
    @protect
    def value(self, value: Any) -> None:
        self._value = value



def test_protected_class() -> None:
    """Test the Protected class."""
    obj = ProtectedClass(42)

    with pytest.raises(RuntimeError):
        obj.get_value()
    with pytest.raises(RuntimeError):
        obj.set_value(42)
    with pytest.raises(RuntimeError):
        obj.value
    with pytest.raises(RuntimeError):
        obj.value = 42

    with obj as p:
        p.set_value(99)
        assert p.get_value() == 99
        p.set_value(100)
        assert p.value == 100
