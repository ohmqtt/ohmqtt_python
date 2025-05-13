from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from pytest_mock import MockerFixture

from ohmqtt.connection import Address
from ohmqtt.connection.fsm import FSM, FSMState, InvalidStateError
from ohmqtt.connection.types import ConnectParams, StateData, StateEnvironment


class _TestError(Exception):
    """Custom exception for testing purposes."""


class MockState(FSMState):
    """Simulate a blocking state."""
    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment,
               params: ConnectParams, max_wait: float | None) -> bool:
        with fsm.cond:
            fsm.cond.wait(max_wait)
        return True

class MockStateA(MockState):
    """This is set up like initial state."""

class MockStateB(MockState):
    """This is an intermediate state."""

class MockStateC(MockState):
    """This state is final."""
    @classmethod
    def handle(cls, *args: Any, **kwargs: Any) -> bool:
        return True

class MockStateErr(MockState):
    """This state is just broken."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        raise _TestError("TEST Error in state enter")

    @classmethod
    def handle(cls, *args: Any, **kwargs: Any) -> bool:
        raise _TestError("TEST Error in state handler")


MockStateA.transitions_to = (MockStateB, MockStateC, MockStateErr)

MockStateB.transitions_to = (MockStateC, MockStateErr)
MockStateB.can_request_from = (MockStateA,)

MockStateC.can_request_from = (MockStateA, MockStateB)

MockStateErr.transitions_to = (MockStateC,)
MockStateErr.can_request_from = (MockStateA, MockStateB)


class EnvironmentCallbacks:
    """Container for StateEnvironment callbacks."""
    def __init__(self, mocker: MockerFixture) -> None:
        self.packet = mocker.Mock()


@pytest.fixture
def callbacks(mocker: MockerFixture) -> EnvironmentCallbacks:
    return EnvironmentCallbacks(mocker)


@pytest.fixture
def env(callbacks: EnvironmentCallbacks) -> StateEnvironment:
    """Fixture to create a StateEnvironment."""
    return StateEnvironment(
        packet_callback=callbacks.packet,
    )


def test_fsm_init(env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)
    assert fsm.env == env
    assert fsm.previous_state == MockStateA
    assert fsm.requested_state == MockStateA
    assert fsm.state == MockStateA
    assert fsm.error_state == MockStateC


def test_fsm_props(env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    params = ConnectParams(address=Address("test_address"))
    fsm.set_params(params)
    assert fsm.params == params

    assert fsm.get_state() == MockStateA

    with pytest.raises(TypeError):
        MockStateA()


@pytest.mark.parametrize("do_request", [True, False])
def test_fsm_wait_for_state(do_request: bool, env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)
    assert fsm.wait_for_state([MockStateA], 0.001) is True
    assert fsm.wait_for_state([MockStateB, MockStateC], 0.001) is False

    start = threading.Event()
    def notifier() -> None:
        start.wait()
        if do_request:
            fsm.request_state(MockStateB)
        else:
            fsm.change_state(MockStateB)
        fsm.loop_once()

    thread = threading.Thread(target=notifier, daemon=True)
    thread.start()

    try:
        start.set()
        assert fsm.wait_for_state([MockStateB, MockStateC], 0.5) is True
    finally:
        thread.join(0.1)
        assert not thread.is_alive()


@pytest.mark.parametrize("do_request", [True, False])
def test_fsm_loop_until_state(do_request: bool, env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    start = threading.Event()
    def notifier() -> None:
        start.wait()
        time.sleep(0.1)
        if do_request:
            fsm.request_state(MockStateB)
        else:
            fsm.change_state(MockStateB)

    thread = threading.Thread(target=notifier, daemon=True)
    thread.start()

    try:
        start.set()
        assert fsm.loop_until_state([MockStateB]) is True
    finally:
        thread.join(0.1)
        assert not thread.is_alive()


def test_fsm_loop_until_state_timeout(env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)
    t0 = time.monotonic()
    assert fsm.loop_until_state([MockStateC], timeout=0.001) is False
    assert time.monotonic() - t0 < 0.1


@pytest.mark.parametrize("do_request", [True, False])
def test_fsm_loop_until_state_error(do_request: bool, env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    start = threading.Event()
    def notifier() -> None:
        start.wait()
        time.sleep(0.1)
        if do_request:
            fsm.request_state(MockStateErr)
        else:
            fsm.change_state(MockStateErr)

    thread = threading.Thread(target=notifier, daemon=True)
    thread.start()

    try:
        start.set()
        with pytest.raises(_TestError):
            fsm.loop_until_state([MockStateB])
    finally:
        thread.join(0.1)
        assert not thread.is_alive()


@pytest.mark.parametrize("do_request", [True, False])
def test_fsm_loop_until_state_final(do_request: bool, env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    start = threading.Event()
    def notifier() -> None:
        start.wait()
        time.sleep(0.1)
        if do_request:
            fsm.request_state(MockStateC)
        else:
            fsm.change_state(MockStateC)

    thread = threading.Thread(target=notifier, daemon=True)
    thread.start()

    try:
        start.set()
        assert fsm.loop_until_state([MockStateB]) is False
    finally:
        thread.join(0.1)
        assert not thread.is_alive()


def test_fsm_loop_once_error(env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateErr, error_state=MockStateC)

    with pytest.raises(_TestError):
        fsm.loop_once()
    assert fsm.state == MockStateC
    assert fsm.previous_state == MockStateErr


def test_fsm_change_state(env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    # Redundant calls should not break the state machine.
    fsm.change_state(MockStateB)
    fsm.change_state(MockStateB)
    assert fsm.state == MockStateB
    assert fsm.previous_state == MockStateA

    with pytest.raises(InvalidStateError):
        fsm.change_state(MockStateA)


def test_fsm_request_state(env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    # Redundant calls should not break the state machine.
    fsm.request_state(MockStateB)
    fsm.request_state(MockStateB)
    fsm.loop_once()
    assert fsm.state == MockStateB
    assert fsm.previous_state == MockStateA

    # Should not raise if we couldn't request the state.
    fsm.request_state(MockStateA)
    fsm.loop_once()
    assert fsm.state == MockStateB
    assert fsm.previous_state == MockStateA

    # Requesting a valid state then changing to an invalid should not explode.
    fsm.request_state(MockStateErr)
    fsm.change_state(MockStateC)
    fsm.loop_once()
    assert fsm.state == MockStateC
    assert fsm.previous_state == MockStateB


def test_fsm_loop_error_explosion(env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateErr, error_state=MockStateA)

    # We can't reach the error state from the broken initial state.
    with pytest.raises(_TestError):
        fsm.loop_once()
    assert fsm.state == MockStateErr
    assert fsm.previous_state == MockStateErr


def test_fsm_loop_idle(env: StateEnvironment) -> None:
    fsm = FSM(env=env, init_state=MockStateC, error_state=MockStateC)

    start = threading.Event()
    def wakeup() -> None:
        start.wait()
        time.sleep(0.1)
        with fsm.cond:
            fsm.cond.notify_all()

    thread = threading.Thread(target=wakeup, daemon=True)
    thread.start()

    start.set()
    ret = fsm.loop_until_state([MockStateA])
    assert ret is False
