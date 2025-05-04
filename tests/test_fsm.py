import logging
import threading
import time

import pytest

from ohmqtt.connection import Address
from ohmqtt.connection.fsm import FSM, FSMState, InvalidStateError
from ohmqtt.connection.types import ConnectParams, StateData, StateEnvironment


class _TestError(Exception):
    """Custom exception for testing purposes."""
    pass


class MockState(FSMState):
    """Simulate a blocking state."""
    @classmethod
    def handle(cls, fsm, data, env, params, max_wait):
        with fsm.cond:
            logging.debug(f"Waiting {max_wait=} in {cls.__name__} handle")
            fsm.cond.wait(max_wait)
        return True
class MockStateA(MockState):
    """This is set up like initial state."""
    pass
class MockStateB(MockState):
    """This is an intermediate state."""
    pass
class MockStateC(MockState):
    """This state is final."""
    @classmethod
    def handle(*args, **kwargs):
        return True
class MockStateErr(MockState):
    """This state is just broken."""
    @classmethod
    def enter(cls, *args, **kwargs):
        raise _TestError("TEST Error in state enter")
    @classmethod
    def handle(cls, *args, **kwargs):
        raise _TestError("TEST Error in state handler")


MockStateA.transitions_to = (MockStateB, MockStateC, MockStateErr)

MockStateB.transitions_to = (MockStateC, MockStateErr)
MockStateB.can_request_from = (MockStateA,)

MockStateC.can_request_from = (MockStateA, MockStateB)

MockStateErr.transitions_to = (MockStateC,)
MockStateErr.can_request_from = (MockStateA, MockStateB)


@pytest.fixture
def callbacks(mocker):
    class EnvironmentCallbacks:
        """Container for StateEnvironment callbacks."""
        def __init__(self, mocker):
            self.close = mocker.Mock()
            self.open = mocker.Mock()
            self.read = mocker.Mock()
    return EnvironmentCallbacks(mocker)


@pytest.fixture
def env(callbacks):
    """Fixture to create a StateEnvironment."""
    return StateEnvironment(
        close_callback=callbacks.close,
        open_callback=callbacks.open,
        read_callback=callbacks.read,
    )


def test_fsm_init(env):
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)
    assert fsm.env == env
    assert fsm.previous_state == MockStateA
    assert fsm.requested_state == MockStateA
    assert fsm.state == MockStateA
    assert fsm.error_state == MockStateC
    assert fsm._state_changed
    assert not fsm._state_requested
    assert isinstance(fsm._state_data, StateData)


def test_fsm_props(env):
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    params = ConnectParams(address=Address("test_address"))
    fsm.set_params(params)
    assert fsm.params == params

    assert fsm.get_state() == MockStateA

    with pytest.raises(TypeError):
        MockStateA()


@pytest.mark.parametrize("do_request", [True, False])
def test_fsm_wait_for_state(do_request, env):
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)
    assert fsm.wait_for_state([MockStateB, MockStateC], 0.001) is False

    start = threading.Event()
    def notifier():
        start.wait()
        time.sleep(0.1)
        if do_request:
            fsm.request_state(MockStateB)
        else:
            fsm.change_state(MockStateB)
        fsm.loop_once()

    thread = threading.Thread(target=notifier, daemon=True)
    thread.start()

    try:
        assert fsm.wait_for_state([MockStateA], 0.1) is True
        start.set()
        assert fsm.wait_for_state([MockStateB, MockStateC], 0.2) is True
    finally:
        thread.join(0.1)
        assert not thread.is_alive()


@pytest.mark.parametrize("do_request", [True, False])
def test_fsm_loop_until_state(do_request, env):
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    start = threading.Event()
    def notifier():
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


def test_fsm_loop_until_state_timeout(env):
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)
    t0 = time.monotonic()
    assert fsm.loop_until_state([MockStateC], timeout=0.001) is False
    assert time.monotonic() - t0 < 0.1


@pytest.mark.parametrize("do_request", [True, False])
def test_fsm_loop_until_state_error(do_request, env):
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    start = threading.Event()
    def notifier():
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
def test_fsm_loop_until_state_final(do_request, env):
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    start = threading.Event()
    def notifier():
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


def test_fsm_loop_once_error(env):
    fsm = FSM(env=env, init_state=MockStateErr, error_state=MockStateC)

    with pytest.raises(_TestError):
        fsm.loop_once()
    assert fsm.state == MockStateC
    assert fsm.previous_state == MockStateErr


def test_fsm_change_state(env):
    fsm = FSM(env=env, init_state=MockStateA, error_state=MockStateC)

    # Redundant calls should not break the state machine.
    fsm.change_state(MockStateB)
    fsm.change_state(MockStateB)
    assert fsm.state == MockStateB
    assert fsm.previous_state == MockStateA

    with pytest.raises(InvalidStateError):
        fsm.change_state(MockStateA)


def test_fsm_request_state(env):
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


def test_fsm_loop_error_explosion(env):
    fsm = FSM(env=env, init_state=MockStateErr, error_state=MockStateA)

    # We can't reach the error state from the broken initial state.
    with pytest.raises(_TestError):
        fsm.loop_once()
    assert fsm.state == MockStateErr
    assert fsm.previous_state == MockStateErr


def test_fsm_loop_idle(env):
    fsm = FSM(env=env, init_state=MockStateC, error_state=MockStateC)

    start = threading.Event()
    def wakeup():
        start.wait()
        time.sleep(0.1)
        with fsm.cond:
            fsm.cond.notify_all()

    thread = threading.Thread(target=wakeup, daemon=True)
    thread.start()

    start.set()
    ret = fsm.loop_until_state([MockStateA])
    assert ret is False
