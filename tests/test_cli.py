import argparse
from functools import partial
import logging
import threading
import time
from typing import TypeAlias

import pytest
from pytest_mock import MockerFixture

from ohmqtt import __version__, MQTTPublishPacket
from ohmqtt.cli import main
from ohmqtt.cli.publish import PublishCommand
from ohmqtt.cli.subscribe import SubscribeCommand
from ohmqtt.cli.common import get_client, BrokerConnectionError
from .util.fake_broker import FakeBroker


CapSysT: TypeAlias = pytest.CaptureFixture[str]


def test_cli_main_no_args(capsys: CapSysT) -> None:
    main([])
    captured = capsys.readouterr()
    assert "ohmqtt Command Line Interface" in captured.out
    assert "Available commands" in captured.out


def test_cli_main_version(capsys: CapSysT) -> None:
    with pytest.raises(SystemExit):
        main(["--version"])
    captured = capsys.readouterr()
    assert __version__ in captured.out


@pytest.mark.parametrize("arg", ["-v", "--verbose"])
def test_cli_main_verbose(mocker: MockerFixture, arg: str) -> None:
    mock_logging_basic_config = mocker.patch("logging.basicConfig")
    main([arg])
    mock_logging_basic_config.assert_called_once_with(level=logging.DEBUG)


def test_cli_main_publish(mocker: MockerFixture) -> None:
    mock_publish_execute = mocker.patch(
        "ohmqtt.cli.publish.PublishCommand.execute"
    )
    main(["publish", "test/topic", "Hello, MQTT!"])
    mock_publish_execute.assert_called_once()
    args = mock_publish_execute.call_args[0][0]
    assert args.topic == "test/topic"
    assert args.message == "Hello, MQTT!"


@pytest.mark.parametrize("password", [None, "secret"])
def test_cli_command_get_client(mocker: MockerFixture, password: str | None) -> None:
    args = argparse.Namespace()
    args.connect_timeout = 1
    args.topic_alias_maximum = 0xffff  # Set any property to test the code path
    args.username = "foo"
    if password is not None:
        args.password = password

    with FakeBroker() as broker:
        args.address = f"localhost:{broker.port}"
        client = get_client(args)
        assert client.is_connected()
        client.shutdown()

    mocker.patch(
        "ohmqtt.cli.common.Client.loop_until_connected", return_value=False
    )
    with FakeBroker() as broker:
        args.address = f"localhost:{broker.port}"
        with pytest.raises(BrokerConnectionError):
            client = get_client(args)

    args.address = "invalid_address"
    with pytest.raises(BrokerConnectionError):
        get_client(args)


@pytest.mark.parametrize("correlation_data", [None, "asdf"])
@pytest.mark.parametrize("qos", [0, 1, 2])
def test_cli_command_publish(capsys: CapSysT, mocker: MockerFixture, correlation_data: str | None, qos: int) -> None:
    args = argparse.Namespace()
    args.address = "localhost"
    args.topic = "test/topic"
    args.message = "Hello, MQTT!"
    args.qos = qos

    if correlation_data is not None:
        args.correlation_data = correlation_data

    mock_client = mocker.MagicMock()
    mock_get_client = mocker.patch(
        "ohmqtt.cli.publish.get_client", return_value=mock_client
    )

    PublishCommand.execute(args)

    mock_get_client.assert_called_once_with(args)
    mock_client.publish.assert_called_once_with(
        "test/topic",
        b"Hello, MQTT!",
        qos=qos,
        retain=False,
        properties=mocker.ANY,
    )
    if correlation_data is not None:
        called_props = mock_client.publish.call_args[1]["properties"]
        assert called_props.CorrelationData == correlation_data.encode("utf-8")

    captured = capsys.readouterr()
    assert "Published message to test/topic" in captured.out


def test_cli_command_subscribe(capsys: CapSysT, mocker: MockerFixture) -> None:
    class MockArgs(argparse.Namespace):
        address = "localhost"
        topic = "test/topic"

    mock_client = mocker.MagicMock()
    mock_get_client = mocker.patch(
        "ohmqtt.cli.subscribe.get_client", return_value=mock_client
    )

    # Mock the loop_forever method to raise KeyboardInterrupt soon
    def mock_loop_forever() -> None:
        time.sleep(1.0)
        raise KeyboardInterrupt

    mock_client.loop_forever.side_effect = mock_loop_forever

    thread = threading.Thread(
        target=partial(SubscribeCommand.execute, MockArgs())
    )
    thread.start()

    time.sleep(0.4)  # Ensure subscription is set up
    mock_get_client.assert_called_once_with(MockArgs())
    mock_client.subscribe.assert_called_once()
    callback = mock_client.subscribe.call_args[0][1]
    callback(mock_client, MQTTPublishPacket(topic="test/topic", payload=b"Test message"))
    callback(mock_client, MQTTPublishPacket(topic="test/topic", payload=b"\x00\x01\x02"))
    callback(mock_client, MQTTPublishPacket(topic="test/topic", payload=b"\xC0\x80"))
    time.sleep(0.1)
    captured = capsys.readouterr()
    out = captured.out.splitlines()
    assert out == [
        "test/topic : Test message",
        "test/topic : <3 bytes of binary data>",
        "test/topic : <2 bytes of binary data>",
    ]
    thread.join(timeout=1.0)
