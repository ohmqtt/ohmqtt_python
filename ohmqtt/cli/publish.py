import argparse
import time

from .common import add_broker_arguments, get_client, Command, SubParsersT
from .. import MQTTPublishProps


class PublishCommand(Command):
    """Publish a message to a topic."""

    @classmethod
    def register(cls, sub_parsers: SubParsersT) -> None:
        parser = sub_parsers.add_parser(
            "publish", help="Publish a message to a topic"
        )
        add_broker_arguments(parser)
        parser.add_argument("topic", type=str, help="The topic to publish to")
        parser.add_argument("message", type=str, nargs="?", default="", help="The message content to publish")
        parser.add_argument(
            "--qos", type=int, choices=[0, 1, 2], default=0, help="The QoS level for the message"
        )
        parser.add_argument(
            "--retain", action="store_true", help="Set the retain flag for the message"
        )
        parser.add_argument(
            "--payload_format_indicator", action="store_true", help="Indicate that the payload is UTF-8 encoded"
        )
        parser.add_argument(
            "--message_expiry_interval", type=int, help="Set the message expiry interval in seconds"
        )
        parser.add_argument(
            "--content_type", type=str, help="Set the content type of the message"
        )
        parser.add_argument(
            "--response_topic", type=str, help="Set the response topic for the message"
        )
        parser.add_argument(
            "--correlation_data", type=str, help="Set the correlation data for the message (will be encoded as UTF-8)"
        )
        parser.set_defaults(func=cls.execute)

    @classmethod
    def execute(cls, args: argparse.Namespace) -> None:
        topic = args.topic
        message = args.message.encode("utf-8")
        qos = getattr(args, "qos", 0)
        retain = getattr(args, "retain", False)
        props = MQTTPublishProps()
        props_map = {
            "payload_format_indicator": "PayloadFormatIndicator",
            "message_expiry_interval": "MessageExpiryInterval",
            "content_type": "ContentType",
            "response_topic": "ResponseTopic",
            "correlation_data": "CorrelationData",
        }
        for arg_name, prop_name in props_map.items():
            value = getattr(args, arg_name, None)
            if value is not None:
                setattr(props, prop_name, value)

        if props.CorrelationData is not None:
            props.CorrelationData = props.CorrelationData.encode("utf-8")  # type: ignore

        client = get_client(args)

        handle = client.publish(
            topic,
            message,
            qos=qos,
            retain=retain,
            properties=props,
        )
        client.loop_once(max_wait=0)
        t0 = time.monotonic()
        while qos > 0 and (handle.ack is None and time.monotonic() - t0 < 10.0):
            client.loop_once(max_wait=0.1)  # pragma: no cover
        print(f"Published message to {topic}")
        client.shutdown()
        client.loop_forever()
