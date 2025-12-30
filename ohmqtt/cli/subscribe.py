import argparse
import string

from .common import add_broker_arguments, get_client, Command, SubParsersT
from .. import Client, MQTTPublishPacket, RetainPolicy


class SubscribeCommand(Command):
    """Subscribe to a topic and print messages."""

    @classmethod
    def register(cls, sub_parsers: SubParsersT) -> None:
        parser = sub_parsers.add_parser(
            "subscribe", help="Subscribe to a topic and print messages"
        )
        add_broker_arguments(parser)
        parser.add_argument("topic", type=str, help="The topic to subscribe to")
        parser.add_argument(
            "--max_qos", type=int, choices=[0, 1, 2], default=2, help="The maximum QoS level for received messages"
        )
        parser.add_argument(
            "--no_retained", action="store_true", help="Do not receive retained messages"
        )
        parser.set_defaults(func=cls.execute)

    @classmethod
    def execute(cls, args: argparse.Namespace) -> None:
        topic = args.topic
        max_qos = args.max_qos
        retain_policy = RetainPolicy.NEVER if args.no_retained else RetainPolicy.ALWAYS

        def callback(_: Client, msg: MQTTPublishPacket) -> None:
            content = f"<{len(msg.payload)} bytes of binary data>"
            try:
                decoded = msg.payload.decode("utf-8")
                if all(c in string.printable for c in decoded):
                    content = decoded
            except UnicodeDecodeError:
                pass
            print(f"{msg.topic} : {content}")

        client = get_client(args)

        client.subscribe(
            topic,
            callback,
            max_qos=max_qos,
            retain_policy=retain_policy,
        )
        try:
            client.loop_forever()
        except KeyboardInterrupt:
            pass
        finally:
            client.shutdown()
