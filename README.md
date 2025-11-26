# ΩQTT

*pron. "ohm cue tee tee" or "omega cutie"*

A reliable and persistent MQTT 5.0 client library for Python.

## Features

### QoS and Persistence

ΩQTT supports publish and subscribing all QoS levels with optional persistence to disk for QoS >0.
When not persisting to disk, a fast (but volatile) in memory store is used.
Either way, publishing a QoS >0 message returns a handle with a method to wait for the message to be fully acknowledged by the broker.

### Automatic Topic Alias

Set an alias policy when publishing a message and a topic alias will be generated, if allowed by the broker.
You can also force QoS 0 messages to require a topic alias, to avoid silently bleeding bandwidth.

### Properties

Access all optional properties of all MQTT control packet types.
If you ever wanted to check the user properties of your packets, welcome home.

### Portability

ΩQTT is tested on Linux, Windows and MacOS with CPython versions 3.10-3.13.
It should work on any platform that CPython runs on.

### Reliability

ΩQTT has been implemented to a high standard of test coverage and static analysis, from the beginning.
It continues to improve.

## Installation

ΩQTT is published to the Python Package Index.

`python3 -m pip install ohmqtt`

## Examples

See the `examples/` directory for examples of using ΩQTT.
With `uv` installed, run them like e.g.

`uv run python3 -m examples.publish`

By default, all of the examples try to connect to an MQTT broker at `localhost:1883`.
Supply an alternate `--address` argument if you want to connect to a broker at a different location.
If you want to run a local broker in Docker, try <https://hub.docker.com/_/eclipse-mosquitto>

## Example Use Cases

### Low Bandwidth

Besides compressing your payloads, you can reduce MQTT publish overhead by using topic aliases.

Instruct the broker that it can send topic aliases while connecting:

```python
from ohmqtt import Client, MQTTConnectProps

# The maximum value for TopicAliasMaximum is USHRT_MAX (65535).
# It must be >0 for the broker to use topic aliases when sending messages to the client.
connect_props = MQTTConnectProps(TopicAliasMaximum=0xffff)
with Client() as client:
    client.connect(address, connect_properties=connect_props)
```

Specify that topic aliases should be used when publishing messages:

```python
from ohmqtt import AliasPolicy

client.publish("some/topic", b"the payload", alias_policy=AliasPolicy.TRY)
```

This will automatically assign topic aliases to topics up to the maximum topic alias ID reported by the broker.

Higher QoS levels will use more overall bandwidth.
You may force a maximum QoS level when subscribing to a topic:

```python
client.subscribe("some/topic", callback, max_qos=0)
```

### Reliable Messaging

You can wait for QoS>0 messages to be acknowledged and access the acknowledgement packets.
For QoS 1 this will be PUBACK, for QoS 2 it may be PUBREC (in case of an error) or PUBCOMP.

```python
from ohmqtt import LostMessageError, MQTTError, MQTTQoS, MQTTReasonCode

handle = client.publish("topic", b"payload", qos=MQTTQoS.Q2)
try:
    ack = handle.wait_for_ack(timeout=5.0)
    # The broker MAY use this reason code when it knows there are no subscribers.
    #   Verify that your broker supports this for QoS 1 and/or 2.
    if ack.reason_code == MQTTReasonCode.NoMatchingSubscribers:
        raise Exception("Broker reported no subscribers!")
    print(ack)
except TimeoutError:  # Was not acknowledged in time
    ...
except LostMessageError:  # The message was dropped upon new session from the broker
    ...
except MQTTError as exc:  # The acknowledgement had an error code
    print(f"Error from server while publishing: {exc.reason_code}")
```

### Persistence

You can persist QoS>0 publish state to disk to guarantee delivery beyond the lifetime of your application.

ΩQTT will follow the MQTT specification, meaning that when you connect to a broker and it does not recognize your session,
the persistent state will be cleared.

Specify the path to the database when creating the `Client` (a new database will be created if it does not exist).

Also specify a `client_id` and session expiry interval when connecting to the broker:

```python
from ohmqtt import Client, MQTTConnectProps

# Set SessionExpiryInterval=UINT_MAX to indicate that the session should never expire.
# Otherwise the interval is in seconds and must be >0 to persist the session.
connect_props = MQTTConnectProps(SessionExpiryInterval=0xffffffff)
with Client(db_path="/path/to/ohmqtt.db") as client:
    client.connect(address, client_id="my_client_id", connect_properties=connect_props)
```

By default the database will operate in a very safe mode.
Calls to publish with QoS>0 will not return, and data will not be sent over the wire, until the message is fully committed to disk.
You can specify to use a faster, less synchronous configuration which may be good enough for your use case.
The implementation will use SQLite WAL. Set `db_fast=True` when constructing the client like so:

```python
from ohmqtt import Client, MQTTConnectProps

connect_props = MQTTConnectProps(SessionExpiryInterval=0xffffffff)
with Client(db_path="/path/to/ohmqtt.db", db_fast=True) as client:
    client.connect(address, client_id="my_client_id", connect_properties=connect_props)
```

## Development

I wrote ΩQTT in 2025 as a personal challenge with two goals.

First, I wanted a complete understanding of the MQTT 5.0 protocol specification.

Second, I set out to create the fastest,
most complete,
most analyzed,
most tested,
best architected,
and most maintainable stand-alone FOSS MQTT 5.0 client implementation for pure Python.

I am committed to the care and maintenance of ΩQTT, with the cooperation and feedback of community.

### Support

Contact me at <matt@endpointdev.com> for support with ΩQTT or other MQTT projects.

### Running the Tests

This project uses `nox` and `uv` to run the tests against all supported Python versions.

To do all of this in a venv:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install nox uv
nox
```

### Publishing to PyPI

Create a release on GitHub to publish to PyPI.

### Building the Docs

The docs are automatically built on [readthedocs](https://ohmqtt-python.readthedocs.io/en/latest/) upon release.

To manually build the docs: `uv run make clean && uv run make html`

### Contributing

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for guidelines.
