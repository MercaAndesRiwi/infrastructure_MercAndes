import os

import pika
import pytest

from operational_messaging.rabbitmq import connection_parameters, declare_topology

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_RABBITMQ_TESTS") != "1",
        reason="set RUN_RABBITMQ_TESTS=1 with RabbitMQ running",
    ),
]


def test_declaring_the_topology_twice_is_safe():
    """A second declare with different arguments would raise PRECONDITION_FAILED."""
    connection = pika.BlockingConnection(connection_parameters())
    try:
        channel = connection.channel()
        declare_topology(channel)
        declare_topology(channel)
    finally:
        connection.close()
