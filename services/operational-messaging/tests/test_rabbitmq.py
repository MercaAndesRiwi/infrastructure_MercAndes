from types import SimpleNamespace

import pytest

from operational_messaging.config import MAX_RETRIES
from operational_messaging.rabbitmq import handle_failure, next_action, publish_message


class FakeChannel:
    def __init__(self):
        self.acked = []
        self.nacked = []

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacked.append((delivery_tag, requeue))


@pytest.mark.parametrize("retry_count", range(MAX_RETRIES))
def test_messages_are_retried_while_attempts_remain(retry_count):
    assert next_action(retry_count) == "retry"


def test_message_is_dead_lettered_after_max_retries():
    assert next_action(MAX_RETRIES) == "dead_letter"
    assert next_action(MAX_RETRIES + 1) == "dead_letter"


def test_publishing_to_an_unknown_queue_fails_before_connecting():
    with pytest.raises(ValueError, match="Unsupported operational queue"):
        publish_message("not.an.operational.queue", {"message_id": "m-1"})


def test_failed_delivery_is_republished_to_the_retry_queue(monkeypatch):
    published = []
    monkeypatch.setattr(
        "operational_messaging.rabbitmq.publish_message",
        lambda queue, message, **kwargs: published.append((queue, message, kwargs)),
    )
    channel = FakeChannel()

    handle_failure(
        channel,
        SimpleNamespace(delivery_tag=7),
        "alertas.riesgo_quiebre",
        b'{"message_id": "m-1"}',
        {},
        0,
    )

    assert published == [
        ("alertas.riesgo_quiebre", {"message_id": "m-1"}, {"retry_count": 1, "retry": True})
    ]
    assert channel.acked == [7]
    assert channel.nacked == []


def test_exhausted_retries_are_moved_to_the_dead_letter_queue(monkeypatch):
    dead_letters = []
    monkeypatch.setattr(
        "operational_messaging.rabbitmq.publish_dead_letter",
        lambda queue, body, headers: dead_letters.append((queue, body)),
    )
    channel = FakeChannel()

    handle_failure(
        channel,
        SimpleNamespace(delivery_tag=8),
        "alertas.riesgo_quiebre",
        b'{"message_id": "m-2"}',
        {},
        MAX_RETRIES,
    )

    assert dead_letters == [("alertas.riesgo_quiebre", b'{"message_id": "m-2"}')]
    assert channel.acked == [8]
    assert channel.nacked == []


def test_a_non_object_json_body_goes_to_the_dead_letter_queue_without_retrying(monkeypatch):
    dead_letters = []
    monkeypatch.setattr(
        "operational_messaging.rabbitmq.publish_dead_letter",
        lambda queue, body, headers: dead_letters.append((queue, body)),
    )
    channel = FakeChannel()

    handle_failure(
        channel,
        SimpleNamespace(delivery_tag=9),
        "alertas.riesgo_quiebre",
        b"[]",
        {},
        0,
    )

    assert dead_letters == [("alertas.riesgo_quiebre", b"[]")]
    assert channel.acked == [9]
    assert channel.nacked == []
