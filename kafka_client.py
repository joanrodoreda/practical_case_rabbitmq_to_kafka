"""Minimal Apache Kafka helpers (confluent-kafka) mirroring rabbit_client.py."""
import time
import uuid

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:9092"
IDLE_TIMEOUT_S = 3       # stop consuming after this many seconds without messages
GROUP_JOIN_GRACE_S = 7   # extra wait before the first message (consumer-group join)


def wait_for_kafka(timeout_s=120):
    """Retry until the broker answers a metadata request."""
    deadline = time.time() + timeout_s
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception as error:
            if time.time() > deadline:
                raise RuntimeError(f"Kafka not reachable: {error}")
            print("  waiting for Kafka ...")
            time.sleep(3)


def create_topic(topic_name):
    """Create a fresh single-partition topic (one partition keeps ordering comparable to a queue)."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    future = admin.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
    future[topic_name].result()


def delete_topic(topic_name):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.delete_topics([topic_name])[topic_name].result()


def publish(topic_name, message_count, payload):
    """Produce with acks=all (Kafka's equivalent of publisher confirms). Returns seconds."""
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "linger.ms": 5})
    failures = []

    def on_delivery(error, _message):
        if error:
            failures.append(error)

    start = time.perf_counter()
    for _ in range(message_count):
        while True:
            try:
                producer.produce(topic_name, payload, on_delivery=on_delivery)
                break
            except BufferError:  # local queue full: let it drain
                producer.poll(0.1)
        producer.poll(0)
    producer.flush()
    elapsed = time.perf_counter() - start
    if failures:
        raise RuntimeError(f"{len(failures)} Kafka deliveries failed: {failures[0]}")
    return elapsed


def consume(topic_name, max_count, group_id=None):
    """Read up to max_count messages from the earliest offset with a NEW consumer group.

    A new group has no committed offsets, so auto.offset.reset=earliest makes it start at
    the beginning of the log. Returns (messages_received, seconds from first to last message).
    """
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id or f"group-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic_name])

    received, first_at, last_at = 0, None, None
    idle_since = time.time()
    while received < max_count:
        batch = consumer.consume(num_messages=1000, timeout=0.5)
        valid = [message for message in batch if message.error() is None]
        if valid:
            received += len(valid)
            last_at = time.perf_counter()
            first_at = first_at or last_at
            idle_since = time.time()
        else:
            # Before the first message, allow extra time for the consumer-group join.
            idle_limit = IDLE_TIMEOUT_S if first_at else IDLE_TIMEOUT_S + GROUP_JOIN_GRACE_S
            if time.time() - idle_since > idle_limit:
                break
    consumer.close()
    return received, (last_at - first_at) if received else 0.0
