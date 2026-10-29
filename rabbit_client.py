"""Minimal RabbitMQ helpers (AMQP 0-9-1 via pika) for classic, quorum and stream queues."""
import time

import pika

RABBIT_PARAMS = pika.ConnectionParameters(host="localhost", port=5672, heartbeat=0)
PREFETCH = 1000          # required for streams over AMQP 0-9-1, used for all queue types
ACK_BATCH = 500          # ack with multiple=True every N messages
CONFIRM_WINDOW = 1000    # max published-but-unconfirmed messages in flight
IDLE_TIMEOUT_S = 3       # stop consuming after this many seconds without messages


def wait_for_rabbitmq(timeout_s=120):
    """Retry until the broker accepts connections; return the server version string."""
    deadline = time.time() + timeout_s
    while True:
        try:
            connection = pika.BlockingConnection(RABBIT_PARAMS)
            version = connection._impl.server_properties["version"]
            version = version.decode() if isinstance(version, bytes) else version
            connection.close()
            return version
        except Exception as error:
            if time.time() > deadline:
                raise RuntimeError(f"RabbitMQ not reachable: {error}")
            print("  waiting for RabbitMQ ...")
            time.sleep(3)


def declare_queue(queue_name, queue_type, params=RABBIT_PARAMS):
    """Create a fresh queue of type 'classic', 'quorum' or 'stream'."""
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True,  # quorum/stream must be durable
                          arguments={"x-queue-type": queue_type})
    connection.close()


def delete_queue(queue_name):
    connection = pika.BlockingConnection(RABBIT_PARAMS)
    connection.channel().queue_delete(queue=queue_name)
    connection.close()


def publish(queue_name, message_count, payload, params=RABBIT_PARAMS):
    """Publish with publisher confirms (async, windowed). Returns seconds until the last confirm."""
    state = {"sent": 0, "confirmed": 0, "nacked": 0}

    def on_connection_open(connection):
        connection.channel(on_open_callback=on_channel_open)

    def on_channel_open(channel):
        state["channel"] = channel
        channel.confirm_delivery(on_confirm)
        publish_more()

    def publish_more():
        channel = state["channel"]
        while state["sent"] < message_count and state["sent"] - state["confirmed"] < CONFIRM_WINDOW:
            channel.basic_publish(exchange="", routing_key=queue_name, body=payload,
                                  properties=pika.BasicProperties(delivery_mode=2))
            state["sent"] += 1

    def on_confirm(frame):
        method = frame.method
        if isinstance(method, pika.spec.Basic.Nack):
            state["nacked"] += 1
        # Confirms arrive in order; "multiple" confirms everything up to delivery_tag
        state["confirmed"] = method.delivery_tag if method.multiple else state["confirmed"] + 1
        if state["confirmed"] >= message_count:
            state["connection"].close()
        else:
            publish_more()

    start = time.perf_counter()
    connection = pika.SelectConnection(params, on_open_callback=on_connection_open,
                                       on_close_callback=lambda conn, reason: conn.ioloop.stop())
    state["connection"] = connection
    connection.ioloop.start()
    elapsed = time.perf_counter() - start
    if state["nacked"]:
        raise RuntimeError(f"{state['nacked']} messages were nacked by RabbitMQ")
    return elapsed


def consume(queue_name, queue_type, max_count, params=RABBIT_PARAMS, ack=True):
    """Read (and ack) up to max_count messages, stopping early after IDLE_TIMEOUT_S of silence.

    Streams are read from the first offset ("x-stream-offset": "first"), so every new
    consumer re-reads the whole stream. Classic/quorum consumers just take what is left.
    With ack=False nothing is acked, so classic/quorum messages are requeued on close
    (used by Test 4 to count messages without removing them).
    Returns (messages_received, seconds from first to last message).
    """
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=PREFETCH)
    consume_args = {"x-stream-offset": "first"} if queue_type == "stream" else None

    received, last_tag, first_at, last_at = 0, None, None, None
    for method, _properties, _body in channel.consume(queue_name, auto_ack=False,
                                                      arguments=consume_args,
                                                      inactivity_timeout=IDLE_TIMEOUT_S):
        if method is None:  # idle timeout: nothing more to read
            break
        received += 1
        last_tag = method.delivery_tag
        last_at = time.perf_counter()
        first_at = first_at or last_at
        if ack and received % ACK_BATCH == 0:
            channel.basic_ack(last_tag, multiple=True)
            last_tag = None
        if received >= max_count:
            break
    if ack and last_tag is not None:
        channel.basic_ack(last_tag, multiple=True)
    channel.cancel()
    connection.close()
    return received, (last_at - first_at) if received else 0.0
