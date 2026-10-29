"""RabbitMQ stream via the native stream protocol (rstream, port 5552), used by Test 3b."""
import asyncio
import time

from rstream import ConsumerOffsetSpecification, OffsetType, Consumer, Producer

STREAM_HOST, STREAM_PORT = "localhost", 5552
CREDENTIALS = {"username": "guest", "password": "guest"}
PUBLISH_BATCH = 1000  # messages per send_batch call
INITIAL_CREDIT = 100  # chunks the broker may push before the client asks for more


async def _publish(stream_name, message_count, payload):
    """Publish in batches and wait for every publisher confirm. Returns seconds.

    send_batch() is asynchronous; rstream calls on_confirm once per message when the
    broker's PublishConfirm (or PublishError) frame for it arrives. The timer stops only
    after the LAST confirmation, so the rate counts broker-confirmed messages only.
    """
    all_confirmed = asyncio.Event()
    answered, rejected = 0, 0

    def on_confirm(status):
        nonlocal answered, rejected
        answered += 1
        rejected += 0 if status.is_confirmed else 1
        if answered >= message_count:
            all_confirmed.set()

    async with Producer(STREAM_HOST, STREAM_PORT, **CREDENTIALS) as producer:
        await producer.create_stream(stream_name, exists_ok=True)
        start = time.perf_counter()
        for sent in range(0, message_count, PUBLISH_BATCH):
            batch = [payload] * min(PUBLISH_BATCH, message_count - sent)
            await producer.send_batch(stream_name, batch, on_publish_confirm=on_confirm)
        await asyncio.wait_for(all_confirmed.wait(), timeout=120)
        elapsed = time.perf_counter() - start
    if rejected:
        raise RuntimeError(f"{rejected} stream messages were rejected by the broker")
    return elapsed


async def _consume(stream_name, message_count):
    """Read from the first offset until message_count messages. Returns seconds first-to-last."""
    times = {"first": None, "last": None}
    received = 0
    consumer = Consumer(STREAM_HOST, STREAM_PORT, **CREDENTIALS)

    async def on_message(_message, _context):
        nonlocal received
        received += 1
        times["last"] = time.perf_counter()
        times["first"] = times["first"] or times["last"]
        if received >= message_count:
            consumer.stop()

    await consumer.start()
    await consumer.subscribe(stream_name, on_message, initial_credit=INITIAL_CREDIT,
                             offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None))
    await asyncio.wait_for(consumer.run(), timeout=120)
    await consumer.delete_stream(stream_name, missing_ok=True)
    await consumer.close()
    return times["last"] - times["first"]


def publish(stream_name, message_count, payload):
    return asyncio.run(_publish(stream_name, message_count, payload))


def consume(stream_name, message_count):
    """Consume, then delete the stream. Returns seconds from first to last message."""
    return asyncio.run(_consume(stream_name, message_count))
