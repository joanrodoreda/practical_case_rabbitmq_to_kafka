# Results: RabbitMQ (classic / quorum / stream) vs Apache Kafka

| Metric | Classic | Quorum | Stream | Kafka |
|---|---|---|---|---|
| Test 1: consumer A received | 1000 | 1000 | 1000 | 1000 |
| Test 1: consumer B received | 0 | 0 | 1000 | 1000 |
| Test 2: replay possible / messages replayed | No / 0 | No / 0 | Yes / 1000 | Yes / 1000 |
| Test 3: publish rate (msg/s) | 18,353 | 12,431 | 18,348 | 211,479 |
| Test 3: consume rate (msg/s) | 23,027 | 21,335 | 22,181 | 425,258 |
| Test 3b: stream protocol, publish WITH confirms (msg/s) | n/a | n/a | 138,718 | n/a |
| Test 3b: stream protocol, consume (msg/s) | n/a | n/a | 254,773 | n/a |

Tests 1-2: 1,000 messages (identical in all 3 runs). Tests 3/3b: 100,000 messages of 100 bytes, **throughput = average of 3 runs**. Every publish rate counts a message only once the broker has confirmed it. Consume rate = first-to-last message received.

## Methodology

| Target | Client library | Protocol / port | Publish confirmation | Consumer ack / flow control | Batching / linger | Messages |
|---|---|---|---|---|---|---|
| Classic | pika 1.4.4 | AMQP 0-9-1 / 5672 | publisher confirms | manual ack (multiple=True every 500), prefetch 1,000 | one basic_publish per message, async confirms, max 1,000 unconfirmed in flight | 100,000 x 100 B (tests 1-2: 1,000) |
| Quorum | pika 1.4.4 | AMQP 0-9-1 / 5672 | publisher confirms | manual ack (multiple=True every 500), prefetch 1,000 | one basic_publish per message, async confirms, max 1,000 unconfirmed in flight | 100,000 x 100 B (tests 1-2: 1,000) |
| Stream via AMQP | pika 1.4.4 | AMQP 0-9-1 / 5672 | publisher confirms | manual ack (multiple=True every 500), prefetch 1,000, x-stream-offset "first" | one basic_publish per message, async confirms, max 1,000 unconfirmed in flight | 100,000 x 100 B (tests 1-2: 1,000) |
| Stream via stream protocol | rstream 1.1.1 | RabbitMQ Streams / 5552 | stream publish confirms (callback per message) | no per-message ack; credit-based, initial credit 100 chunks, offset first | send_batch of 1,000 messages | 100,000 x 100 B (test 3b only) |
| Kafka | confluent-kafka 2.16.0 | Kafka / 9092 | acks=all (delivery callback) | auto commit, new consumer group, consume() batches of 1,000 | linger.ms=5, librdkafka defaults batch.size 1 MB / batch.num.messages 10,000 | 100,000 x 100 B (tests 1-2: 1,000) |

**rstream confirmation mode (Test 3b):** the publisher uses asynchronous `send_batch()` with an `on_publish_confirm` callback that rstream calls once per message when the broker's PublishConfirm arrives; the timer stops only after the last of the confirmations, so Test 3b publish rates count broker-confirmed messages only (WITH confirms), exactly like the other targets.

Known limits: everything runs on a single machine and tests 1-3b use a single-node broker, so Kafka acks=all and quorum/stream replication have no network or replica cost; the Python clients are implemented differently (pika and rstream are pure Python, confluent-kafka wraps the C library librdkafka), which affects throughput as much as the brokers do.

## Machine and versions

- **Date:** 2026-10-27 18:00
- **OS:** Linux 6.11.0-26-generic
- **CPU:** 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz (8 logical cores)
- **RAM:** 15.3 GB
- **Python:** 3.12.3
- **RabbitMQ version:** 4.4.0
- **Kafka version:** 4.3.1

## Test 4: node failure (3-node RabbitMQ cluster)

| Metric | Classic | Quorum | Stream |
|---|---|---|---|
| Readable while the leader's node is down (of 1,000) | 0 (unavailable: ChannelClosedByBroker) | 1000 | 1000 |
| Readable after the node restarts (of 1,000) | 1000 | 1000 | 1000 |

Classic queues live on a single node: while it is down the queue is unavailable, but the persistent messages are not lost and come back with the node. Quorum queues (Raft) and streams are replicated to all 3 nodes and elect a new leader, so they stay readable.
