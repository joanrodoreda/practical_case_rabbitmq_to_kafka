# RabbitMQ (classic / quorum / stream) vs Apache Kafka — practical case

Small benchmark that runs the same tests against four targets: a RabbitMQ **classic** queue,
a RabbitMQ **quorum** queue, a RabbitMQ **stream** and a **Kafka** topic. Every test uses a
fresh queue/topic so results don't mix.

## Tests

| Test | What it does | What it shows |
|---|---|---|
| 1. Read twice | Publish 1,000 messages. Consumer A reads and acks everything, then a separate consumer B does the same (Kafka: two different consumer groups, earliest offset). | **Destructive vs non-destructive consumption.** Classic/quorum queues delete a message once it is acked, so B gets 0. Streams and Kafka are append-only logs, so B also gets 1,000. |
| 2. Replay | A new consumer tries to re-read the test-1 messages from the beginning (stream: `x-stream-offset = "first"`; Kafka: new group with `auto.offset.reset = earliest`). | **Replay** is only possible on logs (stream, Kafka). Classic/quorum queues return 0. |
| 3. Throughput | Publish 100,000 messages of 100 bytes, then consume them all (AMQP 0-9-1 for every RabbitMQ queue type). | Indicative publish and consume rates (msg/s). |
| 3b. Stream protocol | Test 3 again for the RabbitMQ stream only, over the native stream protocol (`rstream`, port 5552). | How much faster streams are with their dedicated protocol (batched publishing, chunk-based delivery). |
| 4. Node failure (optional) | 3-node RabbitMQ cluster. Publish 1,000 messages to a classic queue, a quorum queue and a stream, stop the node hosting each leader and count readable messages (without acking), then restart the node and count again. | Classic queues live on one node: unavailable while it is down, but not lost. Quorum queues (Raft) and streams are replicated and stay available. |

The whole suite (tests 1-3b) runs 3 times; throughput rows are the **average of 3 runs**
(tests 1-2 must give the same result in every run, otherwise the script stops).

## How to run

```bash
docker compose up -d                       # RabbitMQ 4 (5672, UI 15672, streams 5552) + Kafka KRaft (9092)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Optional Test 4 first, so it is included in results.md / results.csv
docker compose -f docker-compose-cluster.yaml up -d   # rabbit1..3 on ports 5673-5675
python test4_node_failure.py                          # -> results_test4.md, results_test4.csv
docker compose -f docker-compose-cluster.yaml down

python run_all.py                          # tests 1-3b x 3 runs -> results.md, results.csv (~3 min)
python make_figures.py                     # -> figures/throughput.png, figures/behaviour.png
python demo_queues.py                      # optional: demo-classic/quorum/stream for UI screenshots
docker compose down
```

The scripts wait and retry until the brokers are ready, so they can run right after `up -d`.
After `demo_queues.py`, the management UI (http://localhost:15672, guest/guest) shows
demo-classic and demo-quorum with 0 messages and demo-stream still holding 1,000.

## Methodology notes

- **Every publish rate counts confirmed messages only**: RabbitMQ AMQP uses publisher confirms
  (asynchronous, up to 1,000 unconfirmed in flight), the stream protocol uses stream publish
  confirms (callback per message), Kafka uses `acks=all`. The timer stops at the last confirmation.
- **Consume rate** is measured from the first to the last message received (excludes connection
  setup and Kafka consumer-group join). AMQP consumers use manual acks (`multiple=True` every
  500) with prefetch 1,000; Kafka uses auto commit; the stream protocol uses credit-based flow
  control (no per-message ack).
- Kafka producer: `linger.ms=5`, librdkafka default batching. Topics have 1 partition.
- **Known limits:** single machine and single-node brokers for tests 1-3b (Kafka `acks=all` and
  quorum/stream replication have no network cost); the Python clients differ in implementation
  (`pika` and `rstream` are pure Python, `confluent-kafka` wraps the C library librdkafka), which
  affects throughput as much as the brokers. Read the numbers as indicative.
- `results.md` contains the full methodology table plus the machine and broker versions used.

## Files

- `run_all.py` – tests 1-3b (one function per test), averaging and the report.
- `rabbit_client.py`, `kafka_client.py`, `stream_protocol_client.py` – small publish/consume helpers.
- `test4_node_failure.py`, `docker-compose-cluster.yaml`, `cluster_rabbitmq.conf` – optional Test 4.
- `make_figures.py` – report figures from `results.csv`.
- `demo_queues.py` – demo queues for management-UI screenshots.
- `docker-compose.yaml`, `rabbitmq_enabled_plugins`, `rabbitmq_stream.conf` – single-node brokers
  (stream plugin enabled, advertised host `localhost`).
- `results.md`, `results.csv`, `results_test4.*`, `figures/` – results of the last run.
