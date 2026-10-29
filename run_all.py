"""Run tests 1-3b against RabbitMQ classic / quorum / stream queues and a Kafka topic.

Usage: python run_all.py   (brokers must be running: docker compose up -d)
Writes results.md and results.csv next to this file. If results_test4.csv/.md exist
(written by test4_node_failure.py) their rows are included too.
"""
import csv
import datetime
import os
import platform
import subprocess
import time
from importlib.metadata import version as package_version

import kafka_client
import rabbit_client
import stream_protocol_client

TEST1_MESSAGES = 1_000
TEST3_MESSAGES = 100_000
RUNS = 3  # the whole suite runs RUNS times; throughput rows are averaged
PAYLOAD = b"x" * 100  # ~100-byte message body, identical for every target
RUN_ID = time.strftime("%H%M%S")  # suffix so every run uses fresh queues/topics


class RabbitTarget:
    def __init__(self, queue_type):
        self.queue_type = queue_type
        self.label = queue_type.capitalize()

    def create(self, name):
        rabbit_client.declare_queue(name, self.queue_type)

    def delete(self, name):
        rabbit_client.delete_queue(name)

    def publish(self, name, count):
        return rabbit_client.publish(name, count, PAYLOAD)

    def consume(self, name, max_count):
        return rabbit_client.consume(name, self.queue_type, max_count)


class KafkaTarget:
    label = "Kafka"

    def create(self, name):
        kafka_client.create_topic(name)

    def delete(self, name):
        kafka_client.delete_topic(name)

    def publish(self, name, count):
        return kafka_client.publish(name, count, PAYLOAD)

    def consume(self, name, max_count):
        return kafka_client.consume(name, max_count)  # each call = new consumer group


TARGETS = [RabbitTarget("classic"), RabbitTarget("quorum"), RabbitTarget("stream"), KafkaTarget()]


# ---------------------------------------------------------------- tests

def test1_read_twice(target, run):
    """Publish 1,000 messages; consumer A reads all it can, then consumer B does the same."""
    name = f"t1-{target.label.lower()}-{RUN_ID}-r{run}"
    target.create(name)
    target.publish(name, TEST1_MESSAGES)
    received_a, _ = target.consume(name, TEST1_MESSAGES)
    received_b, _ = target.consume(name, TEST1_MESSAGES)
    return name, received_a, received_b


def test2_replay(target, name):
    """Try to read the test-1 messages again from the beginning with a brand-new consumer."""
    replayed, _ = target.consume(name, TEST1_MESSAGES)
    target.delete(name)  # keep the broker (and its management UI) clean
    return replayed


def test3_throughput(target, run):
    """Publish then consume 100,000 x 100-byte messages; return (publish msg/s, consume msg/s)."""
    name = f"t3-{target.label.lower()}-{RUN_ID}-r{run}"
    target.create(name)
    publish_seconds = target.publish(name, TEST3_MESSAGES)
    consumed, consume_seconds = target.consume(name, TEST3_MESSAGES)
    target.delete(name)
    if consumed != TEST3_MESSAGES:
        raise RuntimeError(f"{target.label}: consumed {consumed}/{TEST3_MESSAGES}")
    return TEST3_MESSAGES / publish_seconds, TEST3_MESSAGES / consume_seconds


def test3b_stream_protocol(run):
    """Test 3 again for the RabbitMQ stream, over the native stream protocol (port 5552).

    The publish timer stops only when the broker has confirmed the last message.
    """
    name = f"t3b-stream-{RUN_ID}-r{run}"
    publish_seconds = stream_protocol_client.publish(name, TEST3_MESSAGES, PAYLOAD)
    consume_seconds = stream_protocol_client.consume(name, TEST3_MESSAGES)  # also deletes it
    return TEST3_MESSAGES / publish_seconds, TEST3_MESSAGES / consume_seconds


# ---------------------------------------------------------------- environment info

def kafka_version():
    """Ask the Kafka container for its version (e.g. '4.1.0')."""
    try:
        output = subprocess.run(
            ["docker", "exec", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--version"],
            capture_output=True, text=True, timeout=60).stdout
        return output.strip().split()[0]
    except Exception:
        return "unknown"


def machine_details(rabbit_version):
    cpu = platform.processor() or platform.machine()
    try:
        with open("/proc/cpuinfo") as cpuinfo:
            cpu = next(line.split(":", 1)[1].strip() for line in cpuinfo if line.startswith("model name"))
    except (OSError, StopIteration):
        pass
    try:
        ram_gb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024 ** 3
        ram = f"{ram_gb:.1f} GB"
    except (ValueError, OSError, AttributeError):
        ram = "unknown"
    return {
        "Date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "OS": f"{platform.system()} {platform.release()}",
        "CPU": f"{cpu} ({os.cpu_count()} logical cores)",
        "RAM": ram,
        "Python": platform.python_version(),
        "RabbitMQ version": rabbit_version,
        "Kafka version": kafka_version(),
    }


# ---------------------------------------------------------------- reporting

def build_table(results, rate):
    """rate: formatter for msg/s values (with thousands separators for Markdown, plain for CSV)."""
    header = ["Metric"] + [target.label for target in TARGETS]

    def row(metric, key, fmt):
        values = [results[target.label].get(key) for target in TARGETS]
        return [metric] + ["n/a" if value is None else fmt(value) for value in values]

    replay = lambda count: f"{'Yes' if count else 'No'} / {count}"
    rows = [
        row("Test 1: consumer A received", "a", str),
        row("Test 1: consumer B received", "b", str),
        row("Test 2: replay possible / messages replayed", "replay", replay),
        row("Test 3: publish rate (msg/s)", "publish_rate", rate),
        row("Test 3: consume rate (msg/s)", "consume_rate", rate),
        row("Test 3b: stream protocol, publish WITH confirms (msg/s)", "native_publish_rate", rate),
        row("Test 3b: stream protocol, consume (msg/s)", "native_consume_rate", rate),
    ]
    return header, rows


def markdown_table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def methodology_table():
    """One row per target: client, protocol, confirms, consumer acks, batching, messages."""
    pika_v, rstream_v, kafka_v = (package_version(name) for name in ("pika", "rstream", "confluent-kafka"))
    messages = f"{TEST3_MESSAGES:,} x {len(PAYLOAD)} B (tests 1-2: {TEST1_MESSAGES:,})"
    amqp_publish = (f"one basic_publish per message, async confirms, max "
                    f"{rabbit_client.CONFIRM_WINDOW:,} unconfirmed in flight")
    amqp_consume = (f"manual ack (multiple=True every {rabbit_client.ACK_BATCH}), "
                    f"prefetch {rabbit_client.PREFETCH:,}")
    header = ["Target", "Client library", "Protocol / port", "Publish confirmation",
              "Consumer ack / flow control", "Batching / linger", "Messages"]
    rows = [
        ["Classic", f"pika {pika_v}", "AMQP 0-9-1 / 5672", "publisher confirms",
         amqp_consume, amqp_publish, messages],
        ["Quorum", f"pika {pika_v}", "AMQP 0-9-1 / 5672", "publisher confirms",
         amqp_consume, amqp_publish, messages],
        ["Stream via AMQP", f"pika {pika_v}", "AMQP 0-9-1 / 5672", "publisher confirms",
         amqp_consume + ', x-stream-offset "first"', amqp_publish, messages],
        ["Stream via stream protocol", f"rstream {rstream_v}", "RabbitMQ Streams / 5552",
         "stream publish confirms (callback per message)",
         f"no per-message ack; credit-based, initial credit {stream_protocol_client.INITIAL_CREDIT} chunks, "
         "offset first", f"send_batch of {stream_protocol_client.PUBLISH_BATCH:,} messages",
         f"{TEST3_MESSAGES:,} x {len(PAYLOAD)} B (test 3b only)"],
        ["Kafka", f"confluent-kafka {kafka_v}", "Kafka / 9092", "acks=all (delivery callback)",
         "auto commit, new consumer group, consume() batches of 1,000",
         "linger.ms=5, librdkafka defaults batch.size 1 MB / batch.num.messages 10,000",
         messages],
    ]
    return markdown_table(header, rows)


def load_test4_rows():
    """Rows from test4_node_failure.py (Kafka was not part of Test 4)."""
    if not os.path.exists("results_test4.csv"):
        return []
    with open("results_test4.csv") as test4_file:
        rows = list(csv.reader(test4_file))[1:]
    return [row + ["not tested"] for row in rows]


def save_results(results, details):
    header, csv_rows = build_table(results, rate=lambda value: f"{value:.0f}")
    with open("results.csv", "w", newline="") as csv_file:
        csv.writer(csv_file).writerows([header] + csv_rows + load_test4_rows())
    header, md_rows = build_table(results, rate=lambda value: f"{value:,.0f}")
    with open("results.md", "w") as md_file:
        md_file.write("# Results: RabbitMQ (classic / quorum / stream) vs Apache Kafka\n\n")
        md_file.write(markdown_table(header, md_rows) + "\n\n")
        md_file.write(f"Tests 1-2: {TEST1_MESSAGES:,} messages (identical in all {RUNS} runs). "
                      f"Tests 3/3b: {TEST3_MESSAGES:,} messages of {len(PAYLOAD)} bytes, "
                      f"**throughput = average of {RUNS} runs**. Every publish rate counts a message "
                      "only once the broker has confirmed it. Consume rate = first-to-last message "
                      "received.\n\n")
        md_file.write("## Methodology\n\n" + methodology_table() + "\n\n")
        md_file.write("**rstream confirmation mode (Test 3b):** the publisher uses asynchronous "
                      "`send_batch()` with an `on_publish_confirm` callback that rstream calls once "
                      "per message when the broker's PublishConfirm arrives; the timer stops only "
                      "after the last of the confirmations, so Test 3b publish rates count "
                      "broker-confirmed messages only (WITH confirms), exactly like the other "
                      "targets.\n\n")
        md_file.write("Known limits: everything runs on a single machine and tests 1-3b use a "
                      "single-node broker, so Kafka acks=all and quorum/stream replication have no "
                      "network or replica cost; the Python clients are implemented differently "
                      "(pika and rstream are pure Python, confluent-kafka wraps the C library "
                      "librdkafka), which affects throughput as much as the brokers do.\n\n")
        md_file.write("## Machine and versions\n\n")
        md_file.writelines(f"- **{key}:** {value}\n" for key, value in details.items())
        if os.path.exists("results_test4.md"):  # optional Test 4, see test4_node_failure.py
            with open("results_test4.md") as test4_file:
                md_file.write("\n" + test4_file.read())


def average_runs(runs):
    """Average the throughput metrics over all runs; tests 1-2 must be identical in every run."""
    averaged = {}
    for label, first_run in runs[0].items():
        averaged[label] = {}
        for key, value in first_run.items():
            values = [run[label][key] for run in runs]
            if key.endswith("_rate"):
                averaged[label][key] = sum(values) / len(values)
            elif len(set(values)) > 1:
                raise RuntimeError(f"{label} {key} differs between runs: {values}")
            else:
                averaged[label][key] = value
    return averaged


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("Waiting for brokers ...")
    rabbit_version = rabbit_client.wait_for_rabbitmq()
    kafka_client.wait_for_kafka()

    runs = []  # one {label: {metric: value}} dict per run
    for run in range(1, RUNS + 1):
        run_results = {}
        for target in TARGETS:
            print(f"\n=== Run {run}/{RUNS}: {target.label} ===")
            name, received_a, received_b = test1_read_twice(target, run)
            print(f"  Test 1: A={received_a}  B={received_b}")
            replayed = test2_replay(target, name)
            print(f"  Test 2: replayed={replayed}")
            publish_rate, consume_rate = test3_throughput(target, run)
            print(f"  Test 3: publish={publish_rate:,.0f} msg/s  consume={consume_rate:,.0f} msg/s")
            run_results[target.label] = {"a": received_a, "b": received_b, "replay": replayed,
                                         "publish_rate": publish_rate, "consume_rate": consume_rate}
        native_publish, native_consume = test3b_stream_protocol(run)
        print(f"  Test 3b (stream protocol): publish={native_publish:,.0f} msg/s  "
              f"consume={native_consume:,.0f} msg/s")
        run_results["Stream"].update(native_publish_rate=native_publish,
                                     native_consume_rate=native_consume)
        runs.append(run_results)

    results = average_runs(runs)
    details = machine_details(rabbit_version)
    save_results(results, details)
    print("\n" + markdown_table(*build_table(results, rate=lambda value: f"{value:,.0f}")))
    print(f"(throughput rows: average of {RUNS} runs)")
    print("\n" + "\n".join(f"{key}: {value}" for key, value in details.items()))
    print("\nSaved results.md and results.csv")


if __name__ == "__main__":
    main()
