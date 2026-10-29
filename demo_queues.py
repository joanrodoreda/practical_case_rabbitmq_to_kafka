"""Create demo queues for management-UI screenshots (single-node broker, http://localhost:15672).

Usage: python demo_queues.py
Publishes 1,000 messages to demo-classic, demo-quorum and demo-stream, then consumes and
acks demo-classic and demo-quorum once and reads demo-stream once from offset "first".
Afterwards the UI shows: classic 0 messages, quorum 0 messages, stream still 1,000.
"""
import rabbit_client

MESSAGES = 1_000
PAYLOAD = b"x" * 100
DEMO_QUEUES = {"demo-classic": "classic", "demo-quorum": "quorum", "demo-stream": "stream"}


def main():
    rabbit_client.wait_for_rabbitmq()
    for queue_name, queue_type in DEMO_QUEUES.items():
        rabbit_client.delete_queue(queue_name)  # start from a clean queue on every run
        rabbit_client.declare_queue(queue_name, queue_type)
        rabbit_client.publish(queue_name, MESSAGES, PAYLOAD)
        read, _ = rabbit_client.consume(queue_name, queue_type, MESSAGES)  # acks every message
        print(f"{queue_name:13} ({queue_type}): published {MESSAGES}, read and acked {read}")
    print("Ready: open http://localhost:15672 (guest/guest) -> Queues and Streams")


if __name__ == "__main__":
    main()
