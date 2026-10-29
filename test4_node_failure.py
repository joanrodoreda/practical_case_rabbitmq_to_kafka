"""Test 4 (optional): node failure on a 3-node RabbitMQ cluster: classic vs quorum vs stream.

Usage: docker compose -f docker-compose-cluster.yaml up -d
       python test4_node_failure.py
Publishes 1,000 messages to a classic queue, a quorum queue and a stream, stops the node
hosting each one's leader, counts how many messages can still be read through a surviving
node, then restarts the node and counts again. Writes results_test4.md and results_test4.csv (both included in results.md /
results.csv by run_all.py).
"""
import base64
import csv
import json
import os
import subprocess
import time
import urllib.parse
import urllib.request

import pika

import rabbit_client

MESSAGES = 1_000
PAYLOAD = b"x" * 100
# node name -> (AMQP port, management port) as mapped in docker-compose-cluster.yaml
NODES = {"rabbit1": (5673, 15673), "rabbit2": (5674, 15674), "rabbit3": (5675, 15675)}
RUN_ID = time.strftime("%H%M%S")


def amqp_params(node):
    return pika.ConnectionParameters(host="localhost", port=NODES[node][0], heartbeat=0,
                                     connection_attempts=3, retry_delay=2)


def queue_leader(queue_name, via_node):
    """Ask the management API which node hosts the queue (its leader)."""
    url = (f"http://localhost:{NODES[via_node][1]}/api/queues/%2F/"
           + urllib.parse.quote(queue_name, safe=""))
    request = urllib.request.Request(url, headers={
        "Authorization": "Basic " + base64.b64encode(b"guest:guest").decode()})
    info = json.load(urllib.request.urlopen(request, timeout=10))
    return (info.get("leader") or info["node"]).split("@")[1]  # 'rabbit@rabbit2' -> 'rabbit2'


def wait_for_cluster(timeout_s=180):
    """Retry until every node accepts AMQP connections."""
    deadline = time.time() + timeout_s
    for node in NODES:
        while True:
            try:
                pika.BlockingConnection(amqp_params(node)).close()
                break
            except Exception:
                if time.time() > deadline:
                    raise RuntimeError(f"cluster node {node} not reachable")
                print(f"  waiting for {node} ...")
                time.sleep(3)


def count_readable(name, queue_type, node):
    """Count messages readable via `node` without acking them (they stay in the queue)."""
    try:
        readable, _ = rabbit_client.consume(name, queue_type, MESSAGES,
                                            params=amqp_params(node), ack=False)
        return str(readable)
    except pika.exceptions.AMQPError as error:
        return f"0 (unavailable: {type(error).__name__})"


def test4_node_failure(queue_type):
    """Publish, stop the leader's node, count readable messages; restart it and count again."""
    name = f"t4-{queue_type}-{RUN_ID}"
    rabbit_client.declare_queue(name, queue_type, params=amqp_params("rabbit1"))
    rabbit_client.publish(name, MESSAGES, PAYLOAD, params=amqp_params("rabbit1"))
    leader = queue_leader(name, "rabbit1")
    survivor = next(node for node in NODES if node != leader)
    print(f"  {queue_type}: leader on {leader}, stopping it; reading via {survivor}")

    subprocess.run(["docker", "stop", leader], check=True, capture_output=True)
    time.sleep(10)  # give the cluster time to notice (and quorum/stream time to elect a new leader)
    try:
        during_failure = count_readable(name, queue_type, survivor)
    finally:
        subprocess.run(["docker", "start", leader], check=True, capture_output=True)
        wait_for_cluster()
    time.sleep(10)  # let the restarted node recover its queues
    after_restart = count_readable(name, queue_type, survivor)
    print(f"  {queue_type}: readable during failure = {during_failure}, after restart = {after_restart}")
    return during_failure, after_restart


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("Waiting for the 3-node cluster ...")
    wait_for_cluster()
    results = {queue_type: test4_node_failure(queue_type)
               for queue_type in ("classic", "quorum", "stream")}
    during = " | ".join(results[queue_type][0] for queue_type in results)
    after = " | ".join(results[queue_type][1] for queue_type in results)
    section = (
        "## Test 4: node failure (3-node RabbitMQ cluster)\n\n"
        "| Metric | Classic | Quorum | Stream |\n|---|---|---|---|\n"
        f"| Readable while the leader's node is down (of {MESSAGES:,}) | {during} |\n"
        f"| Readable after the node restarts (of {MESSAGES:,}) | {after} |\n\n"
        "Classic queues live on a single node: while it is down the queue is unavailable, but the "
        "persistent messages are not lost and come back with the node. Quorum queues (Raft) and "
        "streams are replicated to all 3 nodes and elect a new leader, so they stay readable.\n")
    with open("results_test4.md", "w") as section_file:
        section_file.write(section)
    count = lambda outcome: outcome.split()[0]  # "0 (unavailable: ...)" -> "0"
    with open("results_test4.csv", "w", newline="") as csv_file:
        csv.writer(csv_file).writerows([
            ["Metric", "Classic", "Quorum", "Stream"],
            ["Test 4: readable during node failure"] + [count(r[0]) for r in results.values()],
            ["Test 4: readable after node restart"] + [count(r[1]) for r in results.values()],
        ])
    print("\n" + section + "\nSaved results_test4.md and results_test4.csv")


if __name__ == "__main__":
    main()
