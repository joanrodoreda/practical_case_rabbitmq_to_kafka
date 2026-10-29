## Test 4: node failure (3-node RabbitMQ cluster)

| Metric | Classic | Quorum | Stream |
|---|---|---|---|
| Readable while the leader's node is down (of 1,000) | 0 (unavailable: ChannelClosedByBroker) | 1000 | 1000 |
| Readable after the node restarts (of 1,000) | 1000 | 1000 | 1000 |

Classic queues live on a single node: while it is down the queue is unavailable, but the persistent messages are not lost and come back with the node. Quorum queues (Raft) and streams are replicated to all 3 nodes and elect a new leader, so they stay readable.
