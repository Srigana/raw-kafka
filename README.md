# raw-kafka

A simplified Kafka implementation built from scratch in Python 
to understand how the real thing works internally.

## What's built

- Binary wire protocol using Python's `struct` module
- ZooKeeper integration for broker registration and failure detection
- Append-only log segment storage on disk
- Multi-threaded broker handling produce, fetch, and metadata requests
- High-level producer and consumer APIs

## Running it

```bash
pip install kazoo
zkServer start
python -m tests.test_system
```

## Key takeaways

- How Kafka's binary protocol works at the byte level
- Why broker nodes in ZooKeeper are ephemeral but topic nodes are persistent
- Why append-only writes are central to Kafka's performance
- How the broker decouples producers and consumers completely
