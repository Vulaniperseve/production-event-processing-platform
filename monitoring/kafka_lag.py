"""
Streaming consumer lag: how many Kafka messages Spark hasn't processed yet.

Spark Structured Streaming doesn't commit offsets to a Kafka consumer group;
it keeps them in its checkpoint directory:

    <checkpoint>/offsets/<batch>   the offsets a micro-batch will read up to
    <checkpoint>/commits/<batch>   written once that batch has completed

So the processed position is the offsets file of the newest *committed*
batch, and lag = (latest offset in Kafka) - (processed position), per partition.
"""

import json
from pathlib import Path


def _batch_files(directory):
    path = Path(directory)
    if not path.is_dir():
        return {}
    return {int(p.name): p for p in path.iterdir() if p.is_file() and p.name.isdigit()}


def committed_offsets(checkpoint_dir, topic):
    """{partition: offset} processed by the newest completed batch, or {} if none."""
    offsets = _batch_files(Path(checkpoint_dir) / "offsets")
    committed = set(_batch_files(Path(checkpoint_dir) / "commits"))
    done = sorted(committed & set(offsets))
    if not done:
        return {}
    # File layout: "v1", a metadata JSON line, then one JSON line per source.
    for line in offsets[done[-1]].read_text().splitlines()[2:]:
        try:
            source = json.loads(line)
        except ValueError:
            continue
        if isinstance(source, dict) and topic in source:
            return {int(p): int(o) for p, o in source[topic].items()}
    return {}


def offset_resets(end_offsets, processed):
    """
    Partitions whose latest offset is *behind* what Spark already processed.
    That only happens when the topic was deleted and recreated: offsets
    restart at 0, and Spark (failOnDataLoss=false) skips ahead to its old
    position, silently missing every message up to there.
    """
    return sorted(p for p, end in end_offsets.items() if end < processed.get(p, 0))


def consumer_lag(end_offsets, processed):
    """
    Messages waiting, summed over partitions. A partition never read counts
    from 0, and after an offset reset every message in it counts as waiting:
    clamping to 0 there would hide exactly the messages that are being lost.
    """
    resets = set(offset_resets(end_offsets, processed))
    return sum(end if p in resets else max(end - processed.get(p, 0), 0)
               for p, end in end_offsets.items())


def kafka_end_offsets(bootstrap, topic, timeout=5.0):
    """{partition: latest offset} from the broker."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "platform-lag-exporter",
                         "enable.auto.commit": False})
    try:
        metadata = consumer.list_topics(topic, timeout=timeout)
        partitions = metadata.topics[topic].partitions if topic in metadata.topics else {}
        ends = {}
        for p in partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout)
            ends[p] = high
        return ends
    finally:
        consumer.close()
