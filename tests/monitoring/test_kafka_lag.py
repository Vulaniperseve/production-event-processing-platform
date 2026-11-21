import json

from monitoring.kafka_lag import committed_offsets, consumer_lag, offset_resets

TOPIC = "market.price_ticks"


def write_batch(checkpoint, batch, offsets, committed=True):
    (checkpoint / "offsets").mkdir(parents=True, exist_ok=True)
    (checkpoint / "commits").mkdir(parents=True, exist_ok=True)
    (checkpoint / "offsets" / str(batch)).write_text(
        "v1\n" + json.dumps({"batchWatermarkMs": 0, "conf": {}}) + "\n" + json.dumps({TOPIC: offsets}))
    if committed:
        (checkpoint / "commits" / str(batch)).write_text("v1\n{}")


def test_uses_the_newest_completed_batch(tmp_path):
    write_batch(tmp_path, 7, {"0": 100, "1": 200})
    write_batch(tmp_path, 9, {"0": 150, "1": 260})
    write_batch(tmp_path, 10, {"0": 999, "1": 999}, committed=False)   # still running

    assert committed_offsets(tmp_path, TOPIC) == {0: 150, 1: 260}


def test_no_checkpoint_yet(tmp_path):
    assert committed_offsets(tmp_path / "missing", TOPIC) == {}
    write_batch(tmp_path, 0, {"0": 5}, committed=False)
    assert committed_offsets(tmp_path, TOPIC) == {}


def test_ignores_hidden_and_temp_files(tmp_path):
    write_batch(tmp_path, 3, {"0": 30})
    (tmp_path / "offsets" / ".3.crc").write_text("x")
    (tmp_path / "offsets" / "4.tmp").write_text("x")
    assert committed_offsets(tmp_path, TOPIC) == {0: 30}


def test_other_topics_are_ignored(tmp_path):
    (tmp_path / "offsets").mkdir()
    (tmp_path / "commits").mkdir()
    (tmp_path / "offsets" / "0").write_text('v1\n{}\n{"other.topic": {"0": 5}}')
    (tmp_path / "commits" / "0").write_text("v1")
    assert committed_offsets(tmp_path, TOPIC) == {}


def test_consumer_lag():
    assert consumer_lag({0: 150, 1: 300, 2: 0}, {0: 150, 1: 260}) == 40
    assert consumer_lag({0: 50}, {}) == 50                 # never read: everything is lag


def test_topic_recreated_counts_every_message_as_waiting():
    # Real incident: Kafka was recreated without a volume, its offsets
    # restarted at 0 (now 390/780) while Spark's checkpoint said 780/1560.
    end, processed = {0: 390, 1: 780, 2: 0}, {0: 780, 1: 1560, 2: 0}
    assert offset_resets(end, processed) == [0, 1]
    assert consumer_lag(end, processed) == 1170            # not 0: these would be lost
    assert offset_resets({0: 900}, {0: 780}) == []
