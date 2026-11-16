"""
S3 publish tests against moto's in-memory S3 (no AWS account, no network).
"""

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from src.lake.s3_sync import (  # noqa: E402
    UnsafeSyncError,
    is_data_file,
    plan_layer,
    sync_lake_to_s3,
)

BUCKET = "test-lake-bucket"
REGION = "eu-west-1"


@pytest.fixture
def s3(monkeypatch):
    # Fake credentials: makes sure nothing can ever reach a real account.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with moto.mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        yield client


def write(root, rel, content):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content if isinstance(content, bytes) else content.encode())
    return path


def keys(client, prefix=""):
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    return sorted(obj["Key"] for page in pages for obj in page.get("Contents", []))


@pytest.fixture
def lake(tmp_path):
    root = tmp_path / "lake"
    write(root, "bronze/t/ingest_date=2026-11-15/raw_event_id=000000002.json", '{"a": 1}')
    write(root, "silver/m/symbol=AAPL/trade_month=2026-11/part-0.parquet", b"PAR1aapl")
    write(root, "silver/m/symbol=MSFT/trade_month=2026-11/part-0.parquet", b"PAR1msft")
    write(root, "gold/daily/symbol=AAPL/part-0.parquet", b"PAR1gold")
    # Spark / writer bookkeeping that must never be published:
    write(root, "gold/daily/_SUCCESS", "")
    write(root, "gold/daily/symbol=AAPL/.part-0.parquet.crc", "crc")
    write(root, "bronze/t/ingest_date=2026-11-15/.raw_event_id=000000003.json.tmp", "half")
    write(root, "silver/m/_temporary/0/part-9.parquet", b"PAR1tmp")
    return root


def sync(s3, lake, **kwargs):
    return sync_lake_to_s3(client=s3, bucket=BUCKET, prefix="lake", root=lake, **kwargs)


@pytest.mark.parametrize("rel, expected", [
    ("silver/m/symbol=AAPL/trade_month=2026-11/part-0.parquet", True),
    ("gold/daily/_SUCCESS", False),
    ("gold/daily/symbol=AAPL/.part-0.parquet.crc", False),
    ("silver/m/_temporary/0/part-9.parquet", False),
    ("bronze/t/ingest_date=2026-11-15/.raw_event_id=3.json.tmp", False),
])
def test_is_data_file(rel, expected):
    assert is_data_file(rel) is expected


def test_plan_layer_is_a_pure_diff(tmp_path):
    same = write(tmp_path, "same", "x")
    changed = write(tmp_path, "changed", "new")
    new = write(tmp_path, "new", "n")
    remote = {
        "same": "9dd4e461268c8034f5c8564e155c67a6",   # md5("x")
        "changed": "0" * 32,
        "gone": "1" * 32,
    }
    local = {"same": same, "changed": changed, "new": new}

    assert plan_layer(local, remote) == (["changed", "new"], ["gone"])


def test_first_sync_uploads_only_data_files(s3, lake):
    result = sync(s3, lake)

    assert keys(s3) == [
        "lake/bronze/t/ingest_date=2026-11-15/raw_event_id=000000002.json",
        "lake/gold/daily/symbol=AAPL/part-0.parquet",
        "lake/silver/m/symbol=AAPL/trade_month=2026-11/part-0.parquet",
        "lake/silver/m/symbol=MSFT/trade_month=2026-11/part-0.parquet",
    ]
    assert result["uploaded"] == 4 and result["deleted"] == 0
    assert result["bytes_uploaded"] == len('{"a": 1}') + 3 * 8

    body = s3.get_object(Bucket=BUCKET, Key="lake/gold/daily/symbol=AAPL/part-0.parquet")
    assert body["Body"].read() == b"PAR1gold"
    assert body["ServerSideEncryption"] == "AES256"


def test_second_sync_is_a_no_op(s3, lake):
    sync(s3, lake)
    result = sync(s3, lake)

    assert result["uploaded"] == 0
    assert result["deleted"] == 0
    assert result["unchanged"] == 4


def test_rewritten_partition_replaces_old_files(s3, lake):
    """Spark's partition overwrite writes new file names; S3 must not keep the old ones."""
    sync(s3, lake)
    old = lake / "silver/m/symbol=AAPL/trade_month=2026-11/part-0.parquet"
    old.unlink()
    write(lake, "silver/m/symbol=AAPL/trade_month=2026-11/part-1.parquet", b"PAR1aapl-v2")

    result = sync(s3, lake)

    assert keys(s3, "lake/silver/m/symbol=AAPL/") == [
        "lake/silver/m/symbol=AAPL/trade_month=2026-11/part-1.parquet"
    ]
    assert result["layers"]["silver"] == {
        "uploaded": 1, "deleted": 1, "unchanged": 1, "bytes_uploaded": len(b"PAR1aapl-v2"),
    }


def test_changed_content_is_reuploaded(s3, lake):
    sync(s3, lake)
    write(lake, "gold/daily/symbol=AAPL/part-0.parquet", b"PAR1gold-v2")

    result = sync(s3, lake)

    assert result["layers"]["gold"]["uploaded"] == 1
    body = s3.get_object(Bucket=BUCKET, Key="lake/gold/daily/symbol=AAPL/part-0.parquet")
    assert body["Body"].read() == b"PAR1gold-v2"


def test_dry_run_changes_nothing(s3, lake):
    result = sync(s3, lake, dry_run=True)

    assert result["uploaded"] == 4
    assert keys(s3) == []


def test_objects_outside_the_lake_prefix_are_never_touched(s3, lake):
    s3.put_object(Bucket=BUCKET, Key="athena-results/query-1.csv", Body=b"x")
    s3.put_object(Bucket=BUCKET, Key="lake-backup/gold/old.parquet", Body=b"x")

    sync(s3, lake)

    assert "athena-results/query-1.csv" in keys(s3)
    assert "lake-backup/gold/old.parquet" in keys(s3)


def test_empty_local_layer_refuses_to_wipe_s3(s3, lake, tmp_path):
    sync(s3, lake)
    empty = tmp_path / "unmounted"
    empty.mkdir()

    with pytest.raises(UnsafeSyncError, match="bronze"):
        sync_lake_to_s3(client=s3, bucket=BUCKET, prefix="lake", root=empty)

    assert len(keys(s3)) == 4   # nothing deleted, not even in other layers


def test_bucket_and_prefix_come_from_the_environment(s3, lake, monkeypatch):
    monkeypatch.setenv("LAKE_S3_BUCKET", BUCKET)
    monkeypatch.setenv("LAKE_S3_PREFIX", "/env-lake/")

    result = sync_lake_to_s3(client=s3, root=lake)

    assert result["prefix"] == "env-lake"
    assert all(k.startswith("env-lake/") for k in keys(s3))


def test_missing_bucket_or_empty_prefix_is_rejected(s3, lake, monkeypatch):
    monkeypatch.delenv("LAKE_S3_BUCKET", raising=False)
    with pytest.raises(ValueError, match="LAKE_S3_BUCKET"):
        sync_lake_to_s3(client=s3, root=lake)

    with pytest.raises(ValueError, match="PREFIX"):
        sync_lake_to_s3(client=s3, bucket=BUCKET, prefix="/", root=lake)
