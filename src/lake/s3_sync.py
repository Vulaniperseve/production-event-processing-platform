"""
Publish the local data lake to Amazon S3.

Spark runs on the local machine (cheap, no cluster to pay for), while S3 is
the durable, shared copy of the lake that other tools read, for example
Athena querying the gold tables with SQL.

The sync is incremental and idempotent:

* a file is uploaded only when it is missing in S3 or its MD5 differs from
  the object's ETag (objects are written with a single PUT and SSE-S3
  encryption, so the ETag is the MD5 of the content);
* objects whose local file no longer exists are deleted. Spark's partition
  overwrite gives rewritten partitions new file names, so without this step
  S3 would keep the old files and readers would count rows twice;
* uploads happen before deletes, so a reader never sees a partition with
  its data missing (at worst it briefly sees old and new files together;
  table formats such as Iceberg or Delta Lake remove that window with
  atomic commits);
* running it twice in a row does nothing the second time.

A guard refuses to delete a layer's objects when that layer is empty
locally, so an unmounted or wiped local lake cannot empty the bucket.
"""

import base64
import hashlib
import logging
import os
from pathlib import Path

from src.lake.config import lake_root

log = logging.getLogger(__name__)

LAYERS = ("bronze", "silver", "gold")

CONTENT_TYPES = {".json": "application/json", ".parquet": "application/vnd.apache.parquet"}


class UnsafeSyncError(RuntimeError):
    """The sync would delete a whole layer from S3."""


def s3_target():
    """(bucket, prefix) from the environment, or (None, prefix) if not configured."""
    bucket = os.getenv("LAKE_S3_BUCKET") or None
    prefix = os.getenv("LAKE_S3_PREFIX", "lake").strip("/")
    return bucket, prefix


def is_data_file(rel_path):
    """
    Skip Spark bookkeeping (_SUCCESS, _temporary/) and hidden files
    (.crc checksums, bronze's .tmp files that are still being written).
    """
    return not any(part.startswith(("_", ".")) for part in Path(rel_path).parts)


def file_md5(path):
    # A checksum to compare with S3's ETag, not a security use of MD5.
    digest = hashlib.md5(usedforsecurity=False)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest


def local_files(root, layer):
    """{relative posix path: local Path} for one layer, data files only."""
    base = Path(root) / layer
    if not base.is_dir():
        return {}
    files = {}
    for path in base.rglob("*"):
        rel = path.relative_to(root).as_posix()
        if path.is_file() and is_data_file(rel):
            files[rel] = path
    return files


def remote_objects(client, bucket, prefix, layer):
    """{relative posix path: etag without quotes} for one layer."""
    objects = {}
    key_prefix = f"{prefix}/{layer}/"
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            objects[obj["Key"][len(prefix) + 1:]] = obj["ETag"].strip('"')
    return objects


def plan_layer(local, remote):
    """Pure diff: which relative paths to upload and which to delete."""
    to_upload = sorted(
        rel for rel, path in local.items()
        if remote.get(rel) != file_md5(path).hexdigest()
    )
    to_delete = sorted(set(remote) - set(local))
    return to_upload, to_delete


def _upload(client, bucket, key, path):
    digest = file_md5(path)
    with open(path, "rb") as body:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            # S3 rejects the upload if the bytes it received don't match.
            ContentMD5=base64.b64encode(digest.digest()).decode(),
            ContentType=CONTENT_TYPES.get(path.suffix, "application/octet-stream"),
            ServerSideEncryption="AES256",
        )


def _delete(client, bucket, keys):
    # DeleteObjects accepts at most 1000 keys per request.
    for start in range(0, len(keys), 1000):
        batch = keys[start:start + 1000]
        response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        if response.get("Errors"):
            raise RuntimeError(f"S3 delete failed: {response['Errors'][:3]}")


def sync_lake_to_s3(client=None, bucket=None, prefix=None, root=None,
                    layers=LAYERS, dry_run=False):
    """
    Make s3://bucket/prefix/<layer>/ match the local lake for each layer.
    Returns per-layer and total counts.
    """
    env_bucket, env_prefix = s3_target()
    bucket = bucket or env_bucket
    prefix = (prefix if prefix is not None else env_prefix).strip("/")
    root = Path(root) if root else lake_root()
    if not bucket:
        raise ValueError("No bucket: set LAKE_S3_BUCKET")
    if not prefix:
        # The lake lives under a prefix so the rest of the bucket (Athena
        # results) is never inside the area the sync is allowed to delete.
        raise ValueError("LAKE_S3_PREFIX must not be empty")
    if client is None:
        import boto3

        client = boto3.client("s3")

    # Plan every layer before changing anything, so the guard can stop the
    # whole sync rather than leave it half done.
    plans = {}
    for layer in layers:
        local = local_files(root, layer)
        remote = remote_objects(client, bucket, prefix, layer)
        if not local and remote:
            raise UnsafeSyncError(
                f"Local layer '{layer}' is empty but s3://{bucket}/{prefix}/{layer}/ "
                f"has {len(remote)} objects; refusing to delete them."
            )
        to_upload, to_delete = plan_layer(local, remote)
        plans[layer] = (local, to_upload, to_delete, len(local) - len(to_upload))

    result = {"bucket": bucket, "prefix": prefix, "dry_run": dry_run, "layers": {}}
    for layer, (local, to_upload, to_delete, unchanged) in plans.items():
        if not dry_run:
            for rel in to_upload:
                _upload(client, bucket, f"{prefix}/{rel}", local[rel])
            _delete(client, bucket, [f"{prefix}/{rel}" for rel in to_delete])
        result["layers"][layer] = {
            "uploaded": len(to_upload),
            "deleted": len(to_delete),
            "unchanged": unchanged,
            "bytes_uploaded": sum(local[rel].stat().st_size for rel in to_upload),
        }
        log.info("s3 sync %s: %s", layer, result["layers"][layer])

    for key in ("uploaded", "deleted", "unchanged", "bytes_uploaded"):
        result[key] = sum(stats[key] for stats in result["layers"].values())
    return result
