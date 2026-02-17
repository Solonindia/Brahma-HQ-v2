from google.cloud import storage
from typing import Iterable, List, Optional
import json

client = storage.Client()

def list_objects(bucket: str, prefix: str) -> List[str]:
    b = client.bucket(bucket)
    return [blob.name for blob in client.list_blobs(b, prefix=prefix)]

def read_text(bucket: str, object_name: str) -> str:
    b = client.bucket(bucket)
    blob = b.blob(object_name)
    return blob.download_as_text()

def read_bytes(bucket: str, object_name: str) -> bytes:
    b = client.bucket(bucket)
    blob = b.blob(object_name)
    return blob.download_as_bytes()

def write_text(bucket: str, object_name: str, text: str, content_type: str = "text/plain") -> None:
    b = client.bucket(bucket)
    blob = b.blob(object_name)
    blob.upload_from_string(text, content_type=content_type)

def write_bytes(bucket: str, object_name: str, data: bytes, content_type: str) -> None:
    b = client.bucket(bucket)
    blob = b.blob(object_name)
    blob.upload_from_string(data, content_type=content_type)

def write_json(bucket: str, object_name: str, payload: dict) -> None:
    write_text(bucket, object_name, json.dumps(payload, indent=2), content_type="application/json")

def copy_object(bucket: str, src_name: str, dst_name: str) -> None:
    b = client.bucket(bucket)
    src_blob = b.blob(src_name)
    client.copy_blob(src_blob, b, dst_name)

def exists(bucket: str, object_name: str) -> bool:
    b = client.bucket(bucket)
    return b.blob(object_name).exists()
