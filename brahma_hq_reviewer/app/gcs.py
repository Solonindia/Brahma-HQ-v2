import json
from google.cloud import storage

client = storage.Client()

def list_blobs(bucket: str, prefix: str):
    b = client.bucket(bucket)
    return [x.name for x in client.list_blobs(b, prefix=prefix)]

def read_json(bucket: str, path: str):
    blob = client.bucket(bucket).blob(path)
    return json.loads(blob.download_as_text())

def write_json(bucket: str, path: str, obj: dict):
    client.bucket(bucket).blob(path).upload_from_string(
        json.dumps(obj, indent=2),
        content_type="application/json"
    )
