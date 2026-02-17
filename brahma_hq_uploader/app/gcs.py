import json
from google.cloud import storage

client = storage.Client()

def write_metadata_json(bucket_name: str, object_path: str, metadata: dict):
    meta_path = object_path.replace(".pdf", "_metadata.json")

    blob = client.bucket(bucket_name).blob(meta_path)
    blob.upload_from_string(
        json.dumps(metadata, indent=2),
        content_type="application/json"
    )
