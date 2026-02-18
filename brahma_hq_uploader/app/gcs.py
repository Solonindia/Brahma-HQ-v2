import json
from google.cloud import storage


def generate_signed_put_url(bucket_name: str, object_path: str, content_type: str):
    client = storage.Client()

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    url = blob.generate_signed_url(
        version="v4",
        expiration=3600,
        method="PUT",
        content_type=content_type,
    )

    return url


def write_metadata_json(bucket_name: str, object_path: str, metadata: dict):
    client = storage.Client()

    meta_path = object_path.replace(".pdf", "_metadata.json")

    blob = client.bucket(bucket_name).blob(meta_path)
    blob.upload_from_string(
        json.dumps(metadata, indent=2),
        content_type="application/json"
    )
