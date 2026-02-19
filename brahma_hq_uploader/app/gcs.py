# app/gcs.py

from datetime import timedelta
import json
from google.cloud import storage


def generate_signed_put_url(bucket_name: str, object_name: str, content_type: str) -> str:
    """
    Generate a V4 signed PUT URL using Cloud Run's default service account credentials (no JSON key file).
    """
    client = storage.Client()  # ✅ Uses Cloud Run ADC automatically
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="PUT",
        content_type=content_type,
    )


def write_metadata_json(bucket_name: str, object_path: str, metadata: dict) -> str:
    """
    Writes metadata JSON to the same bucket next to the uploaded PDF.
    Example: myfile.pdf -> myfile_metadata.json
    """
    client = storage.Client()  # ✅ Uses Cloud Run ADC automatically
    bucket = client.bucket(bucket_name)

    metadata_path = object_path.replace(".pdf", "_metadata.json")
    blob = bucket.blob(metadata_path)

    blob.upload_from_string(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

    return metadata_path
