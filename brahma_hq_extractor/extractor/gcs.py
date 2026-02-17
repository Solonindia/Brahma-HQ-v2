from google.cloud import storage

client = storage.Client()

def read_blob(bucket, path):
    return client.bucket(bucket).blob(path).download_as_bytes()

def write_blob(bucket, path, data):
    client.bucket(bucket).blob(path).upload_from_string(
        data,
        content_type="application/json"
    )
