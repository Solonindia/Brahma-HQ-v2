from google.cloud import storage
from datetime import timedelta

def generate_signed_put_url(bucket_name, object_name, content_type):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="PUT",
        content_type=content_type,
        service_account_email=client._credentials.service_account_email,
        access_token=client._credentials.token,
    )

    return url
