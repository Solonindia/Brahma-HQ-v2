from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from google.cloud import storage
from datetime import timedelta

from .models import PublishRequest, PublishResponse, ActiveReleaseResponse, SignedDownloadsResponse
from .publisher import publish_release
from .config import GCS_BUCKET, ACTIVE_OBJECT, RELEASE_ROOT, SIGN_URL_MINUTES, PRODUCT_DB_NAME
from .gcs_utils import read_text, list_objects, exists

app = FastAPI(title="Brahma HQ Publisher", version="1.0.0")
client = storage.Client()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/publish", response_model=PublishResponse)
def publish(req: PublishRequest):
    try:
        result = publish_release(req.release_notes, dry_run=req.dry_run)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/active", response_model=ActiveReleaseResponse)
def active():
    try:
        if not exists(GCS_BUCKET, ACTIVE_OBJECT):
            raise HTTPException(status_code=404, detail="ACTIVE pointer not found")
        rid = read_text(GCS_BUCKET, ACTIVE_OBJECT).strip()
        return {
            "active_release_id": rid,
            "release_prefix": f"{RELEASE_ROOT}/{rid}"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/active/raw", response_class=PlainTextResponse)
def active_raw():
    rid = read_text(GCS_BUCKET, ACTIVE_OBJECT).strip()
    return rid + "\n"

@app.get("/active/signed", response_model=SignedDownloadsResponse)
def active_signed():
    """
    Returns signed URLs for:
      - compiled sqlite
      - all standards yaml
      - manifest.json
    """
    rid = read_text(GCS_BUCKET, ACTIVE_OBJECT).strip()
    prefix = f"{RELEASE_ROOT}/{rid}"

    # find files in release
    sqlite_obj = f"{prefix}/compiled/{PRODUCT_DB_NAME}"
    manifest_obj = f"{prefix}/manifest.json"

    standards_prefix = f"{prefix}/02_Databases/Standards/"
    standards = [o for o in list_objects(GCS_BUCKET, standards_prefix) if o.lower().endswith((".yaml", ".yml"))]

    bucket = client.bucket(GCS_BUCKET)

    def sign(obj: str, content_type: str = "application/octet-stream") -> str:
        blob = bucket.blob(obj)
        if not blob.exists():
            raise HTTPException(status_code=404, detail=f"Missing object: {obj}")
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=SIGN_URL_MINUTES),
            method="GET",
            response_type=content_type
        )

    sqlite_url = sign(sqlite_obj, "application/x-sqlite3")
    manifest_url = sign(manifest_obj, "application/json")
    standards_urls = [sign(o, "text/yaml") for o in standards]

    return {
        "active_release_id": rid,
        "sqlite_url": sqlite_url,
        "standards_urls": standards_urls,
        "manifest_url": manifest_url,
    }
