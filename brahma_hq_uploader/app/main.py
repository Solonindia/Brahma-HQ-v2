from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pathlib import Path
import mimetypes

from .config import GCS_BUCKET
from .models import UploadRequest, UploadComplete
from .utils import build_object_path
from .gcs import write_metadata_json, generate_signed_put_url


app = FastAPI(
    title="Brahma HQ Uploader",
    version="1.0",
    swagger_ui_parameters={"persistAuthorization": True},
)

# ---------------------------------------------------
# UI SERVING
# ---------------------------------------------------

UI_PATH = Path(__file__).resolve().parent.parent / "ui" / "index.html"


def serve_ui() -> HTMLResponse:
    if not UI_PATH.exists():
        return HTMLResponse(
            "<h3>UI not found. Ensure ui/index.html is packaged into container.</h3>",
            status_code=500,
        )
    return HTMLResponse(UI_PATH.read_text(encoding="utf-8"))


@app.get("/", include_in_schema=False)
def root_ui():
    return serve_ui()


@app.get("/ui", include_in_schema=False)
def ui_page():
    return serve_ui()


# ---------------------------------------------------
# HEALTH
# ---------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/version")
def version():
    return {"service": "brahma-hq-uploader", "version": "1.0"}


# ---------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------

@app.post("/upload_request")
def upload_request(request: UploadRequest):
    """
    1) Build object path in bucket
    2) Generate signed PUT URL
    3) Return signed_url + object_path
    """
    try:
        object_path = build_object_path(request)

        content_type, _ = mimetypes.guess_type(request.filename)
        if not content_type:
            content_type = "application/octet-stream"

        signed_url = generate_signed_put_url(
            bucket_name=GCS_BUCKET,
            object_name=object_path,
            content_type=content_type,
        )

        return {"signed_url": signed_url, "object_path": object_path}

    except Exception as e:
        # Proper 500 response (not 200 with {"error":...})
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload_complete")
def upload_complete(req: UploadComplete):
    """
    Store metadata JSON next to the uploaded PDF in the same bucket.
    """
    try:
        metadata_path = write_metadata_json(
            bucket_name=GCS_BUCKET,
            object_path=req.object_path,
            metadata=req.model_dump(),  # Pydantic v2
        )

        return {"status": "registered", "metadata_path": metadata_path}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))