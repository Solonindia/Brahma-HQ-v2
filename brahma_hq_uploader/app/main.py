from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pathlib import Path
import mimetypes
import uvicorn

from .config import GCS_BUCKET
from .models import UploadRequest, UploadComplete
from .utils import build_object_path
from .gcs import write_metadata_json, generate_signed_put_url


app = FastAPI(
    title="Brahma HQ Uploader",
    version="1.0",
    swagger_ui_parameters={"persistAuthorization": True}
)

# ---------------------------------------------------
# UI SERVING
# ---------------------------------------------------

UI_PATH = Path(__file__).resolve().parent.parent / "ui" / "index.html"


def serve_ui():
    if not UI_PATH.exists():
        return HTMLResponse(
            "<h3>UI not found. Ensure ui/index.html is packaged into container.</h3>",
            status_code=500
        )
    return HTMLResponse(UI_PATH.read_text(encoding="utf-8"))


@app.get("/", include_in_schema=False)
def root_ui():
    return serve_ui()


@app.get("/ui", include_in_schema=False)
def ui_page():
    return serve_ui()


@app.get("/health")
def health():
    return {"status": "service alive"}


# ---------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------

@app.post("/upload_request")
def upload_request(request: UploadRequest):
    try:
        # Build storage object path
        object_path = build_object_path(request)

        # Detect content type dynamically
        content_type, _ = mimetypes.guess_type(request.filename)
        if content_type is None:
            content_type = "application/octet-stream"

        # Generate signed URL
        signed_url = generate_signed_put_url(
            bucket_name=GCS_BUCKET,
            object_name=object_path,
            content_type=content_type
        )

        return {
            "signed_url": signed_url,
            "object_path": object_path
        }

    except Exception as e:
        return {"error": str(e)}


@app.post("/upload_complete")
def upload_complete(req: UploadComplete):
    try:
        # Write metadata JSON to bucket
        write_metadata_json(
            bucket_name=GCS_BUCKET,
            object_path=req.object_path,
            metadata=req.dict()
        )

        return {
            "status": "registered",
            "metadata_path": req.object_path.replace(".pdf", "_metadata.json")
        }

    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------
# LOCAL RUN (For Development Only)
# ---------------------------------------------------

