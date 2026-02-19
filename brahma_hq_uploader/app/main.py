from fastapi import FastAPI, Depends
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBearer
from pathlib import Path

from .config import GCS_BUCKET
from .models import UploadRequest, UploadComplete
from .utils import build_object_path, validate_filetype
from .gcs import write_metadata_json, generate_signed_put_url

import os
import uvicorn

app = FastAPI(
    title="Brahma HQ Uploader",
    version="1.0",
    swagger_ui_parameters={"persistAuthorization": True}
)

security = HTTPBearer()

# ---------- UI ----------
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


# ---------- API (Protected) ----------
@app.post("/upload_request")
def upload_request(req: UploadRequest):
    validate_filetype(req.filename)

    object_path = build_object_path(req.mfr, req.filename)

    # ✅ Signed URL required for browser PUT upload
    signed_url = generate_signed_put_url(
        bucket_name=GCS_BUCKET,
        object_path=object_path,
        content_type="application/pdf"
    )

    return {
        "object_path": object_path,
        "gcs_uri": f"gs://{GCS_BUCKET}/{object_path}",
        "signed_url": signed_url
    }


@app.post("/upload_complete")
def upload_complete(req: UploadComplete):
    write_metadata_json(
        bucket_name=GCS_BUCKET,
        object_path=req.object_path,
        metadata=req.dict()
    )
    return {
        "status": "registered",
        "metadata_path": req.object_path.replace(".pdf", "_metadata.json")
    }
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
    )
