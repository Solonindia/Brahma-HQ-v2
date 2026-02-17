from __future__ import annotations

import datetime as dt
import json
import os
import secrets
import uuid
from io import BytesIO
from typing import Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import storage

from app.config import GCS_BUCKET, MASTER_ROOT, RELEASE_ROOT
from app.gcs import list_blobs, read_json, write_json
from app.models import ReviewRequest
from app.utils import safe_key, utc_now_iso

app = FastAPI(title="Brahma HQ Reviewer", version="1.0")

# ---- GCS client ----
gcs_client = storage.Client()

# ---- Templates ----
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /app
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# ---- Basic Auth ----
security = HTTPBasic()
BASIC_USER = os.getenv("BASIC_USER", "admin")
BASIC_PASS = os.getenv("BASIC_PASS", "admin123")


def require_basic(credentials: HTTPBasicCredentials = Depends(security)) -> bool:
    """
    IMPORTANT: To force browser login prompt, we MUST return 401 with
    'WWW-Authenticate: Basic' header.
    """
    ok_user = secrets.compare_digest(credentials.username, BASIC_USER)
    ok_pass = secrets.compare_digest(credentials.password, BASIC_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


def derive_pdf_object_path(cand: dict) -> str:
    pdf_gs = cand.get("source_pdf", "")
    if isinstance(pdf_gs, str) and pdf_gs.startswith("gs://"):
        parts = pdf_gs.split("/", 3)
        if len(parts) >= 4:
            return parts[3]

    obj_path = cand.get("object_path", "")
    if isinstance(obj_path, str) and obj_path.endswith(".pdf"):
        return obj_path

    return ""


# -------------------- Health --------------------

@app.get("/")
def health():
    return {"status": "reviewer alive"}


# -------------------- API endpoints (Basic Auth protected) --------------------

@app.get("/candidates")
def candidates(prefix: str = Query("02_Candidates/"), _=Depends(require_basic)):
    paths = [p for p in list_blobs(GCS_BUCKET, prefix) if p.endswith(".json")]
    return {"count": len(paths), "items": paths}


@app.get("/candidate")
def candidate(path: str, _=Depends(require_basic)):
    try:
        return read_json(GCS_BUCKET, path)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


def _process_review(req: ReviewRequest) -> dict:
    """
    Shared business logic:
    - write review log
    - if approved: write masterdata json
    - ALWAYS update candidate json so it disappears from pending list
    """
    cand = read_json(GCS_BUCKET, req.candidate_path)

    review_id = uuid.uuid4().hex[:16]
    reviewed_at = utc_now_iso()

    # apply patch edits
    final_obj = dict(cand)
    if req.patch:
        final_obj.update(req.patch)

    # write review record
    review_record = {
        "review_id": review_id,
        "candidate_path": req.candidate_path,
        "decision": req.decision,
        "reviewer": req.reviewer,
        "comments": req.comments,
        "patch": req.patch,
        "reviewed_at_utc": reviewed_at,
    }
    review_path = f"02_Candidates/_reviews/review_{review_id}.json"
    write_json(GCS_BUCKET, review_path, review_record)

    # ----- REJECT / OTHER -----
    if req.decision != "approved":
        cand_update = dict(final_obj)
        cand_update.update({
            "status": req.decision,      # "rejected" etc.
            "needs_review": False,
            "reviewed_at_utc": reviewed_at,
            "reviewed_by": req.reviewer,
            "review_path": review_path,
        })
        write_json(GCS_BUCKET, req.candidate_path, cand_update)

        return {"status": "recorded", "review_path": review_path}

    # ----- APPROVE -----
    mfr = final_obj.get("mfr") or final_obj.get("manufacturer") or "unknown"
    model = final_obj.get("model") or "unknown_model"

    out_path = f"03_MasterData/modules/{safe_key(mfr)}/{safe_key(model)}.json"

    final_obj.update(
        {
            "status": "approved",
            "approved_at_utc": reviewed_at,
            "approved_by": req.reviewer,
            "source_candidate_path": req.candidate_path,
            "review_id": review_id,
            "review_path": review_path,
        }
    )
    write_json(GCS_BUCKET, out_path, final_obj)

    # Update candidate so it won't show again
    cand_update = dict(final_obj)
    cand_update.update({
        "needs_review": False,
        "masterdata_path": out_path,
    })
    write_json(GCS_BUCKET, req.candidate_path, cand_update)

    return {"status": "approved", "review_path": review_path, "masterdata_path": out_path}


@app.post("/review")
def review(req: ReviewRequest, _=Depends(require_basic)):
    return _process_review(req)


# -------------------- UI routes (Basic Auth protected) --------------------

@app.get("/ui", response_class=HTMLResponse)
def ui_index(
    request: Request,
    prefix: str = "02_Candidates/modules/",
    show_all: bool = Query(False),
    _=Depends(require_basic),
):
    """
    Default: show ONLY pending (needs_review=true).
    If show_all=true, shows everything.
    """
    try:
        all_paths = [p for p in list_blobs(GCS_BUCKET, prefix) if p.endswith(".json")]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GCS list failed for prefix={prefix}: {e}")

    items = []
    for p in all_paths:
        if show_all:
            items.append(p)
            continue

        # pending-only filter
        try:
            c = read_json(GCS_BUCKET, p)
            if c.get("needs_review", True) is True and c.get("status") not in ("approved", "rejected"):
                items.append(p)
        except Exception:
            # if candidate json broken, keep it visible so it can be fixed
            items.append(p)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "prefix": prefix,
            "count": len(items),
            "items": items,
            "show_all": show_all,
        },
    )


@app.get("/ui/candidate", response_class=HTMLResponse)
def ui_candidate(
    request: Request,
    path: str,
    back_prefix: str = "02_Candidates/modules/",
    _=Depends(require_basic),
):
    cand = read_json(GCS_BUCKET, path)
    cand_pretty = json.dumps(cand, indent=2)

    pdf_object_path = derive_pdf_object_path(cand)

    return templates.TemplateResponse(
        "candidate.html",
        {
            "request": request,
            "path": path,
            "back_prefix": back_prefix,
            "cand_pretty": cand_pretty,
            "cand": cand,
            "pdf_object_path": pdf_object_path,
            "result": None,
            "result_pretty": None,
        },
    )


# ✅ IMPORTANT: Some old index.html links might use /ui/review?path=...
# Add GET alias so clicking doesn't give Method Not Allowed.
@app.get("/ui/review", response_class=HTMLResponse)
def ui_review_get(
    request: Request,
    path: str,
    _=Depends(require_basic),
):
    return ui_candidate(request=request, path=path, back_prefix="02_Candidates/modules/", _=True)


@app.post("/ui/review", response_class=HTMLResponse)
def ui_review_post(
    request: Request,

    # hidden field
    candidate_path: str = Form(...),

    # review fields
    decision: str = Form(...),
    reviewer: str = Form(...),
    comments: str = Form(""),

    # editable datasheet fields
    mfr: str = Form(""),
    model: str = Form(""),
    technology: str = Form(""),
    almm_compliant: str = Form("true"),

    pmax_w: str = Form(""),
    vmp_v: str = Form(""),
    imp_a: str = Form(""),
    voc_v: str = Form(""),
    isc_a: str = Form(""),
    efficiency_pct: str = Form(""),

    _=Depends(require_basic),
):
    def to_float(v: str) -> Optional[float]:
        v = (v or "").strip()
        if not v:
            return None
        try:
            return float(v)
        except Exception:
            return None

    patch = {
        "mfr": (mfr or "").strip() or None,
        "model": (model or "").strip() or None,
        "technology": (technology or "").strip() or None,
        "almm_compliant": True if (almm_compliant or "").lower() == "true" else False,
        "pmax_w": to_float(pmax_w),
        "vmp_v": to_float(vmp_v),
        "imp_a": to_float(imp_a),
        "voc_v": to_float(voc_v),
        "isc_a": to_float(isc_a),
        "efficiency_pct": to_float(efficiency_pct),
    }

    # remove None so we don't overwrite existing data with nulls
    patch = {k: v for k, v in patch.items() if v is not None and v != ""}

    req = ReviewRequest(
        candidate_path=candidate_path,
        decision=decision,
        reviewer=reviewer,
        comments=comments,
        patch=patch,
    )

    result_obj = _process_review(req)

    # reload candidate for display (will now contain status/needs_review updates)
    cand = read_json(GCS_BUCKET, candidate_path)
    cand_pretty = json.dumps(cand, indent=2)
    pdf_object_path = derive_pdf_object_path(cand)

    return templates.TemplateResponse(
        "candidate.html",
        {
            "request": request,
            "path": candidate_path,
            "back_prefix": "02_Candidates/modules/",
            "cand_pretty": cand_pretty,
            "cand": cand,
            "pdf_object_path": pdf_object_path,
            "result": result_obj,
            "result_pretty": json.dumps(result_obj, indent=2),
        },
    )


@app.get("/ui/pdf")
def ui_pdf(object_path: str, _=Depends(require_basic)):
    try:
        blob = gcs_client.bucket(GCS_BUCKET).blob(object_path)

        if not blob.exists():
            raise HTTPException(status_code=404, detail=f"PDF not found: {object_path}")

        data = blob.download_as_bytes()
        filename = object_path.split("/")[-1] or "datasheet.pdf"

        return StreamingResponse(
            BytesIO(data),
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{filename}"'},
        )

    except Forbidden:
        raise HTTPException(
            status_code=403,
            detail="No permission to read PDF from GCS. Grant roles/storage.objectViewer to reviewer service account.",
        )
    except NotFound:
        raise HTTPException(status_code=404, detail=f"PDF not found: {object_path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PDF fetch failed: {e}")


@app.post("/ui/release")
def ui_release(
    masterdata_path: str = Form(...),
    _=Depends(require_basic),
):
    """
    Release approved master data into 04_Releases/modules/{version}/... and /latest/...
    """
    bucket = gcs_client.bucket(GCS_BUCKET)

    # Safety: Only allow releasing from master root
    if not masterdata_path.startswith(MASTER_ROOT + "/") or not masterdata_path.endswith(".json"):
        raise HTTPException(status_code=400, detail="Invalid masterdata_path")

    master_blob = bucket.blob(masterdata_path)
    if not master_blob.exists():
        raise HTTPException(status_code=404, detail=f"Master JSON not found: {masterdata_path}")

    data = master_blob.download_as_bytes()

    # e.g. v20260212-1234 (UTC)
    ver = dt.datetime.utcnow().strftime("v%Y%m%d-%H%M")
    rel = masterdata_path.replace(MASTER_ROOT + "/", "")

    version_path = f"{RELEASE_ROOT}/{ver}/{rel}"
    latest_path = f"{RELEASE_ROOT}/latest/{rel}"

    bucket.blob(version_path).upload_from_string(data, content_type="application/json")
    bucket.blob(latest_path).upload_from_string(data, content_type="application/json")

    return {
        "status": "released",
        "version": ver,
        "version_path": version_path,
        "latest_path": latest_path,
    }
