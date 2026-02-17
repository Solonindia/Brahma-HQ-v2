import os, re
from datetime import datetime, timezone

ALLOWED_EXTENSIONS = [".pdf"]

def make_mfr_slug(mfr: str) -> str:
    slug = mfr.lower().strip().replace(" ", "-")
    return re.sub(r"[^a-z0-9-]", "", slug)

def utc_timestamp():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")

def safe_filename(filename: str) -> str:
    return os.path.basename(filename).replace(" ", "_").lower()

def validate_filetype(filename: str):
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError("Only PDF files are allowed")

def build_object_path(mfr: str, filename: str) -> str:
    return f"01_Raw_Catalogues/modules/{make_mfr_slug(mfr)}/{utc_timestamp()}_{safe_filename(filename)}"
