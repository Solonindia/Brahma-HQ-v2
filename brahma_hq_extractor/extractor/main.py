import os, json
from fastapi import FastAPI, Request
from google.cloud import storage
from extractor.pdf_parser import extract_parameters

app = FastAPI(title="Brahma HQ Extractor (Phase D)")
client = storage.Client()

BUCKET_ENV = os.getenv("GCS_BUCKET", "brahma-hq-prod")

def read_bytes(bucket: str, path: str) -> bytes:
    return client.bucket(bucket).blob(path).download_as_bytes()

def write_json(bucket: str, path: str, obj: dict):
    client.bucket(bucket).blob(path).upload_from_string(
        json.dumps(obj, indent=2),
        content_type="application/json"
    )

@app.get("/")
def health():
    return {"status": "extractor alive"}

@app.post("/")
async def eventarc_receiver(req: Request):
    body = await req.json()

    bucket = body.get("bucket") or BUCKET_ENV
    name = body.get("name")

    if not name:
        return {"status": "ignored", "reason": "no object name"}

    # Only PDFs in correct prefix
    if not (name.startswith("01_Raw_Catalogues/modules/") and name.endswith(".pdf")):
        return {"status": "ignored", "name": name}

    meta_name = name.replace(".pdf", "_metadata.json")

    # Read metadata and PDF
    meta = json.loads(read_bytes(bucket, meta_name).decode("utf-8"))
    pdf_bytes = read_bytes(bucket, name)

    extracted = extract_parameters(pdf_bytes)

    # IMPORTANT:
    #  - Keep keys even if value is empty-string (""), because Publisher will
    #    evolve SQLite schema dynamically based on keys.
    #  - Avoid writing NULLs when we already have some raw string for a field.
    #    (Publisher also normalizes None -> "".)
    candidate = {
        **meta,
        **extracted,
        "source_pdf": f"gs://{bucket}/{name}",
        "source_metadata": f"gs://{bucket}/{meta_name}",
    }

    # Write to 02_Candidates/modules/... (not _provenance)
    out_name = (
        name.replace("01_Raw_Catalogues", "02_Candidates")
            .replace(".pdf", ".json")
    )

    write_json(bucket, out_name, candidate)
    return {"status": "ok", "written": out_name}
