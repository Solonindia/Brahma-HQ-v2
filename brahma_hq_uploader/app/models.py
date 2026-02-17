from pydantic import BaseModel

class UploadRequest(BaseModel):
    mfr: str
    model: str
    filename: str
    almm_compliant: bool | None = False
    technology: str | None = None
    notes: str | None = None

class UploadComplete(BaseModel):
    object_path: str
    mfr: str
    model: str
    almm_compliant: bool | None = False
    technology: str | None = None
    notes: str | None = None

