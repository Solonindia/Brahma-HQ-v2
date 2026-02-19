from pydantic import BaseModel
from typing import Optional


class UploadRequest(BaseModel):
    mfr: str
    filename: str
    series: Optional[str] = None
    model: Optional[str] = None
    notes: Optional[str] = None


class UploadComplete(BaseModel):
    object_path: str
    mfr: str
    filename: str
    series: Optional[str] = None
    model: Optional[str] = None
    notes: Optional[str] = None
