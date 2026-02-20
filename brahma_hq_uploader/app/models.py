# app/models.py

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional


class UploadRequest(BaseModel):
    # Allow using either "filename" (preferred) or "file_name" (old UI)
    model_config = ConfigDict(populate_by_name=True)

    mfr: str
    filename: str = Field(..., alias="file_name")
    series: Optional[str] = None
    model: Optional[str] = None
    notes: Optional[str] = None


class UploadComplete(BaseModel):
    # Allow using either "filename" (preferred) or "file_name" (old UI)
    model_config = ConfigDict(populate_by_name=True)

    object_path: str
    mfr: str
    filename: str = Field(..., alias="file_name")
    series: Optional[str] = None
    model: Optional[str] = None
    notes: Optional[str] = None
