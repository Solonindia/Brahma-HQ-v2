from pydantic import BaseModel, Field
from typing import Optional

class UploadRequest(BaseModel):
    mfr: str
    filename: str = Field(..., alias="file_name")
    series: Optional[str] = None
    model: Optional[str] = None
    notes: Optional[str] = None

    class Config:  # Pydantic v1
        allow_population_by_field_name = True


class UploadComplete(BaseModel):
    object_path: str
    mfr: str
    filename: str = Field(..., alias="file_name")
    series: Optional[str] = None
    model: Optional[str] = None
    notes: Optional[str] = None

    class Config:  # Pydantic v1
        allow_population_by_field_name = True