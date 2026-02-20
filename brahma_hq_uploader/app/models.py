from typing import Optional
from pydantic import BaseModel, Field

# Pydantic v2 supports AliasChoices; v1 won't have it
try:
    from pydantic import AliasChoices, ConfigDict
    PYDANTIC_V2 = True
except Exception:
    PYDANTIC_V2 = False


if PYDANTIC_V2:
    class UploadRequest(BaseModel):
        model_config = ConfigDict(populate_by_name=True)
        mfr: str
        filename: str = Field(..., validation_alias=AliasChoices("filename", "file_name"))
        series: Optional[str] = None
        model: Optional[str] = None
        notes: Optional[str] = None

    class UploadComplete(BaseModel):
        model_config = ConfigDict(populate_by_name=True)
        object_path: str
        mfr: str
        filename: str = Field(..., validation_alias=AliasChoices("filename", "file_name"))
        series: Optional[str] = None
        model: Optional[str] = None
        notes: Optional[str] = None

else:
    # Pydantic v1 fallback
    class UploadRequest(BaseModel):
        mfr: str
        filename: str = Field(..., alias="file_name")
        series: Optional[str] = None
        model: Optional[str] = None
        notes: Optional[str] = None

        class Config:
            allow_population_by_field_name = True

    class UploadComplete(BaseModel):
        object_path: str
        mfr: str
        filename: str = Field(..., alias="file_name")
        series: Optional[str] = None
        model: Optional[str] = None
        notes: Optional[str] = None

        class Config:
            allow_population_by_field_name = True