from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class PublishRequest(BaseModel):
    release_notes: Optional[str] = "Automated publish from Brahma HQ Publisher"
    dry_run: bool = False

class PublishResponse(BaseModel):
    status: str
    release_id: str
    release_prefix: str
    active_object: str
    counts: Dict[str, int]
    outputs: Dict[str, str]

class ActiveReleaseResponse(BaseModel):
    active_release_id: str
    release_prefix: str

class SignedDownloadsResponse(BaseModel):
    active_release_id: str
    sqlite_url: str
    standards_urls: List[str]
    manifest_url: str
