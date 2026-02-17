from pydantic import BaseModel
from typing import Literal, Optional, Dict, Any

class ReviewRequest(BaseModel):
    candidate_path: str
    decision: Literal["approved", "rejected", "needs_fix"]
    reviewer: str
    comments: Optional[str] = None
    patch: Optional[Dict[str, Any]] = None
