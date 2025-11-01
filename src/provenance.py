from typing import Dict, Optional
from datetime import datetime

def stamp(rec: Dict, source: Optional[str] = None, url: Optional[str] = None, steward: Optional[str] = None) -> Dict:
    out = dict(rec)
    if source: out["provenance_source"] = source
    if url: out["provenance_url"] = url
    out["provenance_ts"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    if steward: out["provenance_steward"] = steward
    return out
