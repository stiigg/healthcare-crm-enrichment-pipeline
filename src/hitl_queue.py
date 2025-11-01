from typing import Dict, List, Tuple
from rapidfuzz import fuzz

def needs_review(before: Dict, after: Dict) -> bool:
    email_changed = (before.get("work_email") or "") != (after.get("work_email") or "")
    name_sim = fuzz.token_sort_ratio((before.get("contact_name") or ""), (after.get("contact_name") or ""))
    return email_changed or name_sim < 85

def to_queue(entries: List[Tuple[Dict, Dict]]):
    rows = []
    for before, after in entries:
        if needs_review(before, after):
            rows.append({
                "facility_name": after.get("facility_name"),
                "contact_name": after.get("contact_name"),
                "work_email_before": before.get("work_email"),
                "work_email_after": after.get("work_email"),
                "provenance_source": after.get("provenance_source"),
                "provenance_url": after.get("provenance_url"),
                "review_required": True
            })
    return rows
