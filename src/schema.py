from pydantic import BaseModel
from typing import Optional

class Contact(BaseModel):
    facility_name: str
    facility_type: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    contact_name: str
    role: Optional[str] = None
    work_email: Optional[str] = None
    work_phone: Optional[str] = None
    specialty: Optional[str] = None
    linkedin_url: Optional[str] = None
    last_verified_ts: Optional[str] = None
    provenance_source: Optional[str] = None
    provenance_url: Optional[str] = None
    provenance_ts: Optional[str] = None
    provenance_steward: Optional[str] = None
