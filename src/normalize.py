import re, phonenumbers
CANON_SPECIALTIES = {"cardiology":"Cardiology","cardiologist":"Cardiology",
                     "orthopedics":"Orthopedics","orthopaedics":"Orthopedics"}
ROLE_CANON = {"hr director":"HR Director","talent manager":"Talent Manager","head of hr":"Head of HR"}

def _title(s): return re.sub(r"\s+"," ", (s or "").strip()).title()

def normalize_specialty(s): 
    k=(s or "").strip().lower(); return CANON_SPECIALTIES.get(k, s.strip() if s else s)
def normalize_role(s):
    k=(s or "").strip().lower(); return ROLE_CANON.get(k, s.strip() if s else s)
def normalize_email(s): return (s or "").strip().lower()

def normalize_phone(s, default_region="GB"):
    raw=re.sub(r"[\s()-]","",s or "")
    if not raw: return raw
    try:
        num=phonenumbers.parse(raw, default_region)
        if not phonenumbers.is_valid_number(num): return s.strip()
        return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
    except Exception: return s.strip()

def normalize_record(rec: dict) -> dict:
    rec=dict(rec)
    rec["facility_name"]=_title(rec.get("facility_name",""))
    rec["contact_name"]=_title(rec.get("contact_name",""))
    rec["role"]=normalize_role(rec.get("role",""))
    rec["specialty"]=normalize_specialty(rec.get("specialty",""))
    rec["work_email"]=normalize_email(rec.get("work_email",""))
    rec["work_phone"]=normalize_phone(rec.get("work_phone",""))
    return rec
