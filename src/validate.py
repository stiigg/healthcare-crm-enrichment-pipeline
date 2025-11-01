from typing import Dict
from email_validator import validate_email, EmailNotValidError

def is_valid_email(s: str) -> bool:
    if not s: return False
    try:
        validate_email(s, check_deliverability=False); return True
    except EmailNotValidError: return False

def compute_kpis(rows: list, required_fields: list, freshness_days: int) -> Dict:
    import datetime as dt
    total = len(rows) if rows else 1
    completeness = sum(all((r.get(f) not in (None, "")) for f in required_fields) for r in rows) / total
    emails = [r.get("work_email","") for r in rows if r.get("work_email")]
    email_valid = sum(1 for e in emails if is_valid_email(e)) / (len(emails) if emails else 1)
    fresh = 0
    for r in rows:
        ts = r.get("last_verified_ts") or r.get("provenance_ts") or ""
        try:
            t = dt.datetime.fromisoformat(ts[:10])
            if (dt.datetime.utcnow() - t).days <= freshness_days: fresh += 1
        except Exception: pass
    freshness = fresh / total
    return {"completeness": round(completeness,3), "email_valid": round(email_valid,3), "freshness": round(freshness,3)}

def run_ge_checks(df, ge_expectations_path: str = None) -> dict:
    import json, re, os
    if not ge_expectations_path or not os.path.exists(ge_expectations_path):
        return {"ge": "skipped"}
    spec = json.load(open(ge_expectations_path))
    results = []
    for exp in spec.get("expectations", []):
        t = exp.get("type"); kw = exp.get("kwargs", {})
        if t == "expect_column_values_to_not_be_null":
            col = kw["column"]
            ok = df[col].astype(str).str.len().gt(0).all()
            results.append({t: bool(ok), "column": col})
        elif t == "expect_column_values_to_match_regex":
            col = kw["column"]; import re
            pattern = re.compile(kw["regex"])
            ok = df[col].astype(str).apply(lambda x: bool(pattern.match(x))).all()
            results.append({t: bool(ok), "column": col})
    return {"ge_results": results}
