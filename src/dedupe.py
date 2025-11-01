from typing import List, Dict, Tuple
from rapidfuzz import fuzz

def similarity(a: str, b: str) -> int:
    return fuzz.token_sort_ratio((a or "").lower(), (b or "").lower())

def cluster_records(rows: List[Dict], fuzzy_threshold: int = 90) -> List[List[int]]:
    clusters=[]; seen=set()
    for i, r in enumerate(rows):
        if i in seen: continue
        cluster=[i]; seen.add(i)
        for j in range(i+1, len(rows)):
            if j in seen: continue
            rj=rows[j]
            email_match = r.get("work_email") and r.get("work_email").lower()==(rj.get("work_email") or "").lower()
            name_org_sim = (similarity(r.get("contact_name",""), rj.get("contact_name","")) + similarity(r.get("facility_name",""), rj.get("facility_name",""))) // 2
            if email_match or name_org_sim >= fuzzy_threshold:
                cluster.append(j); seen.add(j)
        clusters.append(cluster)
    return clusters

def pick_survivor(cluster: List[int], rows: List[Dict]) -> int:
    def score(idx:int):
        r=rows[idx]; non_empty=sum(1 for v in r.values() if v not in ("", None))
        ts=r.get("last_verified_ts") or "0000-00-00"
        return (non_empty, int(ts.replace("-","")[:8]) if ts else 0)
    return max(cluster, key=score)
