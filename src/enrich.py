from typing import Dict, List
import pandas as pd
from providers.apollo import ApolloProvider
from providers.cognism import CognismProvider

class EnrichmentProvider:
    def enrich(self, rec: Dict) -> Dict: raise NotImplementedError

class ReferenceTableProvider(EnrichmentProvider):
    def __init__(self, path: str): self.df = pd.read_csv(path).fillna("")
    def enrich(self, rec: Dict) -> Dict:
        cand = self.df[(self.df["facility_name"].str.lower()==(rec.get("facility_name","")).lower()) &
                       (self.df["contact_name"].str.lower()==(rec.get("contact_name","")).lower())]
        if cand.empty: return rec
        row=cand.iloc[0].to_dict(); out=dict(rec)
        for f in ("work_email","linkedin_url"):
            if not out.get(f) and row.get(f):
                out[f]=row[f]
                out["provenance_source"]=row.get("source","reference_table")
                out["provenance_url"]=row.get("linkedin_url") or None
        return out

def build_providers(cfg: dict) -> List[EnrichmentProvider]:
    providers: List[EnrichmentProvider] = []
    for p in cfg.get("enrichment",{}).get("providers",[]):
        if p.get("type")=="reference_table" and p.get("path"):
            providers.append(ReferenceTableProvider(p["path"]))
    return providers

def extend_with_configured_providers(cfg: dict, providers: list) -> list:
    for p in cfg.get("enrichment",{}).get("providers",[]):
        t=p.get("type"); enabled=p.get("enabled", True)
        if not enabled: continue
        if t=="apollo": providers.append(ApolloProvider(base_url=p.get("base_url",""), batch_size=int(p.get("batch_size",50))))
        if t=="cognism": providers.append(CognismProvider(base_url=p.get("base_url",""), batch_size=int(p.get("batch_size",50))))
    return providers

def apply_enrichment(rec: Dict, providers: List[EnrichmentProvider]) -> Dict:
    out=dict(rec)
    for p in providers: out = p.enrich(out)
    return out
