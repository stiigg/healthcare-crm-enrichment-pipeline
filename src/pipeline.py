import argparse, yaml, sys, os
import pandas as pd
from typing import Dict
from pathlib import Path
from dotenv import load_dotenv
from normalize import normalize_record
from dedupe import cluster_records, pick_survivor
from enrich import build_providers, apply_enrichment, extend_with_configured_providers
from validate import compute_kpis, run_ge_checks
from provenance import stamp
from crm import CsvCRM
from dashboard import kpi_table
from hitl_queue import to_queue

def audit(df: pd.DataFrame, cfg: dict) -> Dict:
    rows = df.to_dict(orient="records")
    required = cfg.get("required_fields", [])
    kpis = compute_kpis(rows, required, cfg.get("freshness_days", 180))
    dupe_clusters = cluster_records(rows, cfg.get("dedupe",{}).get("fuzzy_threshold", 90))
    kpis["duplicate_rate"] = round((sum(len(c)>1 for c in dupe_clusters) / max(len(rows),1)), 3)
    return kpis

def run_pipeline(input_path: str, output_path: str, cfg_path: str = "config.yaml") -> Dict:
    cfg = yaml.safe_load(open(cfg_path))
    crm = CsvCRM(input_path, output_path)

    # Fetch
    df = crm.fetch()
    rows = df.to_dict(orient="records")

    # Normalize
    rows_norm = [normalize_record(r) for r in rows]

    # Dedupe
    clusters = cluster_records(rows_norm, cfg.get("dedupe",{}).get("fuzzy_threshold", 90))
    keep = set()
    for c in clusters: keep.add(pick_survivor(c, rows_norm))
    rows_dedup = [r for i, r in enumerate(rows_norm) if i in keep]

    # Enrich
    providers = build_providers(cfg); providers = extend_with_configured_providers(cfg, providers)
    enriched = []
    for r in rows_dedup:
        out = apply_enrichment(r, providers)
        out = stamp(out, source=out.get("provenance_source") or "pipeline", url=out.get("provenance_url"), steward="pipeline_bot")
        enriched.append(out)

    # KPIs + Writeback
    required = cfg.get("required_fields", [])
    kpis = compute_kpis(enriched, required, cfg.get("freshness_days", 180))
    out_df = pd.DataFrame(enriched); crm.writeback(out_df)

    # HITL queue
    hitl_rows = to_queue(list(zip(rows_dedup, enriched)))
    if hitl_rows: pd.DataFrame(hitl_rows).to_csv(Path(output_path).with_suffix(".hitl.csv"), index=False)

    # GE checks (offline minimal spec)
    ge_path = str(Path(__file__).resolve().parents[1] / 'ge' / 'expectations.json')
    ge_res = run_ge_checks(out_df, ge_path)

    # Save KPI sidecar
    kpi_path = Path(output_path).with_suffix(".kpis.csv"); kpi_table(kpis).to_csv(kpi_path, index=False)
    return {"output": output_path, "kpis": kpis, "kpi_csv": str(kpi_path), "ge": ge_res}

def main():
    load_dotenv()
    ap = argparse.ArgumentParser(description="Healthcare CRM enrichment pipeline")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_audit = sub.add_parser("audit")
    ap_audit.add_argument("--input", required=True)
    ap_audit.add_argument("--config", default="config.yaml")

    ap_run = sub.add_parser("run")
    ap_run.add_argument("--input", required=True)
    ap_run.add_argument("--output", required=True)
    ap_run.add_argument("--config", default="config.yaml")

    args = ap.parse_args()

    if args.cmd == "audit":
        cfg = yaml.safe_load(open(args.config))
        df = pd.read_csv(args.input).fillna("")
        kpis = audit(df, cfg)
        print("KPIs:", kpis)
    elif args.cmd == "run":
        res = run_pipeline(args.input, args.output, args.config)
        import json; print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()
