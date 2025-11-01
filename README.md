<<<<<<< HEAD
# Healthcare CRM Enrichment Pipeline (Advanced)

A production-lean Python pipeline for healthcare CRM **audit → normalize → dedupe → enrich → validate → HITL → writeback → KPIs**, with:
- Pluggable enrichment providers (Apollo, Cognism stubs)
- Great Expectations (minimal offline JSON spec)
- Prefect orchestration
- HITL queue CSV for manual review
- .env for API keys

## Quick start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Audit KPIs
python src/pipeline.py audit --input data/sample_contacts.csv

# Run full pipeline
python src/pipeline.py run --input data/sample_contacts.csv --output data/enriched_contacts.csv

# Orchestrate with Prefect
python orchestration/flow.py
```
=======
# healthcare-crm-enrichment-pipeline
A production-ready Python ETL pipeline for healthcare CRM data enrichment, featuring normalization, deduplication, enrichment (Apollo, Cognism), validation (Great Expectations), human-in-the-loop QA, and Prefect orchestration. Designed for compliant, auditable, and scalable data workflows.
>>>>>>> 55cd50ca2647d5ba4d880702fbefeac1ae35d0b5
