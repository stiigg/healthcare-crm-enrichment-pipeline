from prefect import flow, task
import subprocess, sys, pathlib

BASE = pathlib.Path(__file__).resolve().parents[1]

@task
def audit_task(input_csv: str):
    cmd = [sys.executable, str(BASE / "src" / "pipeline.py"), "audit", "--input", input_csv]
    return subprocess.run(cmd, capture_output=True, text=True).stdout

@task
def run_task(input_csv: str, output_csv: str):
    cmd = [sys.executable, str(BASE / "src" / "pipeline.py"), "run", "--input", input_csv, "--output", output_csv]
    return subprocess.run(cmd, capture_output=True, text=True).stdout

@flow(name="healthcare-crm-enrichment")
def main_flow(input_csv: str = "data/sample_contacts.csv", output_csv: str = "data/enriched_contacts.csv"):
    a = audit_task(input_csv)
    r = run_task(input_csv, output_csv)
    return {"audit": a, "run": r}

if __name__ == "__main__":
    print(main_flow())
