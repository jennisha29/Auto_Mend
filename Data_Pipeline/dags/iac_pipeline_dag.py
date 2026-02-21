"""
dags/iac_pipeline_dag.py
─────────────────────────
Manual-trigger DAG for the IaC Payload Layer pipeline.
Uses PythonOperator to call each script's main entry point directly.

Trigger:
  UI  → DAGs → iac_payload_pipeline → ▶ Trigger DAG
  CLI → airflow dags trigger iac_payload_pipeline
"""

import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure repo root is on the path for all tasks
sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ── Task callables ─────────────────────────────────────────────────────────────
# Each function imports and calls the script's own entry point.
# Importing inside the function keeps the DAG file lightweight at parse time.

def task_download(**ctx):
    import os
    os.chdir("/opt/airflow")
    # Pass HF token from Airflow Variable to huggingface_hub
    token = ctx["var"]["value"].get("HF_TOKEN", "")
    if token:
        os.environ["HUGGING_FACE_HUB_TOKEN"] = token
    from scripts.download.stack_iac_sample import download
    download()


def task_analyze(**ctx):
    import os
    os.chdir("/opt/airflow")
    from scripts.analyze.stack_iac_analysis import analyze
    analyze()


def task_preprocess(**ctx):
    import os
    os.chdir("/opt/airflow")
    from scripts.preprocess.payload_pipeline import run
    run()


def task_validate(**ctx):
    import os
    os.chdir("/opt/airflow")
    from scripts.validate.schema_stats import run_validation
    run_validation()


def task_anomaly(**ctx):
    import os
    os.chdir("/opt/airflow")
    from scripts.validate.anomaly_alerts import run_anomaly_check
    run_anomaly_check()


def task_bias(**ctx):
    import os
    os.chdir("/opt/airflow")
    from scripts.validate.bias_detection import run_bias_detection
    run_bias_detection()


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="iac_payload_pipeline",
    description="IaC Payload Layer: download → analyze → preprocess → validate → anomaly → bias",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["iac", "payload-layer", "ml-data"],
) as dag:

    download = PythonOperator(
        task_id="download",
        python_callable=task_download,
        provide_context=True,
        doc_md="Stream IaC YAML files from The Stack → data/raw/chunk_*.parquet",
    )

    analyze = PythonOperator(
        task_id="analyze",
        python_callable=task_analyze,
        provide_context=True,
        doc_md="Analyze raw chunks → logs/analysis_report.json",
    )

    preprocess = PythonOperator(
        task_id="preprocess",
        python_callable=task_preprocess,
        provide_context=True,
        doc_md="Filter + redact + wrap → data/processed/training_records.jsonl",
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
        provide_context=True,
        doc_md="Schema + PII validation → logs/schema_report.json",
    )

    anomaly = PythonOperator(
        task_id="anomaly_check",
        python_callable=task_anomaly,
        provide_context=True,
        doc_md="Threshold alerts → logs/anomaly_report.json",
    )

    bias = PythonOperator(
        task_id="bias_detection",
        python_callable=task_bias,
        provide_context=True,
        doc_md="Slice imbalance report → logs/bias_report.json",
    )

    # ── Linear pipeline ────────────────────────────────────────────────────────
    download >> analyze >> preprocess >> validate >> anomaly >> bias