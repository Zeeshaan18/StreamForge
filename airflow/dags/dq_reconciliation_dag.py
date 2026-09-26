"""
Airflow DAG: Automated Data Quality & Warehouse Reconciliation.
Runs hourly to validate integrity, null constraints, and financial consistency.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "datapulse_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def audit_null_constraints(**kwargs):
    from airflow.plugins.custom_operators import DataQualityAuditRunner
    runner = DataQualityAuditRunner()
    result = runner.run_null_checks()
    if result["status"] == "FAILED":
        raise ValueError(f"Null constraint checks failed: {result}")
    return result


def audit_referential_integrity(**kwargs):
    from airflow.plugins.custom_operators import DataQualityAuditRunner
    runner = DataQualityAuditRunner()
    result = runner.run_referential_integrity_checks()
    if result["status"] == "FAILED":
        raise ValueError(f"Referential integrity checks failed: {result}")
    return result


def audit_financial_reconciliation(**kwargs):
    from airflow.plugins.custom_operators import DataQualityAuditRunner
    runner = DataQualityAuditRunner()
    result = runner.run_financial_reconciliation_checks()
    if result["status"] == "FAILED":
        raise ValueError(f"Financial reconciliation checks failed: {result}")
    return result


with DAG(
    dag_id="datapulse_dq_reconciliation_dag",
    default_args=default_args,
    description="Automated Data Quality and Schema Integrity Audit",
    schedule_interval="0 * * * *",  # Every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datapulse", "data_quality", "reconciliation"],
) as dag:

    task_null_checks = PythonOperator(
        task_id="audit_null_constraints",
        python_callable=audit_null_constraints,
    )

    task_ref_integrity = PythonOperator(
        task_id="audit_referential_integrity",
        python_callable=audit_referential_integrity,
    )

    task_fin_reconciliation = PythonOperator(
        task_id="audit_financial_reconciliation",
        python_callable=audit_financial_reconciliation,
    )

    # Orchestration dependencies
    task_null_checks >> task_ref_integrity >> task_fin_reconciliation
