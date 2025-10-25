from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_DIR = "/opt/earthquake_project"

PYTHON_EXECUTABLE = "python"

@dag(
    dag_id="earthquake_pipeline",
    start_date=datetime(2025, 10, 23),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "earthquake", "dbt"],
)
# Orquesta el pipeline de procesamiento de datos de terremotos:
def earthquake_pipeline():
    task_extract_load = BashOperator(
        task_id="extract_load_earthquake_data",
        bash_command=f"{PYTHON_EXECUTABLE} {PROJECT_DIR}/src/extract_load_gcs.py"
    )
    task_dbt_run = BashOperator(
    task_id="dbt_run_transform",
    bash_command=(
            f"cd {PROJECT_DIR}/earthquake_dbt_project && "
            f"dbt clean && "
            f"dbt run --profiles-dir ."
        )
)
    task_extract_load >> task_dbt_run
earthquake_pipeline()
