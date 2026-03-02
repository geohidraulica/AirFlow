from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
from pipelines.dimensions.dim_zona.load import load

default_args = {"owner": "StarRocks", "retries": 0}

with DAG(
    dag_id="DimZona",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["MTTO", "StarRocks", "Dimensiones"],
) as dag:

    run_task = PythonOperator(
        task_id="DimZona",
        python_callable = load
    )
