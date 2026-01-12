from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
from pipelines.facts.fact_compras.load import load

default_args = {"owner": "StarRocks", "retries": 0}

with DAG(
    dag_id="FactCompras",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Etl", "StarRocks", "Fact"],
) as dag:

    run_task = PythonOperator(
        task_id="stream_load",
        python_callable = load
    )
