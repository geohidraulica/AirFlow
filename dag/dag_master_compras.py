from airflow import DAG # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import datetime

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="ETL_Master_Group",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # solo se ejecuta manualmente o mediante triggers
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Etl", "StarRocks", "Group"],
) as dag:

    # Dispara el DAG de la dimensión
    trigger_dim = TriggerDagRunOperator(
        task_id="trigger_dim_estado_almacen",
        trigger_dag_id="DimEstadoAlmacen",
        wait_for_completion=True  # espera a que termine antes de continuar
    )

    # Dispara el DAG del hecho
    trigger_fact = TriggerDagRunOperator(
        task_id="trigger_fact_compras",
        trigger_dag_id="FactCompras",
        wait_for_completion=True
    )

    # Ejecuta primero la dimensión y luego el hecho (opcional)
    trigger_dim >> trigger_fact
