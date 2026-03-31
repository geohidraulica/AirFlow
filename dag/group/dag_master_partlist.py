from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pendulum # type: ignore

from pipelines.dimensions.dim_partlist.load import load as load_partlist
from pipelines.dimensions.dim_producto.load import load as load_producto

from pipelines.facts.fact_stock_producto.load import load as load_stock
from pipelines.facts.fact_partlist_detalle.load import load as load_fact

default_args = {"owner": "StarRocks", "retries": 0}
local_tz = pendulum.timezone("America/Lima")

DIMENSIONS = {
    "DimPartList": load_partlist,
    "DimProducto": load_producto,
}

INTERMEDIATE = {
    "FactStockProducto": load_stock
}

with DAG(
    dag_id="MasterPartList",
    description="Ejecuta todas las tablas relacionadas para el reporte de partlist detalle",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="0 10,12,16 * * 1-5",
    catchup=False,
    max_active_tasks=5,
    tags=["StarRocks", "Master"],
) as dag:

    with TaskGroup("dimensiones") as dimensiones:
        
        prev_task = None

        for name, func in DIMENSIONS.items():
            task = PythonOperator(
                task_id=name,
                python_callable=func
            )
            if prev_task:
                prev_task >> task

            prev_task = task

    with TaskGroup("proceso_intermedio") as proceso_intermedio:
        for name, func in INTERMEDIATE.items():
            PythonOperator(
                task_id=name,
                python_callable=func
            )

    FactPartListDetalle = PythonOperator(
        task_id="FactPartListDetalle",
        python_callable=load_fact
    )

    dimensiones >> proceso_intermedio >> FactPartListDetalle