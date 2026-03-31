from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pendulum # type: ignore
import requests

from pipelines.dimensions.dim_equipo_bomba.load import load as load_equipo                                                          
from pipelines.dimensions.dim_orden_trabajo.load import load as load_ot
from pipelines.dimensions.dim_producto.load import load as load_producto
from pipelines.dimensions.dim_estado_item_ot.load import load as load_estado_item
from pipelines.dimensions.dim_site.load import load as load_site

from pipelines.facts.fact_stock_producto.load import load as load_stock
from pipelines.facts.fact_mantenimiento.load import load as load_fact

def notificar_dash(context):
    dag_id = context["dag"].dag_id
    try:
        requests.post(
            "http://192.168.3.118:8050/airflow-callback",
            json={"dag_id": dag_id, "estado": "success"},
            timeout=5
        )
    except Exception as e:
        print(f"No se pudo notificar a Dash: {e}")

default_args = {
    "owner": "StarRocks",
    "retries": 1
}

default_args = {"owner": "StarRocks", "retries": 0}
local_tz = pendulum.timezone("America/Lima")

DIMENSIONS = {
    "DimEquipoBomba": load_equipo,
    "DimOrdenTrabajo": load_ot,
    "DimProducto": load_producto,
    "DimEstadoItemOt": load_estado_item,
    "DimSite": load_site,
}

INTERMEDIATE = {
    "FactStockProducto": load_stock
}

with DAG(
    dag_id="MasterMantenimiento",
    description="Ejecuta todas las tablas relacionadas para el reporte de mantenimiento",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="0 8,12,16 * * 1-5",
    catchup=False,
    max_active_tasks=5,
    tags=["StarRocks", "Master"],
    on_success_callback=notificar_dash
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

    FactMantenimiento = PythonOperator(
        task_id="FactMantenimiento",
        python_callable=load_fact
    )

    dimensiones >> proceso_intermedio >> FactMantenimiento