from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pendulum # type: ignore

from pipelines.dimensions.dim_equipo_bomba.load import load as load_equipo
from pipelines.dimensions.dim_orden_trabajo.load import load as load_ot
from pipelines.dimensions.dim_producto.load import load as load_producto
from pipelines.dimensions.dim_estado_item_ot.load import load as load_estado_item
from pipelines.dimensions.dim_site.load import load as load_site

from pipelines.facts.fact_stock_producto.load import load as load_stock
from pipelines.facts.fact_mantenimiento.load import load as load_fact

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
) as dag:

    with TaskGroup("dimensiones") as dimensiones:
        for name, func in DIMENSIONS.items():
            PythonOperator(
                task_id=name,
                python_callable=func
            )

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