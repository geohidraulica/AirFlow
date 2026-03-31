from airflow import DAG  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime
import pendulum  # type: ignore
import requests

from pipelines.dimensions.dim_estado_compra.load import load as load_estado
from pipelines.dimensions.dim_flujo_compra.load import load as load_flujo
from pipelines.dimensions.dim_forma_pago.load import load as load_forma_pago
from pipelines.dimensions.dim_moneda.load import load as load_moneda
from pipelines.dimensions.dim_personal.load import load as load_personal
from pipelines.dimensions.dim_producto.load import load as load_producto
from pipelines.dimensions.dim_proveedor.load import load as load_proveedor
from pipelines.dimensions.dim_site.load import load as load_site
from pipelines.dimensions.dim_tipo_proyecto.load import load as load_tipo_proyecto
from pipelines.dimensions.dim_tipo_requerimiento.load import load as load_tipo_req

from pipelines.facts.fact_compras.load import load as load_fact

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

local_tz = pendulum.timezone("America/Lima")

DIMENSIONS = {
    "DimEstadoCompra": load_estado,
    "DimFlujoCompra": load_flujo,
    "DimFormaPago": load_forma_pago,
    "DimMoneda": load_moneda,
    "DimPersonal": load_personal,
    "DimProducto": load_producto,
    "DimProveedor": load_proveedor,
    "DimSite": load_site,
    "DimTipoProyecto": load_tipo_proyecto,
    "DimTipoRequerimiento": load_tipo_req,
}

with DAG(
    dag_id="MasterCompras",
    description="Ejecuta todas las tablas relacionadas para el reporte compras",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="0 7,11,15 * * 1-5",
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1,
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

    FactCompras = PythonOperator(
        task_id="FactCompras",
        python_callable=load_fact
    )
    
    dimensiones >> FactCompras