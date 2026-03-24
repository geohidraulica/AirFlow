from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pendulum # type: ignore

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

default_args = {"owner": "StarRocks", "retries": 0}
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
    tags=["StarRocks", "Master"],
) as dag:

    with TaskGroup("dimensiones") as dimensiones:

        for name, func in DIMENSIONS.items():
            PythonOperator(
                task_id=name,
                python_callable=func
            )

    FactCompras = PythonOperator(
        task_id="FactCompras",
        python_callable=load_fact
    )

    dimensiones >> FactCompras