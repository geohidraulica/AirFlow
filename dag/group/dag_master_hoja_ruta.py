from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime 
import pendulum # type: ignore

from pipelines.dimensions.dim_producto.load import load as load_producto
from pipelines.dimensions.dim_equipo_bomba.load import load as load_equipo_bomba
from pipelines.dimensions.dim_orden_fabricacion.load import load as load_of
from pipelines.dimensions.dim_maquina_produccion.load import load as load_maquina_produccion
from pipelines.dimensions.dim_servicio.load import load as load_servicio

from pipelines.facts.fact_hoja_ruta.load import load as load_fact

default_args = {"owner": "StarRocks", "retries": 0}
local_tz = pendulum.timezone("America/Lima")

DIMENSIONS = {
    "DimProducto": load_producto,
    "DimEquipoBomba": load_equipo_bomba,
    "DimOrdenFabricacion": load_of,
    "DimMaquinaProduccion": load_maquina_produccion,
    "DimServicio": load_servicio,
}

with DAG(
    dag_id="MasterHojaRuta",
    description="Ejecuta todas las tablas relacionadas para el reporte hoja de ruta",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="0 7-19 * * 1-5",
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

    FactHojaRuta = PythonOperator(
        task_id="FactHojaRuta",
        python_callable=load_fact
    )

    dimensiones >> FactHojaRuta