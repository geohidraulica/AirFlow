from airflow import DAG #type: ignore
from airflow.utils.task_group import TaskGroup #type: ignore
from airflow.operators.python import PythonOperator #type: ignore
from datetime import datetime 
import pendulum #type: ignore

from pipelines.dimensions.dim_equipo_bomba.load import load as load_equipo
from pipelines.dimensions.dim_site.load import load as load_site
from pipelines.dimensions.dim_zona.load import load as load_zona
from pipelines.dimensions.dim_nivel.load import load as load_nivel
from pipelines.dimensions.dim_poza.load import load as load_poza
from pipelines.dimensions.dim_estado_bomba.load import load as load_estado_bomba
from pipelines.dimensions.dim_periodo_tareo.load import load as load_periodo
from pipelines.dimensions.dim_area_tareo.load import load as load_area
from pipelines.dimensions.dim_cable_bomba.load import load as load_cable

from pipelines.facts.fact_tareo.load import load as load_fact

default_args = {"owner": "StarRocks", "retries": 0}
local_tz = pendulum.timezone("America/Lima")

DIMENSIONS = {
    "DimEquipoBomba": load_equipo,
    "DimSite": load_site,
    "DimZona": load_zona,
    "DimNivel": load_nivel,
    "DimPoza": load_poza,
    "DimEstadoBomba": load_estado_bomba,
    "DimPeriodoTareo": load_periodo,
    "DimAreaTareo": load_area,
    "DimCableBomba": load_cable,
}

with DAG(
    dag_id="MasterTareo",
    description="Ejecuta todas las tablas relacionadas para el reporte tareo",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval=None,
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

    FactTareo = PythonOperator(
        task_id="FactTareo",
        python_callable=load_fact
    )

    dimensiones >> FactTareo