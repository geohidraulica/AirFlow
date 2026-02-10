from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import datetime

default_args = {
    "owner": "StarRocks",
    "retries": 0,
}

with DAG(
    dag_id="MasterMantenimiento",
    description="Ejecuta todas las tablas relacionadas para el reporte de mantenimiento",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 8,12,16 * * 1-5",
    catchup=False,
    tags=["StarRocks", "Group"],
) as dag:
    
    with TaskGroup("dimensiones") as dimensiones:

        DimEquipoBomba = TriggerDagRunOperator(
            task_id="DimEquipoBomba",
            trigger_dag_id="DimEquipoBomba",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimOrdenTrabajo = TriggerDagRunOperator(
            task_id="DimOrdenTrabajo",
            trigger_dag_id="DimOrdenTrabajo",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimProducto = TriggerDagRunOperator(
            task_id="DimProducto",
            trigger_dag_id="DimProducto",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimEstadoItemOt = TriggerDagRunOperator(
            task_id="DimEstadoItemOt",
            trigger_dag_id="DimEstadoItemOt",
            wait_for_completion=True,
            poke_interval=5,
        )

        FactStockProducto = TriggerDagRunOperator(
            task_id="FactStockProducto",
            trigger_dag_id="FactStockProducto",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimSite = TriggerDagRunOperator(
            task_id="DimSite",
            trigger_dag_id="DimSite",
            wait_for_completion=True,
            poke_interval=5,
        )

    FactMantenimiento = TriggerDagRunOperator(
        task_id="FactMantenimiento",
        trigger_dag_id="FactMantenimiento",
        wait_for_completion=True,
        poke_interval=5,
    )

    dimensiones >> FactMantenimiento
