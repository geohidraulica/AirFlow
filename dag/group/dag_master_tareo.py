from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import datetime
import pendulum # type: ignore


default_args = {"owner": "StarRocks","retries": 0}
local_tz = pendulum.timezone("America/Lima")

with DAG(
    dag_id="MasterTareo",
    description="Ejecuta todas las tablas relacionadas para el reporte tareo",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval=None,
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

        DimSite = TriggerDagRunOperator(
            task_id="DimSite",
            trigger_dag_id="DimSite",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimZona = TriggerDagRunOperator(
            task_id="DimZona",
            trigger_dag_id="DimZona",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimNivel = TriggerDagRunOperator(
            task_id="DimNivel",
            trigger_dag_id="DimNivel",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimPoza = TriggerDagRunOperator(
            task_id="DimPoza",
            trigger_dag_id="DimPoza",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimEstadoBomba = TriggerDagRunOperator(
            task_id="DimEstadoBomba",
            trigger_dag_id="DimEstadoBomba",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimPeriodoTareo = TriggerDagRunOperator(
            task_id="DimPeriodoTareo",
            trigger_dag_id="DimPeriodoTareo",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimAreaTareo = TriggerDagRunOperator(
            task_id="DimAreaTareo",
            trigger_dag_id="DimAreaTareo",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimCableBomba = TriggerDagRunOperator(
            task_id="DimCableBomba",
            trigger_dag_id="DimCableBomba",
            wait_for_completion=True,
            poke_interval=5,
        )

    FactTareo = TriggerDagRunOperator(
        task_id="FactTareo",
        trigger_dag_id="FactTareo",
        wait_for_completion=True,
        poke_interval=5,
    )

    dimensiones >> FactTareo
