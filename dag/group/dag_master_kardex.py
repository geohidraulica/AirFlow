from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import datetime
import pendulum # type: ignore


default_args = {"owner": "StarRocks","retries": 0}
local_tz = pendulum.timezone("America/Lima")

with DAG(
    dag_id="MasterKardex",
    description="Ejecuta todas las tablas relacionadas para el reporte kardex",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval=None,
    catchup=False,
    tags=["StarRocks", "Group"],
) as dag:

    with TaskGroup("dimensiones") as dimensiones:

        DimTipoMovimiento = TriggerDagRunOperator(
            task_id="DimTipoMovimiento",
            trigger_dag_id="DimTipoMovimiento",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimCentroCosto = TriggerDagRunOperator(
            task_id="DimCentroCosto",
            trigger_dag_id="DimCentroCosto",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimOrdenTrabajo = TriggerDagRunOperator(
            task_id="DimOrdenTrabajo",
            trigger_dag_id="DimOrdenTrabajo",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimEquipoBomba = TriggerDagRunOperator(
            task_id="DimEquipoBomba",
            trigger_dag_id="DimEquipoBomba",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimProducto = TriggerDagRunOperator(
            task_id="DimProducto",
            trigger_dag_id="DimProducto",
            wait_for_completion=True,
            poke_interval=5,
        )

        # BridgeEquipoxOt = TriggerDagRunOperator(
        #     task_id="BridgeEquipoxOt",
        #     trigger_dag_id="BridgeEquipoxOt",
        #     wait_for_completion=True,
        #     poke_interval=5,
        # )

    FactKardex = TriggerDagRunOperator(
        task_id="FactKardex",
        trigger_dag_id="FactKardex",
        wait_for_completion=True,
        poke_interval=5,
    )

    dimensiones >> FactKardex
