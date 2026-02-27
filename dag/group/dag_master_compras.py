from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import datetime
import pendulum # type: ignore


default_args = {"owner": "StarRocks","retries": 0}
local_tz = pendulum.timezone("America/Lima")

with DAG(
    dag_id="MasterCompras",
    description="Ejecuta todas las tablas relacionadas para el reporte compras",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="0 7,11,15 * * 1-5",
    catchup=False,
    tags=["StarRocks", "Group"],
) as dag:

    with TaskGroup("dimensiones") as dimensiones:

        DimEstadoCompra = TriggerDagRunOperator(
            task_id="DimEstadoCompra",
            trigger_dag_id="DimEstadoCompra",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimFlujoCompra = TriggerDagRunOperator(
            task_id="DimFlujoCompra",
            trigger_dag_id="DimFlujoCompra",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimFormaPago = TriggerDagRunOperator(
            task_id="DimFormaPago",
            trigger_dag_id="DimFormaPago",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimMoneda = TriggerDagRunOperator(
            task_id="DimMoneda",
            trigger_dag_id="DimMoneda",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimPersonal = TriggerDagRunOperator(
            task_id="DimPersonal",
            trigger_dag_id="DimPersonal",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimProducto = TriggerDagRunOperator(
            task_id="DimProducto",
            trigger_dag_id="DimProducto",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimProveedor = TriggerDagRunOperator(
            task_id="DimProveedor",
            trigger_dag_id="DimProveedor",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimSite = TriggerDagRunOperator(
            task_id="DimSite",
            trigger_dag_id="DimSite",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimTipoProyecto = TriggerDagRunOperator(
            task_id="DimTipoProyecto",
            trigger_dag_id="DimTipoProyecto",
            wait_for_completion=True,
            poke_interval=5,
        )

        DimTipoRequerimiento = TriggerDagRunOperator(
            task_id="DimTipoRequerimiento",
            trigger_dag_id="DimTipoRequerimiento",
            wait_for_completion=True,
            poke_interval=5,
        )

    FactCompras = TriggerDagRunOperator(
        task_id="FactCompras",
        trigger_dag_id="FactCompras",
        wait_for_completion=True,
        poke_interval=5,
    )

    dimensiones >> FactCompras
