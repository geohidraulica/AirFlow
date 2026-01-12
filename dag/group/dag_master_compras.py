from airflow import DAG # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import datetime

default_args = {
    "owner": "Group",
    "retries": 0,
}

with DAG(
    dag_id="MasterCompras",
    description="Ejecuta todas las tablas relacionadas para el reporte compras",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Etl", "StarRocks", "Group"],
) as dag:

    DimEstadoAlmacen = TriggerDagRunOperator(
        task_id="DimEstadoAlmacen",
        trigger_dag_id="DimEstadoAlmacen",
        wait_for_completion=True,
    )

    DimEstadoItem = TriggerDagRunOperator(
        task_id="DimEstadoItem",
        trigger_dag_id="DimEstadoItem",
        wait_for_completion=True,
    )

    DimEstadoOc = TriggerDagRunOperator(
        task_id="DimEstadoOc",
        trigger_dag_id="DimEstadoOc",
        wait_for_completion=True,
    )

    DimFlujoCompra = TriggerDagRunOperator(
        task_id="DimFlujoCompra",
        trigger_dag_id="DimFlujoCompra",
        wait_for_completion=True,
    )

    DimFormaPago = TriggerDagRunOperator(
        task_id="DimFormaPago",
        trigger_dag_id="DimFormaPago",
        wait_for_completion=True,
    )

    DimMoneda = TriggerDagRunOperator(
        task_id="DimMoneda",
        trigger_dag_id="DimMoneda",
        wait_for_completion=True,
    )

    DimPersonal = TriggerDagRunOperator(
        task_id="DimPersonal",
        trigger_dag_id="DimPersonal",
        wait_for_completion=True,
    )

    DimProducto = TriggerDagRunOperator(
        task_id="DimProducto",
        trigger_dag_id="DimProducto",
        wait_for_completion=True,
    )

    DimProveedor = TriggerDagRunOperator(
        task_id="DimProveedor",
        trigger_dag_id="DimProveedor",
        wait_for_completion=True,
    )

    DimSite = TriggerDagRunOperator(
        task_id="DimSite",
        trigger_dag_id="DimSite",
        wait_for_completion=True,
    )

    DimTipoProyecto = TriggerDagRunOperator(
        task_id="DimTipoProyecto",
        trigger_dag_id="DimTipoProyecto",
        wait_for_completion=True,
    )

    DimTipoRequerimiento = TriggerDagRunOperator(
        task_id="DimTipoRequerimiento",
        trigger_dag_id="DimTipoRequerimiento",
        wait_for_completion=True,
    )

    FactCompras = TriggerDagRunOperator(
        task_id="FactCompras",
        trigger_dag_id="FactCompras",
        wait_for_completion=True,
    )

    [
        DimEstadoAlmacen,
        DimEstadoItem,
        DimEstadoOc,
        DimFlujoCompra,
        DimFormaPago,
        DimMoneda,
        DimPersonal,
        DimProducto,
        DimProveedor,
        DimSite,
        DimTipoProyecto,
        DimTipoRequerimiento,
    ] >> FactCompras
