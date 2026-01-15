from airflow import DAG  # type: ignore
# from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
from pipelines.operational.rrhh.registro_asistencia.load import load

default_args = {"owner": "RRHH", "retries": 1}

# DAG
with DAG(
    dag_id="RegistroAsistencia",
    description="Sincronizacion de registro de asistencia de cada empleado",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6-11,17-20 * * 1-5",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Operational"],
) as dag:

    run_task = PythonOperator(
        task_id="etl_registro_asitencia",
        python_callable = load
    )