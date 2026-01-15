from airflow import DAG  # type: ignore
# from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
from pipelines.operational.rrhh.horario_empleado.load import load

default_args = {"owner": "RRHH", "retries": 1}

# DAG
with DAG(
    dag_id="HorarioEmpleado",
    description="Sincronizacion de horario de trabajo de cada empleado",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval= None, #"*/5 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Operational"],
) as dag:

    run_task = PythonOperator(
        task_id="etl_horario_empleado",
        python_callable = load
    )

    # Run Spark
    # run_etl_empleado = BashOperator(
    #     task_id="run_etl_horario_empleado",
    #     bash_command="""
    #     echo "JAVA_HOME=$JAVA_HOME"
    #     java -version

    #     /opt/spark/spark/bin/spark-submit \
    #         --jars /opt/etl/jars/mssql-jdbc-12.4.3.jre11.jar \
    #         /opt/etl/AirFlow/pipelines/operational/rrhh/horario_empleado/load.py
    #     """
    # )