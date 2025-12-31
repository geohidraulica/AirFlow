from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from datetime import datetime, timedelta

# Argumentos por defecto
default_args = {
    "owner": "jhon aasdas",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG
with DAG(
    dag_id="sds",
    default_args=default_args,
    description="DAG para ejecutar Spark con Java 11",
    schedule_interval="@daily",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    run_etl_empleado = BashOperator(
        task_id="run_etl_empleado",
        bash_command="""
        echo "JAVA_HOME=$JAVA_HOME"
        java -version

        /opt/spark/spark/bin/spark-submit \
            --jars /opt/etl/jars/mssql-jdbc-12.4.3.jre11.jar \
            /opt/etl/AirFlow/pipelines/operational/rrhh/empleado/load.py
        """
    )