import subprocess
import os
import time  # <<--- importamos time

# ========================
# Rutas Linux
# ========================
SPARK_HOME = "/opt/spark/spark"
SPARK_SUBMIT_PATH = os.path.join(SPARK_HOME, "bin", "spark-submit")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SPARK_RUNNER_PATH = os.path.join(BASE_DIR, "spark_runner.py")

def run_spark(etl_name):
    """Ejecuta un ETL usando spark-submit"""
    print(f"Ejecutando ETL: {etl_name}")
    start_time = time.time()  # <<--- inicio del cronómetro
    
    subprocess.run([
        SPARK_SUBMIT_PATH,
        "--jars", "/opt/etl/jars/mssql-jdbc-12.4.3.jre11.jar",
        SPARK_RUNNER_PATH,
        etl_name
    ], check=True)
    
    end_time = time.time()  # <<--- fin del cronómetro
    elapsed = end_time - start_time
    print(f"ETL '{etl_name}' finalizado en {elapsed:.2f} segundos.")

# ====================================================
# CONFIG de ETLs disponibles
# ====================================================
ETLS = [
    "etl_registro_asistencia",
]

if __name__ == "__main__":
    for etl in ETLS:
        run_spark(etl)
    print("Todos los ETLs han sido ejecutados.")
