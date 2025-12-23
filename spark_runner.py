# spark_runner.py
import sys

def run_pipeline(etl_name):
    module = __import__(f"etl.{etl_name}", fromlist=["run_etl"])
    run_etl = getattr(module, "run_etl")
    run_etl()

if __name__ == "__main__":
    etl_name = sys.argv[1]  # ejemplo: registro_asistencia
    run_pipeline(etl_name)
