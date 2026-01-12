import os
import tempfile
from datetime import datetime


def get_tmp_csv(job_name: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{job_name}_{timestamp}.csv"
    return os.path.join(tempfile.gettempdir(), filename)
