import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "secrets.env")

load_dotenv(ENV_PATH)

CONFIG = {
    "fuxion": {
        "url": os.getenv("URL_FUXION"),
        "user": os.getenv("USER_FUXION"),
        "pass": os.getenv("PASS_FUXION"),
        "server": os.getenv("SERVER_FUXION"),
        "database": os.getenv("DATABASE_FUXION")
    },
    "ssis": {
        "url": os.getenv("URL_SSIS"),
        "user": os.getenv("USER_SSIS"),
        "pass": os.getenv("PASS_SSIS"),
        "server": os.getenv("SERVER_SSIS"),
        "database": os.getenv("DATABASE_SSIS")
    },
    "garita": {
        "url": os.getenv("URL_GARITA"),
        "user": os.getenv("USER_GARITA"),
        "pass": os.getenv("PASS_GARITA"),
        "server": os.getenv("SERVER_GARITA"),
        "database": os.getenv("DATABASE_GARITA")
    },
    "starrocks": {
        "url": os.getenv("URL_STARROCKS"),
        "user": os.getenv("USER_STARROCKS"),
        "pass": os.getenv("PASS_STARROCKS"),
        "server": os.getenv("SERVER_STARROCKS"),
        "database": os.getenv("DATABASE_STARROCKS")
    },
    "mysql":{
        "driver": "com.mysql.cj.jdbc.Driver",
        "jar": os.getenv("MYSQL_JAR")
    },
    "sql": {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jar": os.getenv("SQL_JAR")
    },
    "oracle": {

    },
    "postgresql": {

    }
}
