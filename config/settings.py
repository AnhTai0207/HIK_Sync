import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_PORT = os.getenv("SQL_PORT")
SQL_UID = os.getenv("SQL_UID")
SQL_PWD = os.getenv("SQL_PWD")
SQL_DB = os.getenv("SQL_DB")

IPOS_TOKEN = os.getenv("IPOS_TOKEN")
ARTEMIS_KEY = os.getenv("ARTEMIS_KEY")
ARTEMIS_SECRET = os.getenv("ARTEMIS_SECRET")
ARTEMIS_URL = os.getenv("ARTEMIS_URL")
