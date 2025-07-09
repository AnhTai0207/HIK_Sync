import pyodbc
import time
from config import settings
from utils.logger import setup_logger

logger = setup_logger("db_connection")

def get_db_connection():
    return pyodbc.connect(
        f'Driver={{ODBC Driver 18 for SQL Server}};'
        f'Server={settings.SQL_SERVER};Port={settings.SQL_PORT};'
        f'uid={settings.SQL_UID};pwd={settings.SQL_PWD};DATABASE={settings.SQL_DB};'
        f'Trusted_Connection=No;TrustServerCertificate=yes',
        ansi=True
    )


def reconnect_connection(cursor_holder: dict, retries=3, wait=3):
    for attempt in range(retries):
        try:
            logger.warning(f"Attempting DB reconnect... ({attempt + 1}/{retries})")
            conn = get_db_connection()
            cursor_holder["conn"] = conn
            cursor_holder["cursor"] = conn.cursor()
            logger.info("Reconnected to SQL Server successfully.")
            return
        except Exception as e:
            logger.error(f"Reconnect failed: {e}")
            time.sleep(wait)
    raise Exception("Exceeded max retries to reconnect to DB.")

def safe_execute(query, cursor_holder):
    try:
        cursor_holder["cursor"].execute(query)
    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
        reconnect_connection(cursor_holder)
        cursor_holder["cursor"].execute(query)
