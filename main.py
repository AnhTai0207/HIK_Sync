from datetime import datetime, timedelta
from database.db_connection import get_db_connection
from services.hik_service import fetch_hik_data_range
from utils.logger import setup_logger
from utils.time_utils import get_yesterday_start
from database.db_connection import safe_execute

logger = setup_logger("main")

def run_sync():
    logger.info("Starting sync process...")
    conn = get_db_connection()
    cursor_holder = {"conn": conn, "cursor": conn.cursor()}
    
    safe_execute("SELECT MAX([Time]) FROM cam.camera_Menas", cursor_holder)
    start_date = datetime.combine(cursor_holder['cursor'].fetchone()[0], datetime.min.time())
    end_date = get_yesterday_start()
    fetch_hik_data_range(start_date, end_date, cursor_holder)
    logger.info("Sync process completed.")

if __name__ == "__main__":
    run_sync()
