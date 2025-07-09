from datetime import datetime, timedelta
from database.db_connection import get_db_connection
from services.hik_service import fetch_hik_data_range
from utils.logger import setup_logger
from utils.time_utils import get_yesterday_start
from database.db_connection import safe_execute

logger = setup_logger("main")

def run_sync():
    conn = get_db_connection()
    cursor_holder = {"conn": conn, "cursor": conn.cursor()}
    
    safe_execute("SELECT MAX([Time]) FROM fabi.sales_invoice_items", cursor_holder)
    start_date = cursor_holder['cursor'].fetchone()[0]
    if not start_date:
        start_date = datetime.today() - timedelta(days=7)
    end_date = get_yesterday_start()
    fetch_hik_data_range(start_date, end_date, cursor_holder)

if __name__ == "__main__":
    run_sync()
