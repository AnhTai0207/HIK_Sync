import requests, hashlib, hmac, base64
from datetime import datetime, timedelta
from config.settings import ARTEMIS_KEY, ARTEMIS_SECRET, ARTEMIS_URL
from utils.logger import setup_logger
from database.db_connection import safe_execute

logger = setup_logger("artemis_service")

def generate_signature():
    msg = "GET\\n*/*\\napplication/json\\n/artemis/api/aiapplication/v1/people/statisticsTotalNumByTime"
    return base64.b64encode(hmac.new(ARTEMIS_SECRET.encode(), msg.encode(), hashlib.sha256).digest()).decode()

def fetch_hik_data_range(start_date, end_date, cursor_holder):
    headers = {"X-Ca-Key": ARTEMIS_KEY, "X-Ca-Signature": generate_signature()}
    for day in range((end_date - start_date).days + 1):
        day_start = start_date + timedelta(days=day)
        body = {
            "pageNo": 1, "pageSize": 1, "cameraIndexCodes": "222,306,210,216",
            "statisticsType": 1,
            "startTime": day_start.strftime("%Y-%m-%dT%H:%M:%S+07:00"),
            "endTime": day_start.strftime("%Y-%m-%dT23:59:00+07:00")
        }
        try:
            res = requests.get(url=ARTEMIS_URL, headers=headers, json=body, verify=False).json()
            for cam in res["data"]["list"]:
                try:
                    dt = datetime.strptime(cam['time'], "%Y-%m-%dT%H:%M:%S+07:00")
                except:
                    dt = datetime.strptime(cam['time'], "%Y-%m-%dT%H:%M:%S+08:00")
                q = f"INSERT INTO cam.camera_Menas ([Time], cameraIndexCode, exitNum, enterNum) VALUES ('{dt}', {cam['cameraIndexCode']}, {cam['exitNum']}, {cam['enterNum']})"
                safe_execute(q, cursor_holder)
            cursor_holder['cursor'].commit()
        except Exception as e:
            logger.error(f"Artemis error {day_start}: {e}")