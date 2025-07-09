from datetime import datetime, timedelta

def get_yesterday_start():
    return datetime.strptime((datetime.today() - timedelta(1)).strftime('%d/%m/%Y') + " 00:00:00", "%d/%m/%Y %H:%M:%S")
