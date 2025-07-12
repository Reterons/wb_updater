import schedule
import time
from app import main
import logging

logging.basicConfig(filename='scheduler.log', level=logging.INFO)

def job():
    logging.info("Запуск задачи в %s", time.ctime())
    main()

# Запуск каждый час в :00 минут
schedule.every().hour.at(":00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)