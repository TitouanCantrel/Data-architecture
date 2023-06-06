from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import time

def init():
    print('begin init...')
    time.sleep(5)
    print('end init...')


def scan():
    print('begin netscan')
    time.sleep(5)
    print('end netscan...')

def register():
    print('begin register...')
    time.sleep(5)
    print('end register...')


if __name__ == '__main__':
    scheduler = BlockingScheduler(timezone='Europe/Paris', executors={'default': ThreadPoolExecutor(1)})
    scheduler.add_job(init)
    scheduler.add_job(scan, "interval", seconds=5, misfire_grace_time=30)
    scheduler.add_job(register, "interval", seconds=5, misfire_grace_time=30)
    scheduler.print_jobs()
    scheduler.start()

    try:
        while True:
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()