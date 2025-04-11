from apscheduler.schedulers.background import BackgroundScheduler

from core.scheduler.airflow_sync_scheduler import trigger_sync_job
from core.scheduler.trigger_scheduler import trigger_job

scheduler = BackgroundScheduler()


def start_scheduler():
    scheduler.add_job(trigger_job, "interval", seconds=10)
    scheduler.add_job(trigger_sync_job, "interval", seconds=30)
    scheduler.start()
