from apscheduler.schedulers.background import BackgroundScheduler

from core.schedulers.airflow_sync_scheduler import trigger_sync_job
from core.schedulers.dag_cleaner_scheduler import dag_cleaner_job
from core.schedulers.trigger_scheduler import trigger_job

scheduler = BackgroundScheduler()


def start_scheduler():
    scheduler.add_job(trigger_job, "interval", seconds=10)
    scheduler.add_job(trigger_sync_job, "interval", seconds=30)
    scheduler.add_job(dag_cleaner_job, "interval", seconds=600)
    scheduler.start()
