from celery import Celery
from decouple import config

celery_app = Celery(
    'worker',
    broker=config('CELERY_BROKER_URL'),
    backend=config('CELERY_RESULT_BACKEND'),
)

celery_app.conf.update(
    result_expires=3600,
    worker_pool='prefork',
    broker_connection_retry_on_startup=True,
    worker_hijack_root_logger=False,
    worker_log_format='%(asctime)s [%(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='%(asctime)s [%(levelname)s/%(task_name)s] %(message)s',
    log_level='DEBUG'
)
