import urllib

from kombu import Exchange, Queue


class CeleryConfig:
    BROKER_TRANSPORT = 'pyamqp'
    BROKER_USE_SSL = True
    CELERY_TASK_SERIALIZER = 'pickle'
    CELERY_RESULT_SERIALIZER = 'json'
    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_IMPORTS = 'run'
    CELERYD_HIJACK_ROOT_LOGGER = False

    def __init__(self, settings):
        queue = settings.get('celery_queue')
        task = settings.get('celery_task')

        self.CELERY_QUEUES = (
            Queue(queue, Exchange(queue), routing_key=queue),
        )
        self.CELERY_ROUTES = {task: {'queue': queue}}
        self.BROKER_PASS = settings.get('broker_pass')
        self.BROKER_USER = settings.get('broker_user')
        self.BROKER_URL = settings.get('broker_url')
        self.BROKER_URL = 'amqp://' + self.BROKER_USER + ':' + urllib.quote(self.BROKER_PASS) + '@' + self.BROKER_URL
