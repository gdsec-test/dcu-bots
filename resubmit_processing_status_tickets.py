import logging
import os
from celery import Celery
from celeryconfig import CeleryConfig
from ConfigParser import SafeConfigParser
from datetime import datetime, timedelta
from pymongo import MongoClient


class MongoHelperAPI:
    def __init__(self, settings):
        self._logger = logging.getLogger(__name__)
        client = MongoClient(settings.get('db_url'))
        client[settings.get('db')].authenticate(settings.get('db_user'), settings.get('db_pass'),
                                                mechanism=settings.get('db_auth_mechanism'))
        db = client[settings.get('db')]
        self._collection = db.incidents

    def handle(self):
        return self._collection


class ReturntoMiddleware:
    def __init__(self, settings):
        self._logger = logging.getLogger(__name__)
        self._mongo = MongoHelperAPI(settings)
        capp = Celery()
        capp.config_from_object(CeleryConfig(settings))
        self._celery = capp

    def find_tickets_in_processing(self):
        """
        Retrieves tickets in MongoDB that have been stuck in processing for more than one day since their creation and
        reinserts the task into Middleware queue for processing.
        """
        for ticket in self._mongo.handle().find({'phishstory_status': 'PROCESSING', 'created': {'$lte': datetime.utcnow() - timedelta(hours=24)}}):
            self._send_to_middleware(ticket)

    def _send_to_middleware(self, payload):
        """
        A helper function to send Celery tasks to the Middleware Queue with the provided payload
        :param payload:
        :return:
        """
        try:
            self._logger.info("Sending payload to Middleware {}.".format(payload.get('ticketId')))
            self._celery.send_task('run.process', (payload,))
        except Exception as e:
            self._logger.error("Unable to send payload to Middleware {} {}.".format(payload.get('ticketId'), e.message))

if __name__ == '__main__':
    logging.basicConfig(filename='submit_processing_tickets.log', level=logging.INFO)

    mode = os.getenv('prod')

    configp = SafeConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    configp.read('{}/connection_settings.ini'.format(dir_path))

    settings = dict(configp.items(mode))

    middle = ReturntoMiddleware(settings)
    middle.find_tickets_in_processing()
