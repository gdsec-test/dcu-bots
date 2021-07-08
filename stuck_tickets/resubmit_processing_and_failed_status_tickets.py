import logging
import os
from configparser import ConfigParser
from datetime import datetime, timedelta
from logging.config import dictConfig

import yaml
from celery import Celery
from celeryconfig import CeleryConfig
from pymongo import MongoClient


class MongoHelperAPI:
    def __init__(self, _settings):
        """
        :param _settings:
        :return: None
        """
        self._logger = logging.getLogger(__name__)
        _client = MongoClient(_settings.get('db_url'))
        _client[_settings.get('db')].authenticate(_settings.get('db_user'),
                                                  _settings.get('db_pass'),
                                                  mechanism=_settings.get('db_auth_mechanism'))
        db = _client[_settings.get('db')]
        self._collection = db.incidents

    def handle(self):
        """
        :return: handle to the db collection
        """
        return self._collection


class ReturnToMiddleware:
    def __init__(self, _settings):
        """
        :param _settings:
        :return: None
        """
        self._logger = logging.getLogger(__name__)
        self._mongo = MongoHelperAPI(_settings)
        _capp = Celery()
        _capp.config_from_object(CeleryConfig(_settings))
        self._celery = _capp

    def find_tickets_in_processing(self):
        """
        Retrieves tickets in MongoDB that have been stuck in processing for more than one day since their creation and
        reinserts the task into Middleware queue for processing.
        :return: None
        """
        for _ticket in self._mongo.handle().find({'phishstory_status': 'PROCESSING',
                                                  'created': {'$lte': datetime.utcnow() - timedelta(hours=24)}}):
            self._send_to_middleware(_ticket)

    def find_failed_enrichment_tickets(self):
        """
        Retrieves tickets in MongoDB that have failed enrichment, which includes missing the vipHandling key,
        and reinserts the task into Middleware queue for processing.
        :return: None
        """
        for _ticket in self._mongo.handle().find({'phishstory_status': 'OPEN',
                                                  'created': {'$lte': datetime.utcnow() - timedelta(hours=48)},
                                                  '$and': [{'$or': [{'failedEnrichment': True},
                                                                    {'vipHandling': {'$exists': False}}]}]}):
            self._send_to_middleware(_ticket)

    def _send_to_middleware(self, _payload):
        """
        A helper function to send Celery tasks to the Middleware Queue with the provided payload
        :param _payload:
        :return: None
        """
        try:
            self._logger.info("Sending payload to Middleware {}.".format(_payload.get('ticketId')))
            self._celery.send_task('run.process', (_payload,))
        except Exception as e:
            self._logger.error("Unable to send payload to Middleware {} {}.".format(_payload.get('ticketId'), e))


if __name__ == '__main__':
    path = ''
    value = os.getenv('LOG_CFG', None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            l_config = yaml.safe_load(f.read())
        dictConfig(l_config)
    else:
        logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Retrieving tickets in PROCESSING status and tickets that have Failed Enrichment")

    mode = os.getenv('sysenv', 'dev')

    config_p = ConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_p.read('{}/connection_settings.ini'.format(dir_path))

    settings = dict(config_p.items(mode))

    middle = ReturnToMiddleware(settings)
    middle.find_tickets_in_processing()
    middle.find_failed_enrichment_tickets()
    logger.info("Completed")
