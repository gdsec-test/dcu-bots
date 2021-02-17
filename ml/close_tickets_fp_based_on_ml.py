import logging
import os
import socket
from configparser import ConfigParser
from datetime import datetime
from logging.config import dictConfig

import requests
import yaml
from pymongo import MongoClient


class APIHelper(object):
    """
    This class handles access to the DCU Abuse API
    """
    PAYLOAD = {'close_reason': 'false_positive', 'closed': 'true'}

    def __init__(self, _env_settings):
        """
        :param _env_settings:
        :return: None
        """
        self._logger = logging.getLogger(__name__)
        self._url = _env_settings.get('abuse_api')
        self._header = {'Authorization': _env_settings.get('dcu_middleware_jwt')}

    def close_incident(self, _ticket_id):
        """
        Closes out the provided ticket id as FP using the API PATCH endpoint
        :param _ticket_id:
        :return: boolean
        """
        _success = False
        try:
            _r = requests.patch('{}/{}'.format(self._url, _ticket_id),
                                json=self.PAYLOAD,
                                headers=self._header)
            if _r.status_code == 204:
                _success = True
            else:
                self._logger.warning('Unable to close ticket {} {}'.format(_ticket_id, _r.content))
        except Exception as _e:
            self._logger.error('Exception while closing ticket {} {}'.format(_ticket_id, _e))
        return _success


class DBHelper:
    """
    DB helper class specific to the PhishStory databases
    """
    def __init__(self, _env_settings, _api_handle):
        """
        :param _env_settings: dict from ini settings file
        :param _api_handle: handle to the APIHelper class
        :return: boolean
        """
        self._logger = logging.getLogger(__name__)
        _client = MongoClient(_env_settings.get('db_url'))
        _client[_env_settings.get('db')].authenticate(_env_settings.get('db_user'),
                                                      _env_settings.get('db_pass'),
                                                      mechanism=_env_settings.get('db_auth_mechanism'))
        _db = _client[settings.get('db')]
        self._api_handle = _api_handle
        self._collection = _db.incidents
        self._client = _client

    def close_connection(self):
        """
        Closes the connection to the db
        :return: None
        """
        self._client.close()

    def _update_actions_sub_document(self, _ticket_id):
        """
        Update the database record with a new actions sub-document and entry
        :param _ticket_id: string ticket id for the database record to modify
        :return: boolean
        """
        _success = False
        # *** HARDCODED the method which performs the ticket closure ***
        origin_string = '{}:{}:APIHelper:close_incident'.format(socket.gethostname(), __file__)
        if self._collection.update_one(
                {'_id': _ticket_id},
                {
                    '$push': {
                        'actions': {
                            'origin': origin_string,
                            'timestamp': datetime.utcnow(),
                            'message': 'closed as false_positive',
                            'user': 'ml_automation'
                        }
                    }
                },
                upsert=True):
            _success = True
        return _success

    def close_tickets_with_low_fraud_scores(self):
        """
        Find all open Phishing tickets with low fraud scores, between 0 and 0.05, and send them to the API for closure
        :return: None
        """
        self._logger.info('Start DB Ticket Closures')

        # Find all open phishing tickets with a low fraud score
        _cursor = self._collection.find(
            {
                'type': 'PHISHING',
                'phishstory_status': 'OPEN',
                '$and': [
                    {'fraud_score': {'$gte': 0.0}},
                    {'fraud_score': {'$lte': 0.05}}
                ]
            }
        )

        for _ticket in _cursor:
            _ticket_id = _ticket.get('ticketId')
            self._logger.info('Closing {} via API'.format(_ticket_id))
            if self._api_handle.close_incident(_ticket_id):
                if not self._update_actions_sub_document(_ticket_id):
                    self._logger.warning('Unable to add actions sub-document to {}'.format(_ticket_id))

        self._logger.info('Finish DB Ticket Closures')


def read_config():
    """
    Reads the configuration ini file for the env specific settings
    :return: dict of configuration settings for the env
    """
    _dir_path = os.path.dirname(os.path.realpath(__file__))
    _config_p = ConfigParser()
    _config_p.read('{}/connection_settings.ini'.format(_dir_path))
    return dict(_config_p.items(os.getenv('sysenv', 'dev')))


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    try:
        _path = '/home/dcu-bots/ml/logging.yaml'
        if _path and os.path.exists(_path):
            with open(_path, 'rt') as f:
                _l_config = yaml.safe_load(f.read())
            dictConfig(_l_config)
        else:
            logging.basicConfig(level=logging.INFO)
    except Exception:
        logging.basicConfig(level=logging.INFO)
    finally:
        return logging.getLogger(__name__)


if __name__ == '__main__':
    raise SystemExit('Preventing unintentional execution of this script')
    """
    This script should be used whenever DCU wants to auto-close OPEN Phishing tickets in PhishStory as
    FALSE_POSITIVE whenever the fraud_score values are between 0 and 0.05
    """
    PROCESS_NAME = 'PhishStory FP Ticket Closure for Low Fraud Score Process'

    logger = setup_logging()
    logger.info('Started {}'.format(PROCESS_NAME))

    db_client = None
    try:
        settings = read_config()

        # Create handle to the Abuse API
        api_client = APIHelper(settings)

        # Create handle to the DB
        db_client = DBHelper(settings, api_client)

        # Use DB helper to (find/close) tickets with low fraud scores
        db_client.close_tickets_with_low_fraud_scores()

    except Exception as e:
        logger.fatal(e)
    finally:
        if db_client:
            db_client.close_connection()
        logger.info('Finished {}\n'.format(PROCESS_NAME))
