import json
import logging
import os
from configparser import ConfigParser
from datetime import datetime, timedelta
from logging.config import dictConfig

import requests
import yaml
from pymongo import MongoClient


class SNOWHelper:
    """
    Get all tickets that were created in SNOW Kelvin
    """
    HEADERS = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    KEY_SLACK_CHANNEL = 'slack_channel'
    KEY_SLACK_URL = 'slack_url'
    MESSAGE_MISSING = '<!here> Corresponding SNOW ticket(s) do not exist for the following MONGODB ticket(s):\n'
    MESSAGE_UNEXPECTED = '<!here> Unexpected response received from SNOW for the following URL(s):\n'

    def __init__(self, _env_settings):
        """
        :param _env_settings: dict from ini settings file
        """
        self._logger = logging.getLogger(__name__)
        self._settings = _env_settings
        self._auth = (_env_settings.get('snow_user'), _env_settings.get('snow_pass'))
        self._url = _env_settings.get('snow_url')

    def _write_to_slack(self, message_list, message):
        """
            Writes message to a slack channel
            :param message_list: The list of tickets
            :param message: message body
            :return: none
        """
        if len(message_list):
            payload = {
                'payload': json.dumps(
                    {
                        'channel': self._settings.get(self.KEY_SLACK_CHANNEL),
                        'username': 'API BOT',
                        'text': message + '\n'.join(message_list)
                    }
                )
            }
            requests.post(self._settings.get(self.KEY_SLACK_URL), data=payload)

    def reconcile_snow_tickets_against_mongodb(self, _tickets_closed_in_mongodb_since_yesterday):
        """
        Get all tickets that were created in SNOW Kelvin
        :param _tickets_closed_in_mongodb_since_yesterday: List of closed mongoDB tickets in the past 24 hours
        """
        self._logger.info('Start SNOW Ticket Retrieval')
        _missing_snow_tickets = []
        _unexpected_responses = []

        for _t in _tickets_closed_in_mongodb_since_yesterday:
            _snow_ticket_url = self._url.format(_t)
            _response = requests.get(_snow_ticket_url,
                                     auth=self._auth,
                                     headers=self.HEADERS)

            if _response.status_code != 200:
                self._logger.info('URL: {} ; Status: {}\n'.format(_snow_ticket_url,
                                                                  _response.status_code))
                _unexpected_responses.append('URL: {} ; Status: {}\n'.format(_snow_ticket_url,
                                                                             _response.status_code))
            else:
                if len(_response.json()[u'result']) == 0:
                    _missing_snow_tickets.append(_t)

        if len(_missing_snow_tickets) > 0:
            self._write_to_slack(_missing_snow_tickets, self.MESSAGE_MISSING)

        if len(_unexpected_responses) > 0:
            self._write_to_slack(_unexpected_responses, self.MESSAGE_UNEXPECTED)

        self._logger.info('Finish SNOW Ticket Retrieval')


class DBHelper:
    """
    DB helper class specific to the Kelvin databases
    """

    def __init__(self, _env_settings, _db_name, _db_user, _db_pass):
        """
        :param _env_settings: dict from ini settings file
        :param _db_name: name of the database
        :param _db_user: user name
        :param _db_pass: password
        """
        self._logger = logging.getLogger(__name__)
        _client = MongoClient(_env_settings.get('db_url'))
        _client[_env_settings.get(_db_name)].authenticate(_env_settings.get(_db_user),
                                                          _env_settings.get(_db_pass),
                                                          mechanism=_env_settings.get('db_auth_mechanism'))
        _db = _client[_env_settings.get(_db_name)]
        self._collection = _db.incidents
        self._client = _client

    @property
    def collection(self):
        return self._collection

    def close_connection(self):
        self._client.close()

    def get_closed_tickets(self, _yesterday_date_time):
        """
        :param _yesterday_date_time: UTC datetime object (24 hours ago)
        :return: list of tickets closed in mongoDB in last 24 hours
        """
        _tickets_closed_in_mongodb_since_yesterday = []
        _mongo_result = self._collection.find({'closedAt': {'$gte': _yesterday_date_time}})

        for _result in _mongo_result:
            _tickets_closed_in_mongodb_since_yesterday.append(_result.get('ticketID'))

        return _tickets_closed_in_mongodb_since_yesterday


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    try:
        _path = './logging.yaml'
        value = os.getenv('LOG_CFG', None)
        if value:
            _path = value
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

    _yesterday_date_time = (datetime.utcnow() - timedelta(hours=24)).replace(microsecond=0)

    PROCESS_NAME = 'Reconciling KelvinDB with SNOW'
    _logger = setup_logging()
    _logger.info('Started {}'.format(PROCESS_NAME))

    _config_p = ConfigParser()
    _dir_path = os.path.dirname(os.path.realpath(__file__))
    _config_p.read('{}/settings.ini'.format(_dir_path))
    _settings = dict(_config_p.items(os.getenv('sysenv', 'dev')))

    _db_kelvin_client = None

    try:
        # MONGODB

        # Create handle to the DB
        _db_kelvin_client = DBHelper(_settings, 'db_k', 'db_user_k', 'db_pass_k')

        _tickets_closed_in_mongodb_since_yesterday = _db_kelvin_client.get_closed_tickets(_yesterday_date_time)

        # SNOW

        # Create handle to SNOW
        _snow_client = SNOWHelper(_settings)

        # Reconcile SNOW Tickets against MongoDB
        _snow_client.reconcile_snow_tickets_against_mongodb(_tickets_closed_in_mongodb_since_yesterday)

    except Exception as e:
        _logger.error(e)
    finally:
        if _db_kelvin_client:
            _db_kelvin_client.close_connection()
            _logger.info('Finished {}\n'.format(PROCESS_NAME))
