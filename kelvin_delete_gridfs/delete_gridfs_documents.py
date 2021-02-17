import json
import logging
import os
from configparser import ConfigParser
from logging.config import dictConfig

import requests
import yaml
from pymongo import MongoClient

CLOSED = 'CLOSED'
FAILED_CHUNKS_MESSAGE = '<!here> Failed to delete document from fs.chunks collection for files_id {}'
FAILED_FILES_MESSAGE = '<!here> Failed to delete document from fs.files collection for filename {}'
EXCEPTION_DELETE_CLOSED_CASES_MESSAGE = 'Exception in delete_closed_cases function: {}'
KEY_FILENAME = 'filename'
KEY_FILES_ID = 'files_id'
KEY_ID = '_id'
KEY_KELVIN_STATUS = 'kelvinStatus'
KEY_SLACK_CHANNEL = 'slack_channel'
KEY_SLACK_URL = 'slack_url'
KEY_TICKET_ID = 'ticketID'


class DBHelper:
    """
    DB helper class specific to the Kelvin databases
    """

    def __init__(self, _db_collection):
        """
        :param _db_collection: db collection
        """
        self._logger = logging.getLogger(__name__)
        _client = MongoClient(_settings.get('db_k'))
        _db = _client[_settings.get('db')]
        _db.authenticate(_settings.get('db_user_k'),
                         _settings.get('db_pass_k'),
                         mechanism=_settings.get('db_auth_mechanism'))
        self._collection = _db[_db_collection]
        self._client = _client

    @property
    def collection(self):
        return self._collection

    def close_connection(self):
        self._client.close()


def delete_closed_cases(_db_client_incidents, _db_client_files, _db_client_chunks):
    """
    Deletes the documents from fs.files and fs.chunks if the respective case is closed in incidents collection
    :param _db_client_incidents: db handle for fs.incidents collection
    :param _db_client_files: db handle for fs.files collection
    :param _db_client_chunks: db handle for fs.chunks collection
    :return: None
    """
    # Assign the collections just once
    _db_chunks_collection = _db_client_chunks.collection
    _db_files_collection = _db_client_files.collection
    _db_incident_collection = _db_client_incidents.collection

    _files_documents = _db_files_collection.find(filter={}, projection={KEY_FILENAME: 1})
    _closed_tickets = []
    _closed_files_id = []

    for _files_document in _files_documents:
        _filename = _files_document.get(KEY_FILENAME).encode().decode()
        _file_id = _files_document.get(KEY_ID)
        _incident_collection_documents = _db_incident_collection.find({KEY_TICKET_ID: _filename},
                                                                      projection={KEY_TICKET_ID: 1,
                                                                                  KEY_KELVIN_STATUS: 1})
        for _incident in _incident_collection_documents:
            if _incident.get(KEY_KELVIN_STATUS).encode().decode() == CLOSED:
                _closed_tickets.append(_filename)
                _closed_files_id.append(_file_id)

    for _file_id in _closed_files_id:
        try:
            _db_chunks_collection.delete_many({KEY_FILES_ID: _file_id})
        except Exception as _e:
            _message = FAILED_CHUNKS_MESSAGE.format(_file_id)
            write_to_slack(_settings.get(KEY_SLACK_URL), _settings.get(KEY_SLACK_CHANNEL), _message)
            _logger.error(EXCEPTION_DELETE_CLOSED_CASES_MESSAGE.format(_e))

    for _filename in _closed_tickets:
        try:
            _db_files_collection.delete_many({KEY_FILENAME: _filename})
        except Exception as _e:
            _message = FAILED_FILES_MESSAGE.format(_filename)
            write_to_slack(_settings.get(KEY_SLACK_URL), _settings.get(KEY_SLACK_CHANNEL), _message)
            _logger.error(EXCEPTION_DELETE_CLOSED_CASES_MESSAGE.format(_e))


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    _path = 'logging.yaml'
    try:
        _value = os.getenv('LOG_CFG', None)
        if _value:
            _path = _value
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


def write_to_slack(_endpoint, _channel, _message):
    """
        Writes message to a slack channel
        :param _endpoint: The slack URL
        :param _channel: The slack channel
        :param _message: message body
        :return: none
    """
    _payload = {
        'payload': json.dumps(
            {
                'channel': _channel,
                'username': 'GRIDFS DOCUMENT DELETE BOT',
                'text': _message,
                'icon_emoji': ':-1:'
            }
        )
    }
    requests.post(_endpoint, data=_payload)


if __name__ == '__main__':
    PROCESS_NAME = 'Deleting documents from gridfs for closed cases'
    _logger = setup_logging()
    _logger.info('Started {}'.format(PROCESS_NAME))

    _config_p = ConfigParser()
    _dir_path = os.path.dirname(os.path.realpath(__file__))
    _config_p.read('{}/settings.ini'.format(_dir_path))
    _settings = dict(_config_p.items(os.getenv('sysenv', 'dev')))

    _db_client_files = _db_client_chunks = _db_client_incidents = None

    try:
        # MONGODB
        # Create collection specific handle to the DB
        _db_client_files = DBHelper('fs.files')
        _db_client_chunks = DBHelper('fs.chunks')
        _db_client_incidents = DBHelper('incidents')

        delete_closed_cases(_db_client_incidents, _db_client_files, _db_client_chunks)

    except Exception as e:
        _logger.error(e)
    finally:
        if _db_client_files:
            _db_client_files.close_connection()
        if _db_client_chunks:
            _db_client_chunks.close_connection()
        if _db_client_incidents:
            _db_client_incidents.close_connection()
        _logger.info('Finished {}'.format(PROCESS_NAME))
