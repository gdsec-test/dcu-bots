import json
import logging
import os
import requests
import yaml

from ConfigParser import SafeConfigParser
from logging.config import dictConfig
from pymongo import MongoClient


class DBHelper:
    """
    DB helper class specific to the Kelvin databases
    """

    def __init__(self, db_collection):
        """
        :param db_collection: db collection
        """
        self._logger = logging.getLogger(__name__)
        client = MongoClient(settings.get('db_k'))
        _db = client[settings.get('db')]
        _db.authenticate(settings.get('db_user_k'),
                         settings.get('db_pass_k'),
                         mechanism=settings.get('db_auth_mechanism'))
        self._collection = _db[db_collection]
        self._client = client

    @property
    def collection(self):
        return self._collection

    def close_connection(self):
        self._client.close()


def delete_closed_cases(db_client_incidents, db_client_files, db_client_chunks):
    """
    Deletes the documents from fs.files and fs.chunks if the respective case is closed in incidents collection
    :param db_client_incidents: db handle for fs.incidents collection
    :param db_client_files: db handle for fs.files collection
    :param db_client_chunks: db handle for fs.chunks collection
    :return: none
    """
    files_documents = db_client_files.collection.find(filter={}, projection={'filename': 1})
    closed_tickets = []
    closed_files_id = []
    for files_document in files_documents:
        filename = files_document.get('filename').encode('utf8')
        file_id = files_document.get('_id')
        incident_collection_documents = db_client_incidents.collection.find({'ticketID': filename},
                                                                            projection={'ticketID': 1,
                                                                                        'kelvinStatus': 1})
        for incident in incident_collection_documents:
            if incident.get('kelvinStatus').encode('utf8') == 'CLOSED':
                closed_tickets.append(filename)
                closed_files_id.append(file_id)

    for file_id in closed_files_id:
        try:
            db_client_chunks.collection.delete_many({'files_id': file_id})
        except Exception as exp:
            message = '<!here> Failed to delete document from fs.chunks collection for files_id {}'.format(file_id)
            write_to_slack(settings.get('slack_url'), settings.get('slack_channel'), message)
            logger.error('Exception in delete_closed_cases function {}: '.format(exp))

    for filename in closed_tickets:
        try:
            db_client_files.collection.delete_many({'filename': filename})
        except Exception as exp:
            message = '<!here> Failed to delete document from fs.files collection for filename ' \
                      '{}'.format(filename)
            write_to_slack(settings.get('slack_url'), settings.get('slack_channel'), message)
            logger.error('Exception in delete_closed_cases function {}: '.format(exp))


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    try:
        path = './logging.yaml'
        if path and os.path.exists(path):
            with open(path, 'rt') as f:
                lconfig = yaml.safe_load(f.read())
            dictConfig(lconfig)
        else:
            logging.basicConfig(level=logging.INFO)
    except Exception:
        logging.basicConfig(level=logging.INFO)
    finally:
        return logging.getLogger(__name__)


def write_to_slack(endpoint, channel, message):
    """
        Writes message to a slack channel
        :param endpoint: The slack URL
        :param channel: The slack channel
        :param message: message body
        :return: none
    """
    payload = {'payload': json.dumps({
        'channel': channel,
        'username': 'GRIDFS DOCUMENT DELETE BOT',
        'text': message,
        'icon_emoji': ':-1:'
    })
    }
    requests.post(endpoint, data=payload)


if __name__ == '__main__':

    PROCESS_NAME = 'Deleting documents from gridfs for closed cases'
    logger = setup_logging()
    logger.info('Started {}'.format(PROCESS_NAME))

    configp = SafeConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    configp.read('{}/settings.ini'.format(dir_path))
    settings = dict(configp.items(os.getenv('sysenv', 'dev')))

    db_client_files = None
    db_client_chunks = None
    db_client_incidents = None

    try:
        # MONGODB
        # Create collection specific handle to the DB
        db_client_files = DBHelper('fs.files')
        db_client_chunks = DBHelper('fs.chunks')
        db_client_incidents = DBHelper('incidents')

        delete_closed_cases(db_client_incidents, db_client_files, db_client_chunks)

    except Exception as e:
        logger.error(e.message)
    finally:
        if db_client_files:
            db_client_files.close_connection()
        if db_client_chunks:
            db_client_chunks.close_connection()
        if db_client_incidents:
            db_client_incidents.close_connection()
        logger.info('Finished {}'.format(PROCESS_NAME))
