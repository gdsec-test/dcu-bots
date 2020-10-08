import json
import logging
import os
import requests
import yaml

from ConfigParser import SafeConfigParser
from datetime import datetime, timedelta
from logging.config import dictConfig
from pymongo import MongoClient


class SNOWHelper:
    """
    Get all tickets that were created in SNOW Kelvin
    """
    HEADERS = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    def __init__(self, env_settings):
        """
        :param env_settings: dict from ini settings file
        """
        self._logger = logging.getLogger(__name__)
        self._auth = (env_settings.get('snow_user'), env_settings.get('snow_pass'))
        self._url = env_settings.get('snow_url')

    def reconcile_snow_tickets_against_mongodb(self, tickets_closed_in_mongodb_since_yesterday):
        """
        Get all tickets that were created in SNOW Kelvin
        :param tickets_closed_in_mongodb_since_yesterday: List of closed mongoDB tickets in the past 24 hours
        """
        self._logger.info('Start SNOW Ticket Retrieval')
        data = []
        unexpectedresponses = []

        for t in tickets_closed_in_mongodb_since_yesterday:
            url = self._url.format(t)
            response = requests.get(url,
                                    auth=self._auth,
                                    headers=self.HEADERS)

            if response.status_code != 200:
                logger.info('URL: {} ; Status: {} \n'.format(url, str(response.status_code)))
                unexpectedresponses.append('URL: {} ; Status: {} \n'.format(url, str(response.status_code)))
            else:
                if len(response.json()[u'result']) == 0:
                    data.append(t)

        if len(data) != 0:
            message = '<!here> Corresponding SNOW ticket(s) do not exist for the following MONGODB ticket(s): \n'
            write_to_slack(settings.get('slack_url'),
                           settings.get('slack_channel'),
                           data, message)

        if len(unexpectedresponses) != 0:
            message = '<!here> Unexpected response received from SNOW for the following URL(s): \n'
            write_to_slack(settings.get('slack_url'),
                           settings.get('slack_channel'),
                           unexpectedresponses, message)

        self._logger.info('Finish SNOW Ticket Retrieval')


class DBHelper:
    """
    DB helper class specific to the Kelvin databases
    """

    def __init__(self, env_settings, db_name, db_user, db_pass):
        """
        :param env_settings: dict from ini settings file
        :param db_name: name of the database
        :param db_user: user name
        :param db_pass: password
        """
        self._logger = logging.getLogger(__name__)
        client = MongoClient(env_settings.get('db_url'))
        client[env_settings.get(db_name)].authenticate(env_settings.get(db_user),
                                                       env_settings.get(db_pass),
                                                       mechanism=env_settings.get('db_auth_mechanism'))
        _db = client[settings.get(db_name)]
        self._collection = _db.incidents
        self._client = client

    @property
    def collection(self):
        return self._collection

    def close_connection(self):
        self._client.close()

    def get_closed_tickets(self, yesterday_date_time):
        """
        :param yesterday_date_time: UTC datetime object (24 hours ago)
        :return: list of tickets closed in mongoDB in last 24 hours
        """
        tickets_closed_in_mongodb_since_yesterday = []
        mongo_result = self._collection.find({'closedAt': {'$gte': yesterday_date_time}})

        for x in mongo_result:
            tickets_closed_in_mongodb_since_yesterday.append(x.get('ticketID'))

        return tickets_closed_in_mongodb_since_yesterday


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


def write_to_slack(endpoint, channel, message_list, message):
    """
        Writes message to a slack channel
        :param endpoint: The slack URL
        :param channel: The slack channel
        :param message_list: The list of tickets
        :param message: message body
        :return: none
    """
    if len(message_list):
        payload = {'payload': json.dumps({
            'channel': channel,
            'username': 'API BOT',
            'text': message + '\n'.join(message_list)
        })
        }
        requests.post(endpoint, data=payload)


if __name__ == '__main__':

    yesterday_date_time = datetime.utcnow() - timedelta(hours=24)
    yesterday_date_time = yesterday_date_time.replace(microsecond=0)

    PROCESS_NAME = 'Reconciling KelvinDB with SNOW'
    logger = setup_logging()
    logger.info('Started {}'.format(PROCESS_NAME))

    configp = SafeConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    configp.read('{}/settings.ini'.format(dir_path))
    settings = dict(configp.items(os.getenv('sysenv', 'dev')))

    db_client = ''

    try:
        # MONGODB

        # Create handle to the DB
        db_client = DBHelper(settings, 'db_k', 'db_user_k', 'db_pass_k')

        tickets_closed_in_mongodb_since_yesterday = db_client.get_closed_tickets(yesterday_date_time)

        # SNOW

        # Create handle to SNOW
        snow_client = SNOWHelper(settings)

        # Reconcile SNOW Tickets against MongoDB
        snow_client.reconcile_snow_tickets_against_mongodb(tickets_closed_in_mongodb_since_yesterday)

    except Exception as e:
        logger.error(e.message)
    finally:
        if db_client:
            db_client.close_connection()
            logger.info('Finished {}\n'.format(PROCESS_NAME))
