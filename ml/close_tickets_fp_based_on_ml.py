import logging
import os
import requests
import socket
import yaml

from datetime import datetime
from logging.config import dictConfig
from pymongo import MongoClient

from ConfigParser import SafeConfigParser


class APIHelper(object):
    """
    This class handles access to the DCU Abuse API
    """
    PAYLOAD = {"close_reason": "false_positive", "closed": "true"}

    def __init__(self, env_settings):
        self._logger = logging.getLogger(__name__)
        self._url = env_settings.get('abuse_api')
        self._header = {'Authorization': env_settings.get('dcu_middleware_jwt')}

    def close_incident(self, ticket_id):
        """
        Closes out the provided ticket id as FP using the API PATCH endpoint
        :param ticket_id:
        :return: boolean
        """
        success = False
        try:
            r = requests.patch('{}/{}'.format(self._url, ticket_id), json=self.PAYLOAD, headers=self._header)
            if r.status_code == 204:
                success = True
            else:
                self._logger.warning("Unable to close ticket {} {}".format(ticket_id, r.content))
        except Exception as err:
            self._logger.error("Exception while closing ticket {} {}".format(ticket_id, err.message))
        return success


class DBHelper:
    """
    DB helper class specific to the PhishStory databases
    """
    def __init__(self, env_settings, api_handle):
        """
        :param env_settings: dict from ini settings file
        :param api_handle: handle to the APIHelper class
        :return: boolean
        """
        self._logger = logging.getLogger(__name__)
        client = MongoClient(env_settings.get('db_url'))
        client[env_settings.get('db')].authenticate(env_settings.get('db_user'),
                                                    env_settings.get('db_pass'),
                                                    mechanism=env_settings.get('db_auth_mechanism'))
        self._db = client[settings.get('db')]
        self._api_handle = api_handle
        self._collection = self._db.incidents

    def close_connection(self):
        """
        Closes the connection to the db
        :return: None
        """
        self._db.close()

    def _update_actions_subdocument(self, ticket_id):
        """
        Update the database record with a new actions subdocument and entry
        :param ticket_id: string ticket id for the database record to modify
        :return: boolean
        """
        success = False
        # *** HARDCODED the method which performs the ticket closure ***
        origin_string = '{}:{}:APIHelper:close_incident'.format(socket.gethostname(), __file__)
        if self._collection.update_one({'_id': ticket_id},
                                       {'$push': {'actions': {
                                           'origin': origin_string,
                                           'timestamp': datetime.now(),
                                           'message': 'closed as false positive',
                                           'user': 'automation'
                                       }}}, upsert=True):
            success = True
        return success

    def close_tickets_with_low_fraud_scores(self):
        """
        Find all open Phishing tickets with low fraud scores, between 0 and 0.05, and send them to the API for closure
        :return: None
        """
        logger.info("Start DB Ticket Closures")

        # Find all open phishing tickets with a low fraud score
        cursor = self._collection.find({'type': 'PHISHING',
                                        'phishstory_status': 'OPEN',
                                        '$and': [
                                            {'fraud_score': {'$gte': 0.0}},
                                            {'fraud_score': {'$lte': 0.05}}
                                        ]})

        for ticket in cursor:
            ticket_id = ticket.get('ticketId')
            self._logger.info('Closing {} via API'.format(ticket_id))
            if self._api_handle.close_incident(ticket_id):
                if not self._update_actions_subdocument(ticket_id):
                    self._logger.warn('Unable to add actions sub-document to {}'.format(ticket_id))

        self._logger.info("Finish DB Ticket Closures")


def read_config():
    """
    Reads the configuration ini file for the env specific settings
    :return: dict of configuration settings for the env
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_p = SafeConfigParser()
    config_p.read('{}/connection_settings.ini'.format(dir_path))
    return dict(config_p.items(os.getenv('sysenv', 'dev')))


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    try:
        path = './logging.yaml'
        if path and os.path.exists(path):
            with open(path, 'rt') as f:
                l_config = yaml.safe_load(f.read())
            dictConfig(l_config)
        else:
            logging.basicConfig(level=logging.INFO)
    except Exception:
        logging.basicConfig(level=logging.INFO)
    finally:
        return logging.getLogger(__name__)


if __name__ == '__main__':
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
        logger.fatal(e.message)
    finally:
        if db_client:
            db_client.close_connection()
        logger.info('Finished {}\n'.format(PROCESS_NAME))
