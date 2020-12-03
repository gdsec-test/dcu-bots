import logging
import pymongo
import json
import os
import time
import yaml

from datetime import datetime, timedelta
from logging.config import dictConfig
from pika import BlockingConnection, connection, credentials, exceptions


def time_format(dt):
    """
    Converts a timestamp to a comparable string
    :param dt: datetime timestamp
    :returns: string representation of the provided timestamp
    """
    return '%s:%.3f%sZ' % (dt.strftime('%Y-%m-%dT%H:%M'),
                           float('%.3f' % (dt.second + dt.microsecond / 1e6)),
                           dt.strftime('%z'))


class MongoHelperAPI:
    """
    Class to facilitate db client functionality
    """
    DB_IP = '10.22.9.209'
    DB_NAME = 'phishstory'
    COLLECTION_NAME = 'incidents'

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        _db_user = os.getenv('DB_USER')
        _db_pass = os.getenv('DB_PASS')
        if not _db_user or not _db_pass:
            raise Exception('DB Credentials not provided')

        self._conn = pymongo.MongoClient('mongodb://{}:{}@{}/{}'.format(_db_user,
                                                                        _db_pass,
                                                                        self.DB_IP,
                                                                        self.DB_NAME))
        self._db = self._conn[self.DB_NAME]
        self._collection = self._db[self.COLLECTION_NAME]

    def handle(self):
        return self._collection


class Publisher:
    """
    Class to facilitate RabbitMQ functionality
    """
    EXCHANGE = 'user-logs'
    TYPE = 'direct'
    ROUTING_KEY = 'user-logs'

    def __init__(self, host, port, virtual_host, username, password):
        """
        Constructor
        :param host: string rmq host
        :param port: integer rmq port
        :param virtual_host: string rmq vhost
        :param username: string rmq broker user name
        :param password: string rmq broker password
        :return: None
        """
        self._params = connection.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials.PlainCredentials(username, password),
            ssl=True)
        self._conn = None
        self._channel = None
        self._logger = logging.getLogger(__name__)

    def connect(self):
        """
        Attempt to connect to rmq if not currently connected
        :return: None
        """
        if not self._conn or self._conn.is_closed:
            self._conn = BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.exchange_declare(
                exchange=self.EXCHANGE, exchange_type=self.TYPE, durable=True)

    def _publish(self, msg):
        """
        Private method to publish a message to rmq
        :param msg: dictionary message
        :return: None
        """
        self._channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            body=json.dumps(msg).encode())
        self._logger.debug('message sent: %s', msg)

    def publish(self, msg):
        """
        Public method to publish a message to rmq, reconnecting if necessary.
        :param msg: dictionary message
        :return: None
        """

        try:
            self._publish(msg)
        except exceptions.ConnectionClosed:
            self._logger.debug('reconnecting to queue')
            self.connect()
            self._publish(msg)

    def close(self):
        """
        Close the connection to rmq if there is an open connection
        :return: None
        """
        if self._conn and self._conn.is_open:
            self._logger.debug('closing queue connection')
            self._conn.close()


if __name__ == '__main__':

    KEY_ACTIONS = 'actions'
    KEY_FILENAME = 'fileName'
    KEY_ID = '_id'
    KEY_LAST_MODIFIED = 'last_modified'
    KEY_MESSAGE = 'message'
    KEY_ORIGIN = 'origin'
    KEY_TIMESTAMP = 'timestamp'
    KEY_USER = 'user'
    NUM_OF_ATTEMPTS = 6
    PAUSE_BETWEEN_ATTEMPTS = 15
    PROGRAM_NAME = 'user actions retrieval'
    # We will not be publishing any action entries for sent emails (except ones to emea)
    VALID_MESSAGES = ['closed as', 'warning_hold', 'email_sent_to_emea']
    path = 'logging.yaml'

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

    # DB Specific vars
    db_query = {
        KEY_ACTIONS: {'$exists': True},
        KEY_LAST_MODIFIED: {'$gte': datetime.utcnow() - timedelta(hours=1)}
    }
    db_fields_to_return = {KEY_ACTIONS: 1, KEY_ID: 0}

    logger.info('Starting {}'.format(PROGRAM_NAME))

    rabbit = Publisher(
        host='rmq-dcu.int.godaddy.com',
        port=5672,
        virtual_host='grandma',
        username=os.getenv('BROKER_USER') or 'user',
        password=os.getenv('BROKER_PASS') or 'password')

    # Since we see connection exceptions every couple of weeks, let's try up to 5 attempts
    #  with a 15 second pause in between attempts
    for attempt in range(1, NUM_OF_ATTEMPTS):
        try:
            rabbit.connect()
            break
        except Exception as e:
            logger.error('Error connecting to RMQ: attempt {}: {}'.format(attempt, e))
            time.sleep(PAUSE_BETWEEN_ATTEMPTS)

    mongo = MongoHelperAPI()
    for item in mongo.handle().find(filter=db_query, projection=db_fields_to_return):
        actions_array = item.get(KEY_ACTIONS)
        for action in actions_array:
            # Only publish action messages that contain any list elements from VALID_MESSAGES
            if any(msg in action.get(KEY_MESSAGE) for msg in VALID_MESSAGES):
                # Because logstash salt configs refer to "fileName" for a unique fingerprint, and
                #  we no longer have a "fileName" field, pass the "origin" field as "fileName"
                # Also remove the "closed as " from the message, as we don't need to see that in kibana
                payload = {
                    KEY_USER: action.get(KEY_USER),
                    KEY_MESSAGE: action.get(KEY_MESSAGE).replace('closed as ', ''),
                    KEY_TIMESTAMP: time_format(action.get(KEY_TIMESTAMP)),
                    KEY_FILENAME: action.get(KEY_ORIGIN)
                }
                rabbit.publish(payload)

    rabbit.close()

    logger.info('Finished {}'.format(PROGRAM_NAME))
