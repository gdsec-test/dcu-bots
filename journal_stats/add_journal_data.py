import json
import logging
import os
import time
from datetime import datetime, timedelta
from logging.config import dictConfig
from ssl import create_default_context

import pymongo
import yaml
from pika import (BlockingConnection, SSLOptions, connection, credentials,
                  exceptions)


class MongoHelperAPI:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        _db_user = os.getenv('DB_USER', 'user')
        _db_pass = os.getenv('DB_PASS', 'password')
        self._conn = pymongo.MongoClient('mongodb://{}:{}@10.22.9.209/phishstory'.format(_db_user, _db_pass))
        self._db = self._conn['phishstory']
        self._collection = self._db['journal']

    def handle(self):
        """
        :return: Handle to mongo collection
        """
        return self._collection


class Publisher:
    EXCHANGE = 'journal-records'
    TYPE = 'direct'
    ROUTING_KEY = 'journal-records'

    def __init__(self, _host, _port, _virtual_host, _username, _password):
        """
        :param _host:
        :param _port:
        :param _virtual_host:
        :param _username:
        :param _password:
        :return: None
        """
        context = create_default_context()
        self._params = connection.ConnectionParameters(
            host=_host,
            port=_port,
            virtual_host=_virtual_host,
            credentials=credentials.PlainCredentials(_username, _password),
            ssl_options=SSLOptions(context, _host))
        self._conn = None
        self._channel = None
        self._logger = logging.getLogger(__name__)

    def connect(self):
        """
        :return: None
        """
        if not self._conn or self._conn.is_closed:
            self._conn = BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.queue_declare(queue=self.EXCHANGE, durable=True)
            self._channel.exchange_declare(
                exchange=self.EXCHANGE, exchange_type=self.TYPE, durable=True)

    def _publish(self, _msg):
        """
        :param _msg:
        :return: None
        """
        _msg['_id'] = str(_msg['_id'])
        self._channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            body=json.dumps(_msg).encode())
        self._logger.debug('message sent: %s', _msg)

    def publish(self, _msg):
        """
        Publish msg, reconnecting if necessary.
        :param _msg:
        :return: None
        """
        try:
            self._publish(_msg)
        except exceptions.ConnectionClosed:
            self._logger.debug('reconnecting to queue')
            self.connect()
            self._publish(_msg)

    def close(self):
        """
        :return: None
        """
        if self._conn and self._conn.is_open:
            self._logger.debug('closing queue connection')
            self._conn.close()


def time_format(_dt):
    """
    If function takes in a datetime object a YYYY-mm-ddTHH:MM:SS.fffZ formatted string is returned
    If function takes in a string or unicode a YYYY-mm-ddTHH:MM:SS.fff formatted string is returned
    :param _dt: datetime object or string or unicode
    :return: string
    """
    if type(_dt) is str:
        return _dt[:-3]
    return '%s:%.3f%sZ' % (_dt.strftime('%Y-%m-%dT%H:%M'),
                           float('%.3f' % (_dt.second + _dt.microsecond / 1e6)),
                           _dt.strftime('%z'))


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    try:
        _path = 'logging.yaml'
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


if __name__ == '__main__':
    NUM_OF_ATTEMPTS = 6
    PAUSE_BETWEEN_ATTEMPTS = 15

    _logger = setup_logging()

    _logger.info('Starting journal record retrieval')
    _mongo = MongoHelperAPI()
    _rabbit = Publisher(
        _host='rmq-dcu.int.godaddy.com',
        _port=5672,
        _virtual_host='grandma',
        _username=os.getenv('BROKER_USER', 'user'),
        _password=os.getenv('BROKER_PASS', 'password')
    )

    # Since we see connection exceptions every couple of weeks, let's try up to 5 attempts
    #  with a 15 second pause in between attempts
    for _attempt in range(1, NUM_OF_ATTEMPTS):
        try:
            _rabbit.connect()
            break
        except Exception as e:
            _logger.error('Error connecting to RMQ: attempt {}: {}'.format(_attempt, e))
            time.sleep(PAUSE_BETWEEN_ATTEMPTS)

    for _item in _mongo.handle().find({'createdAt': {'$gte': datetime.utcnow() - timedelta(minutes=15)}}):
        _data = _item
        _data.pop('notes', None)
        _data.pop('assets', None)
        _created_date = _data.get('createdAt')
        if _created_date:
            _data['createdAt'] = time_format(_created_date)
        _rabbit.publish(_data)
    _logger.info('Finished journal records retrieval')
