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

KEYS2KEEP = ['blacklist', 'brand', 'close_reason', 'closed', 'created', 'data', 'evidence',
             'hosted_status', 'iris_created', 'metadata', 'opened', 'phishstory_status', 'proxy',
             'reporter', 'sourceDomainOrIp', 'target', 'ticketId', 'type']


class MongoHelperAPI:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        _db_user = os.getenv('DB_USER', 'user')
        _db_pass = os.getenv('DB_PASS', 'password')
        self._conn = pymongo.MongoClient('mongodb://{}:{}@10.22.9.209/phishstory'.format(_db_user, _db_pass))
        self._db = self._conn['phishstory']
        self._collection = self._db['incidents']

    def handle(self):
        """
        :return: Handle to mongo collection
        """
        return self._collection


class Publisher:
    EXCHANGE = 'ticket-stats'
    TYPE = 'direct'
    ROUTING_KEY = 'ticket-stats'

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
            self._channel.exchange_declare(
                exchange=self.EXCHANGE, exchange_type=self.TYPE, durable=True)

    def _publish(self, _msg):
        """
        :param _msg:
        :return: None
        """
        try:
            self._channel.basic_publish(
                exchange=self.EXCHANGE,
                routing_key=self.ROUTING_KEY,
                body=json.dumps(_msg).encode())
            self._logger.debug('message sent: %s', _msg)
        except TypeError as e:
            self._logger.error('Failed to publish ticket {}: {}'.format(_msg.get('ticketId'), e))

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
    Function takes in a datetime object and returns a YYYY-mm-ddTHH:MM:SS.fffZ formatted string
    :param _dt: datetime object
    :return: string
    """
    if type(_dt) not in [datetime]:
        _logger.error('Received unexpected type: {}'.format(type(_dt)))
        return _dt
    return '%s:%.3f%sZ' % (_dt.strftime('%Y-%m-%dT%H:%M'),
                           float('%.3f' % (_dt.second + _dt.microsecond / 1e6)),
                           _dt.strftime('%z'))


def pop_dict_values(_dict_to_pop_keys_from):
    """
    POP all fields that are NOT listed in KEYS2KEEP, from the dictionary provided,
    so they are NOT published to Rabbit
    :param _dict_to_pop_keys_from: dictionary of the ticket's DB key:value pairs
    :return: None
    """
    for _key in list(_dict_to_pop_keys_from):
        if _key not in KEYS2KEEP:
            _dict_to_pop_keys_from.pop(_key, None)


def merge_dicts(_dict1, _dict2):
    """
    :param _dict1: First dictionary
    :param _dict2: Second dictionary
    """
    _merged_dict = _dict1.copy()
    _merged_dict.update(_dict2)
    return _merged_dict


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

    _logger.info('Starting ticket data retrieval')
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

    for _data in _mongo.handle().find(
            {'$or': [
                {'last_modified': {'$gte': datetime.utcnow() - timedelta(hours=1)}},
                {'closed': {'$gte': datetime.utcnow() - timedelta(hours=1)}}
            ]}
    ):
        pop_dict_values(_data)
        _extra = _data.pop('data', None)
        if _extra:
            _host_data = _extra.get('domainQuery', {}).get('host')
            if _host_data:
                _brand = _host_data.get('brand')
                _product = _host_data.get('product')
                if _brand:
                    _data['brand'] = _brand
                if _product:
                    _data['product'] = _product
            _data['security_subscriptions'] = _extra.get('domainQuery', {})\
                .get('securitySubscription', {})\
                .get('sucuriProduct', [])

        _meta = _data.pop('metadata', None)
        if _meta:
            merge_dicts(_data, _meta)
        for _time_type in ['created', 'closed', 'iris_created', 'opened']:
            _time_val = _data.get(_time_type)
            if _time_val:
                _data[_time_type] = time_format(_time_val)
            else:
                if _time_type == 'created':
                    _closed_val = _data.get('closed')
                    _data[_time_type] = time_format(_closed_val)
                else:
                    # If the timestamp field doesnt have a value, just pop it
                    _data.pop(_time_type, None)

        _rabbit.publish(_data)

    _logger.info('Finished ticket stats retrieval')
