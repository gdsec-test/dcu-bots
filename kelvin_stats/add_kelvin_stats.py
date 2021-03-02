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
        self._conn = pymongo.MongoClient('mongodb://{}:{}@10.22.9.209/dcu_kelvin'.format(_db_user, _db_pass))
        self._db = self._conn['dcu_kelvin']
        self._collection = self._db['incidents']

    def handle(self):
        """
        :return: Handle to mongo collection
        """
        return self._collection


class Publisher:
    EXCHANGE = 'kelvin-stats'
    TYPE = 'direct'
    ROUTING_KEY = 'kelvin-stats'

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


def time_format(_dt, _logger):
    """
    Function takes in a datetime object and returns a YYYY-mm-ddTHH:MM:SS.fffZ formatted string
    :param _dt: datetime object
    :param _logger: handle to logger
    :return: string
    """
    if type(_dt) not in [datetime]:
        _logger.error('Received unexpected type: {}'.format(type(_dt)))
        return _dt
    return '%s:%.3f%sZ' % (_dt.strftime('%Y-%m-%dT%H:%M'),
                           float('%.3f' % (_dt.second + _dt.microsecond / 1e6)),
                           _dt.strftime('%z'))


def merge_dicts(_dict1, _dict2):
    """
    :param _dict1: First dictionary
    :param _dict2: Second dictionary
    """
    _merged_dict = _dict1.copy()
    _merged_dict.update(_dict2)
    return _merged_dict


def clean_pop(_dict, _list_of_keys_to_pop):
    """
    Function to pop all keys from a dict
    :param _dict: dictionary to pop keys from
    :param _list_of_keys_to_pop: list of string key names
    :return: None
    """
    for _key in _list_of_keys_to_pop:
        _dict.pop(_key, None)


def assign_keys(_dict_to_assign_to, _dict_keys_exist_in, _dict_of_keys_to_assign):
    """
    Given a list of keys which may or may not exist in a dict, assign all the key/value
    pairs that do exist into another dict
    :param _dict_to_assign_to: dictionary to add new key/value pairs to
    :param _dict_keys_exist_in: dictionary possibly containing list of keys
    :param _dict_of_keys_to_assign: dict of string key names {key_to_lookup_in_dict_keys_exist_in: key_to_assign}
    """
    for _lookup_key, _assign_key in _dict_of_keys_to_assign.items():
        _val = _dict_keys_exist_in.get(_lookup_key)
        if _val:
            if 'date' in _assign_key:
                if 'T' not in _val:
                    _val = '{}T00:00:00Z'.format(_val)
            _dict_to_assign_to[_assign_key] = _val


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

    _logger.info('Starting Kelvin stats retrieval')
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
            {
                '$or': [
                    {
                        'lastModified': {
                            '$gte': datetime.utcnow() - timedelta(hours=10)
                        }
                    },
                    {
                        'closedAt': {
                            '$gte': datetime.utcnow() - timedelta(hours=10)
                        }
                    }
                ]
            }
    ):

        clean_pop(_data, ['_id', 'archiveCompleted', 'fileInfo', 'info', 'lastModified', 'messageID', 'ncmecReportID',
                          'notified', 'reporterEmail', 'reportFileID', 'source', 'sourceDomainOrIP', 'target',
                          'ticketCategory', 'userGenHoldReason', 'userGenHoldUntil'])

        _domain_brand_data = _data.pop('domain', None)
        if _domain_brand_data:
            assign_keys(
                _data,
                _domain_brand_data,
                {
                    'brand': 'domain_brand',
                    'registrarName': 'domain_registrar',
                    'domainCreateDate': 'domain_create_date'
                }
            )

        _hosting_brand_data = _data.pop('hosting', None)
        if _hosting_brand_data:
            assign_keys(
                _data,
                _hosting_brand_data,
                {
                    'brand': 'hosting_brand',
                    'hostingCompanyName': 'hosting_company_name',
                    'IP': 'hosting_ip'
                }
            )

        _cmap_data = _data.pop('data', None)
        if _cmap_data:
            _host_data = _cmap_data.get('domainQuery', {}).get('host')

            if _host_data:
                assign_keys(
                    _data,
                    _host_data,
                    {
                        'product': 'host_product',
                        'dataCenter': 'host_data_center',
                        'hostname': 'hostname',
                        'mwpId': 'mwp_id',
                        'guid': 'host_guid',
                        'os': 'host_os'
                    }
                )

            _shopper_data = _cmap_data.get('domainQuery', {}).get('shopperInfo')
            if _shopper_data:
                assign_keys(
                    _data,
                    _shopper_data,
                    {
                        'shopperCity': 'shopper_city',
                        'shopperState': 'shopper_state',
                        'shopperCountry': 'shopper_country'
                    }
                )

        _meta = _data.pop('metadata', None)
        if _meta:
            _meta.pop('iris_id', None)
            _data = merge_dicts(_data, _meta)

        for _time_types in ['createdAt', 'closedAt', 'iris_created']:
            _time_type = _data.get(_time_types)
            if _time_type:
                _data[_time_types] = time_format(_time_type, _logger)

        _rabbit.publish(_data)

    _logger.info('Finished Kelvin stats retrieval')
