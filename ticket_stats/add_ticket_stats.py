import logging
import pymongo
import pika
import json
import os
import yaml
from logging.config import dictConfig
from datetime import datetime, timedelta

KEYS2KEEP = ['blacklist', 'brand', 'close_reason', 'closed', 'created', 'data', 'evidence',
             'hosted_status', 'iris_created', 'metadata', 'opened', 'phishstory_status', 'proxy',
             'reporter', 'sourceDomainOrIp', 'target', 'ticketId', 'type']


def time_format(dt):
    """
    Function takes in a datetime object and returns a YYYY-mm-ddTHH:MM:SS.fffZ formatted string
    :param dt: datetime object
    :return: string
    """
    if type(dt) not in [datetime]:
        logger.error('Received unexpected type: {}'.format(type(dt)))
        return dt
    return '%s:%.3f%sZ' % (dt.strftime('%Y-%m-%dT%H:%M'),
                           float('%.3f' % (dt.second + dt.microsecond / 1e6)),
                           dt.strftime('%z'))


def pop_dict_values(my_dict):
    """
    POP all fields that are NOT listed in KEYS2KEEP, from the dictionary provided,
    so they are NOT published to Rabbit
    :param my_dict: dictionary of the ticket's DB key:value pairs
    :return: None
    """
    for key in my_dict.keys():
        if key not in KEYS2KEEP:
            my_dict.pop(key, None)


class MongoHelperAPI:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        _dbuser = os.getenv('DB_USER') or 'user'
        _dbpass = os.getenv('DB_PASS') or 'password'
        self._conn = pymongo.MongoClient('mongodb://{}:{}@10.22.9.209/phishstory'.format(_dbuser, _dbpass))
        self._db = self._conn['phishstory']
        self._collection = self._db['incidents']

    def handle(self):
        return self._collection


class Publisher:
    EXCHANGE = 'ticket-stats'
    TYPE = 'direct'
    ROUTING_KEY = 'ticket-stats'

    def __init__(self, host, port, virtual_host, username, password):
        self._params = pika.connection.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=pika.credentials.PlainCredentials(username, password),
            ssl=True)
        self._conn = None
        self._channel = None
        self._logger = logging.getLogger(__name__)

    def connect(self):
        if not self._conn or self._conn.is_closed:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.exchange_declare(
                exchange=self.EXCHANGE, exchange_type=self.TYPE, durable=True)

    def _publish(self, msg):
        try:
            self._channel.basic_publish(
                exchange=self.EXCHANGE,
                routing_key=self.ROUTING_KEY,
                body=json.dumps(msg).encode())
            self._logger.debug('message sent: %s', msg)
        except TypeError as e:
            self._logger.error('Failed to publish ticket {}: {}'.format(msg.get('ticketId'), e.message))

    def publish(self, msg):
        """
        Publish msg, reconnecting if necessary.
        :param msg: dictionary of payload to publish
        :return: None
        """
        try:
            self._publish(msg)
        except pika.exceptions.ConnectionClosed:
            self._logger.debug('reconnecting to queue')
            self.connect()
            self._publish(msg)

    def close(self):
        if self._conn and self._conn.is_open:
            self._logger.debug('closing queue connection')
            self._conn.close()


def merge_dicts(a, b):
    z = a.copy()
    z.update(b)
    return z


if __name__ == '__main__':

    path = ''
    value = os.getenv('LOG_CFG', None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            lconfig = yaml.safe_load(f.read())
        dictConfig(lconfig)
    else:
        logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info('Starting ticket data retrieval')
    mongo = MongoHelperAPI()
    rabbit = Publisher(
        host='rmq-dcu.int.godaddy.com',
        port=5672,
        virtual_host='grandma',
        username=os.getenv('BROKER_USER') or 'user',
        password=os.getenv('BROKER_PASS') or 'password')
    rabbit.connect()
    for data in mongo.handle().find({'$or': [{'last_modified': {'$gte': datetime.utcnow() - timedelta(hours=1)}},
                                             {'closed': {'$gte': datetime.utcnow() - timedelta(hours=1)}}]}):
        pop_dict_values(data)
        extra = data.pop('data', None)
        if extra:
            host_data = extra.get('domainQuery', {}).get('host')
            if host_data:
                brand = host_data.get('brand')
                product = host_data.get('product')
                if brand:
                    data['brand'] = brand
                if product:
                    data['product'] = product
            data['security_subscriptions'] = extra.get('domainQuery', {})\
                .get('securitySubscription', {})\
                .get('sucuriProduct', [])

        meta = data.pop('metadata', None)
        if meta:
            merge_dicts(data, meta)
        for time in ['created', 'closed', 'iris_created', 'opened']:
            tdata = data.get(time)
            if tdata:
                data[time] = time_format(tdata)
            else:
                if time == 'created':
                    opent = data.get('closed')
                    data[time] = time_format(opent)
                else:
                    # If the timestamp field doesnt have a value, just pop it
                    data.pop(time, None)
        rabbit.publish(data)
    logger.info('Finished ticket stats retrieval')
