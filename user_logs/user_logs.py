import logging
import pymongo
import pika
import json
import os
import yaml
from logging.config import dictConfig
from datetime import datetime, timedelta


def time_format(dt):
    return '%s:%.3f%s' % (dt.strftime('%Y-%m-%dT%H:%M'),
                          float('%.3f' % (dt.second + dt.microsecond / 1e6)),
                          dt.strftime('%z'))


class MongoHelperAPI:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        _dbuser = os.getenv('DB_USER') or 'user'
        _dbpass = os.getenv('DB_PASS') or 'password'
        self._conn = pymongo.MongoClient('mongodb://{}:{}@10.22.9.209/phishstory'.format(_dbuser, _dbpass))
        self._db = self._conn['phishstory']
        self._collection = self._db['logs']

    def handle(self):
        return self._collection


class Publisher:
    EXCHANGE = 'user-logs'
    TYPE = 'direct'
    ROUTING_KEY = 'user-logs'

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
        self._channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            body=json.dumps(msg).encode())
        self._logger.debug('message sent: %s', msg)

    def publish(self, msg):
        """Publish msg, reconnecting if necessary."""

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


if __name__ == '__main__':

    path = 'user_logging.yml'
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

    logger.info('Starting user log retrieval')
    mongo = MongoHelperAPI()
    rabbit = Publisher(
        host='rmq-dcu.int.godaddy.com',
        port=5672,
        virtual_host='grandma',
        username=os.getenv('BROKER_USER') or 'user',
        password=os.getenv('BROKER_PASS') or 'password')
    rabbit.connect()
    for item in mongo.handle().find({'timestamp': {'$gte': datetime.utcnow() - timedelta(hours=1)}}):
        data = item
        data.pop('_id')
        time = data.get('timestamp')
        data['timestamp'] = time_format(time) + 'Z'
        rabbit.publish(data)
    logger.info('Finished user log retrieval')

