import json
import logging
import os
import time
from datetime import datetime, timedelta
from logging.config import dictConfig
from ssl import create_default_context

import yaml
from phishlabs_api import PhishlabsAPI
from pika import (BlockingConnection, SSLOptions, connection, credentials,
                  exceptions)


def time_format(_dt):
    """
    Function takes in a datetime object and returns a YYYY-mm-ddTHH:MM:SS.fffZ formatted string
    :param _dt: datetime object
    :return: string
    """
    if type(_dt) not in [datetime]:
        _logger.error('Received unexpected type: {}'.format(type(_dt)))
        return _dt
    return '%s%sZ' % (_dt.strftime('%Y-%m-%dT%H:%M:%S'), _dt.strftime('%z'))


class Publisher:
    EXCHANGE = 'phishlabs-stats'
    TYPE = 'direct'
    ROUTING_KEY = 'phishlabs-stats'

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


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    try:
        _path = 'logging.yaml'
        value = os.getenv('LOG_CFG', None)
        if value:
            _path = value
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

    _logger.info('Starting PhishLabs stats Retrieval')
    _rabbit = Publisher(
        _host='rmq-dcu.int.godaddy.com',
        _port=5672,
        _virtual_host='grandma',
        _username=os.getenv('BROKER_USER', 'user'),
        _password=os.getenv('BROKER_PASS', 'password'))

    # Since we see connection exceptions every couple of weeks, let's try up to 5 attempts
    #  with a 15 second pause in between attempts
    for _attempt in range(1, NUM_OF_ATTEMPTS):
        try:
            _rabbit.connect()
            break
        except Exception as _e:
            _logger.error('Error connecting to RMQ: attempt {}: {}'.format(_attempt, _e))
            time.sleep(PAUSE_BETWEEN_ATTEMPTS)

    _phishlabs_api = PhishlabsAPI()
    _response = _phishlabs_api.retrieve_tickets(time_format(datetime.utcnow() - timedelta(hours=1)), 'caseModify')
    if _response and _response.get('data'):
        for _case in _response.get('data'):
            _data = dict()
            _data['caseId'] = _case.get('caseId', None)
            _data['caseNumber'] = _case.get('caseNumber', None)
            _data['caseStatus'] = _case.get('caseStatus', None)
            _data['caseType'] = _case.get('caseType', None)
            _data['createdBy'] = _case.get('createdBy', {}).get('name', None)
            _data['dateCreated'] = _case.get('dateCreated', None)
            _dateClosed = _case.get('dateClosed', None)
            if _dateClosed != '0001-01-01T00:00:00Z':
                _data['dateClosed'] = _dateClosed
            _data['dateModified'] = _case.get('dateModified', None)
            _data['description'] = _case.get('description', None)
            _data['notes'] = _case.get('notes', None)
            _data['resolutionStatus'] = _case.get('resolutionStatus', None)
            _rabbit.publish(_data)
        _logger.info('PhishLabs Stats retrieved. Finished PhishLabs stats retrieval')
    else:
        _logger.info('No PhishLabs Stats retrieved. Finished PhishLabs stats retrieval')
