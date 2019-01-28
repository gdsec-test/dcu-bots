import logging
import pika
import json
import os
import yaml
from logging.config import dictConfig
from datetime import datetime, timedelta

from phishlabs_api import PhishlabsAPI


def time_format(dt):
    """
    Function takes in a datetime object and returns a YYYY-mm-ddTHH:MM:SS.fffZ formatted string
    :param dt: datetime object
    :return: string
    """
    if type(dt) not in [datetime]:
        logger.error('Received unexpected type: {}'.format(type(dt)))
        return dt
    return "%s%sZ" % (dt.strftime('%Y-%m-%dT%H:%M:%S'), dt.strftime('%z'))


class Publisher:
    EXCHANGE = 'phishlabs-stats'
    TYPE = 'direct'
    ROUTING_KEY = 'phishlabs-stats'

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

    logger.info("Starting PhishLabs stats Retrieval")
    rabbit = Publisher(
        host='rmq-dcu.int.godaddy.com',
        port=5672,
        virtual_host='grandma',
        username=os.getenv('BROKER_USER', 'user'),
        password=os.getenv('BROKER_PASS', 'password'))
    phishlabs_api = PhishlabsAPI()
    response = phishlabs_api.retrieve_tickets(time_format(datetime.today() - timedelta(hours=1)), 'caseModify')
    if response and response.get('data'):
        phishlabs_data = response.get('data', None).pop(0)
        data = {}
        data['caseId'] = phishlabs_data.get('caseId', None)
        data['caseNumber'] = phishlabs_data.get('caseNumber', None)
        data['caseStatus'] = phishlabs_data.get('caseStatus', None)
        data['caseType'] = phishlabs_data.get('caseType', None)
        data['createdBy'] = phishlabs_data.get('createdBy', {}).get('name', None)
        data['dateCreated'] = phishlabs_data.get('dateCreated', None)
        data['dateClosed'] = phishlabs_data.get('dateClosed', None)
        data['dateModified'] = phishlabs_data.get('dateModified', None)
        data['description'] = phishlabs_data.get('description', None)
        data['notes'] = phishlabs_data.get('notes', None)
        data['resolutionStatus'] = phishlabs_data.get('resolutionStatus', None)
        rabbit.connect()
        rabbit.publish(data)
        logger.info("PhishLabs Stats retrieved. Finished PhishLabs stats retrieval")
    else:
        logger.info("No PhishLabs Stats retrieved. Finished PhishLabs stats retrieval")
